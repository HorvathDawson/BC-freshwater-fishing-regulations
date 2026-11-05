import { describe, expect, it, vi } from "vitest";
import { setFailureSink, type GaugeAlertRule } from "@app/core";
import type { Manifest } from "../manifest";
import type { ObsFile } from "../feed/history";
import { runBackgroundCheck, updateReadyFor, type CheckDeps, type Notice } from "./check";
import {
  createNotifySettings, noteHeld, notifyActive, putRule, removeRule, setNotifyOn, setUpdateReady, NOTIFY_DEFAULT,
  type NotifyState,
} from "./settings";

const T0 = Date.parse("2026-11-05T12:00:00-07:00");
const rule: GaugeAlertRule = {
  id: "r1", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", direction: "above", value: 50,
  water: { item: "w1", name: "Chilliwack River" }, createdAt: T0,
};
const obs = (v: number | null, minutes = 0): ObsFile => ({
  fetchedAt: new Date(T0).toISOString(),
  stations: { "08MH001": [[new Date(T0 + minutes * 60_000).toISOString(), 1.1, v]] },
});
const manifest = (release: string, minAppVersion: string | null = null) =>
  ({ data_release: release, minAppVersion }) as unknown as Manifest;
const offline = () => Object.assign(new TypeError("Network request failed"));

function memory(init: NotifyState | null = null) {
  let text: string | null = init ? JSON.stringify(init) : null;
  return { read: () => text, write: (t: string) => { text = t; } };
}

function harness(state: Partial<NotifyState>, o: { obs?: () => Promise<ObsFile>; manifest?: () => Promise<Manifest>;
                                                    notify?: (n: Notice) => Promise<void> } = {}) {
  const settings = createNotifySettings(memory({ ...NOTIFY_DEFAULT, ...state }));
  const told: Notice[] = [];
  let clock = T0;
  const deps: CheckDeps = {
    settings,
    obs: o.obs ?? (async () => obs(40)),
    manifest: o.manifest ?? (async () => manifest("r-1")),
    notify: o.notify ?? (async (n) => { told.push(n); }),
    appVersion: { version: "0.5.0", build: 8 } as never,
    now: () => clock,
  };
  return { settings, told, deps, tick: (ms: number) => { clock += ms; } };
}

describe("the background check (T234)", () => {
  it("fires a gauge alert once per crossing, across runs, and logs every run", async () => {
    let v = 40;
    const h = harness({ on: true, rules: [rule] }, { obs: async () => obs(v, (h.deps.now() - T0) / 60_000) });
    for (const next of [40, 55, 60, 30, 56]) {
      v = next;
      await runBackgroundCheck(h.deps);
      h.tick(15 * 60_000);
    }
    expect(h.told.map((n) => n.title)).toEqual(["Chilliwack River: flow above 50 m³/s", "Chilliwack River: flow above 50 m³/s"]);
    expect(h.told[0]!.data).toEqual({ kind: "gauge", rule: "r1", station: "08MH001", item: "w1" });
    expect(h.settings.read().runs.map((r) => r.result)).toEqual(["nothing to tell", "1 alert", "nothing to tell", "nothing to tell", "1 alert"]);
  });

  it("offline is silent: nothing raised, nothing reported, the run logged 'offline'", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    try {
      const h = harness({ on: true, updateReady: true, held: "r-0", rules: [rule] },
                        { obs: async () => { throw offline(); }, manifest: async () => { throw offline(); } });
      const r = await runBackgroundCheck(h.deps);
      expect(r.offline).toBe(true);
      expect(h.told).toEqual([]);
      expect(sink).not.toHaveBeenCalled();
      expect(h.settings.read().runs.at(-1)!.result).toBe("offline");
    } finally { undo(); }
  });

  it("a failure that is not the network is reported and logged, never thrown", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    try {
      const h = harness({ on: true, rules: [rule] }, { obs: async () => { throw new Error("obs.json: HTTP 500"); } });
      await runBackgroundCheck(h.deps);
      expect(sink).toHaveBeenCalledTimes(1);
      expect(h.settings.read().runs.at(-1)!.result).toBe("failed: obs.json: HTTP 500");
    } finally { undo(); }
  });

  it("a notification that could not be raised is raised by the next run", async () => {
    const undo = setFailureSink(() => {});
    try {
      let refuse = true;
      const h = harness({ on: true, rules: [rule], ruleState: { r1: { past: false, at: "x", value: 40 } } },
                        { obs: async () => obs(60), notify: async (n) => { if (refuse) throw new Error("no channel"); h.told.push(n); } });
      await runBackgroundCheck(h.deps);
      expect(h.told).toEqual([]);
      refuse = false;
      await runBackgroundCheck(h.deps);
      expect(h.told).toHaveLength(1);
    } finally { undo(); }
  });

  it("stale or missing gauge data fires nothing and reports nothing", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    try {
      const h = harness({ on: true, rules: [rule], ruleState: { r1: { past: false, at: "x", value: 40 } } },
                        { obs: async () => obs(90, -10 * 60) });
      await runBackgroundCheck(h.deps);
      const m = harness({ on: true, rules: [rule], ruleState: { r1: { past: false } } },
                        { obs: async () => ({ fetchedAt: "", stations: {} }) });
      await runBackgroundCheck(m.deps);
      expect([h.told, m.told, sink.mock.calls]).toEqual([[], [], []]);
    } finally { undo(); }
  });

  it("nothing is fetched while notifications are off (opt-in)", async () => {
    const o = vi.fn(async () => obs(90));
    const m = vi.fn(async () => manifest("r-2"));
    const h = harness({ on: false, updateReady: true, held: "r-1", rules: [rule] }, { obs: o, manifest: m });
    await runBackgroundCheck(h.deps);
    expect([o.mock.calls.length, m.mock.calls.length, h.told.length]).toEqual([0, 0, 0]);
    expect(h.settings.read().runs.at(-1)!.result).toBe("no alerts set");
  });
});

describe("new data ready, once per release (T234)", () => {
  it("told once for a new release, not again for the same one; again for the next", async () => {
    let rel = "r-2";
    const h = harness({ on: true, updateReady: true, held: "r-1" }, { manifest: async () => manifest(rel) });
    await runBackgroundCheck(h.deps);
    await runBackgroundCheck(h.deps);
    rel = "r-3";
    await runBackgroundCheck(h.deps);
    expect(h.told.map((n) => n.data)).toEqual([{ kind: "update", release: "r-2" }, { kind: "update", release: "r-3" }]);
    expect(h.told[0]).toMatchObject({ channel: "updates", title: "New regulations data is ready", body: "Open the app to update." });
  });

  it("the decision: switch, release in use, already told, app too old", () => {
    const s = { on: true, updateReady: true, held: "r-1", toldRelease: null };
    const v = { version: "0.5.0", build: 8 } as never;
    expect(updateReadyFor(s, manifest("r-2"), v)).toBe(true);
    expect(updateReadyFor(s, manifest("r-1"), v)).toBe(false);                       // the one in use
    expect(updateReadyFor({ ...s, toldRelease: "r-2" }, manifest("r-2"), v)).toBe(false);
    expect(updateReadyFor({ ...s, held: null }, manifest("r-2"), v)).toBe(false);    // never opened: nothing to compare
    expect(updateReadyFor({ ...s, updateReady: false }, manifest("r-2"), v)).toBe(false);
    expect(updateReadyFor({ ...s, on: false }, manifest("r-2"), v)).toBe(false);
    expect(updateReadyFor(s, manifest("r-2", "9.0.0"), v)).toBe(false);             // this app cannot apply it
  });
});

describe("the settings file (T234)", () => {
  it("opt-in by default; adding an alert turns notifications on; a changed rule starts its state again", () => {
    const st = createNotifySettings(memory());
    expect(st.read()).toEqual(NOTIFY_DEFAULT);
    expect(notifyActive(st.read())).toBe(false);
    st.update((s) => ({ ...s, ruleState: { r1: { past: true } } }));
    st.update(putRule(rule));
    expect(st.read().on).toBe(true);
    expect(st.read().ruleState).toEqual({});
    expect(notifyActive(st.read())).toBe(true);
    st.update(putRule({ ...rule, value: 70 }));
    expect(st.read().rules.map((r) => r.value)).toEqual([70]);
    st.update(removeRule("r1"));
    st.update(setUpdateReady(true));
    st.update(noteHeld("r-9"));
    expect(st.read()).toMatchObject({ rules: [], updateReady: true, held: "r-9" });
    st.update(setNotifyOn(false));
    expect(notifyActive(st.read())).toBe(false);
  });

  it("an unreadable file reads as the defaults", () => {
    expect(createNotifySettings({ read: () => "{nope", write: () => {} }).read()).toEqual(NOTIFY_DEFAULT);
  });
});
