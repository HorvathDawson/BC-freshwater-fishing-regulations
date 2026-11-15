import { describe, expect, it, vi } from "vitest";
import {
  ALERT_SILENT_TELL_MS, alertLine, alertListLine, localStamp, percentileByDay, setFailureSink, type Band, type GaugeAlertRule,
} from "@app/core";
import type { Manifest } from "../manifest";
import type { ObsFile } from "../feed/history";
import { runBackgroundCheck, updateReadyFor, type CheckDeps, type Notice } from "./check";
import {
  createNotifySettings, dismissRefused, noteHeld, notifyActive, putRule, removeRule, setNotifyOn, setRuleOn, setUpdateReady, NOTIFY_DEFAULT,
  type NotifyState,
} from "./settings";

const T0 = Date.parse("2026-11-05T12:00:00-07:00");
const rule: GaugeAlertRule = {
  id: "r1", station: "08MH001", stationName: "Vedder Crossing", quantity: "discharge", direction: "above",
  threshold: { kind: "value", value: 50 }, on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: T0,
};
/** A reading as the feed writes it (local time with its offset), `minutes` after T0. */
const stamp = (minutes: number) => {
  const d = new Date(T0 + minutes * 60_000 - 7 * 3_600_000);
  return `${d.toISOString().slice(0, 19)}-07:00`;
};
const obs = (v: number | null, minutes = 0): ObsFile => ({
  fetchedAt: new Date(T0).toISOString(),
  stations: { "08MH001": [[stamp(minutes), 1.1, v]] },
});
const manifest = (release: string, minAppVersion: string | null = null) =>
  ({ data_release: release, minAppVersion }) as unknown as Manifest;
const offline = () => Object.assign(new TypeError("Network request failed"));

function memory(init: NotifyState | null = null) {
  let text: string | null = init ? JSON.stringify(init) : null;
  return { read: () => text, write: (t: string) => { text = t; } };
}

function harness(state: Partial<NotifyState>, o: { obs?: () => Promise<ObsFile>; manifest?: () => Promise<Manifest>;
                                                    notify?: (n: Notice) => Promise<void>;
                                                    latestApp?: () => Promise<unknown> } = {}) {
  const settings = createNotifySettings(memory({ ...NOTIFY_DEFAULT, ...state }));
  const told: Notice[] = [];
  let clock = T0;
  const deps: CheckDeps = {
    settings,
    obs: o.obs ?? (async () => obs(40)),
    manifest: o.manifest ?? (async () => manifest("r-1")),
    notify: o.notify ?? (async (n) => { told.push(n); }),
    latestApp: o.latestApp ?? null,
    appVersion: { version: "0.5.0", build: 8 } as never,
    now: () => clock,
  };
  return { settings, told, deps, tick: (ms: number) => { clock += ms; } };
}

describe("the background check (T234)", () => {
  it("fires a gauge alert once per crossing, re-armed past the margin, and logs every run", async () => {
    let v = 40;
    const h = harness({ on: true, rules: [rule] }, { obs: async () => obs(v, (h.deps.now() - T0) / 60_000) });
    for (const next of [40, 55, 60, 49.5, 52, 30, 56]) {
      v = next;
      await runBackgroundCheck(h.deps);
      h.tick(15 * 60_000);
    }
    // 55 crosses; 60 stays above; 49.5 is inside the 1 m³/s margin, so 52 is no crossing; 30 re-arms; 56 crosses again
    expect(h.told.map((n) => [n.title, n.body])).toEqual([
      ["Chilliwack River: flow alert", "Vedder Crossing crossed above 50 m³/s — 55 m³/s at 12:15 PT."],
      ["Chilliwack River: flow alert", "Vedder Crossing crossed above 50 m³/s — 56 m³/s at 13:30 PT."],
    ]);
    expect(h.told[0]!.data).toEqual({ kind: "gauge", rule: "r1", station: "08MH001", item: "w1" });
    expect(h.settings.read().runs.map((r) => r.result))
      .toEqual(["nothing to tell", "1 alert", "nothing to tell", "nothing to tell", "nothing to tell", "nothing to tell", "1 alert"]);
    expect(h.settings.read().ruleState.r1).toMatchObject({ past: true, value: 56, firedAt: T0 + 90 * 60_000 });
  });

  it("two alerts on one water each fire on their own crossing (no shared cadence)", async () => {
    let v = 40;
    const low = { ...rule, id: "r2", threshold: { kind: "value" as const, value: 45 } };
    const h = harness({ on: true, rules: [rule, low] }, { obs: async () => obs(v, (h.deps.now() - T0) / 60_000) });
    for (const next of [40, 55]) { v = next; await runBackgroundCheck(h.deps); h.tick(15 * 60_000); }
    expect(h.told.map((n) => (n.data as { rule: string }).rule)).toEqual(["r1", "r2"]);
    expect(h.settings.read().runs.at(-1)!.result).toBe("2 alerts");
  });

  it("a percentile alert crosses that day's line, read from the alert's own per-day line (no release needed)", async () => {
    // the envelope is the same every day: the median is 40, the 75th 60
    const yearBand = Array.from({ length: 366 }, (): Band => [10, 20, 40, 60, 80]);
    const p75: GaugeAlertRule = { ...rule, id: "p", stationName: "Cowichan R. at Lake Cowichan", water: { item: "w9", name: "Cowichan River" },
                                  threshold: { kind: "percentile", pct: 75, byDay: percentileByDay(yearBand, 75) } };
    let v = 50;
    const h = harness({ on: true, rules: [p75] }, { obs: async () => obs(v, (h.deps.now() - T0) / 60_000) });
    await runBackgroundCheck(h.deps);
    h.tick(15 * 60_000);
    v = 61.2;
    await runBackgroundCheck(h.deps);
    expect(h.told.map((n) => n.body)).toEqual(["Cowichan R. at Lake Cowichan crossed above the level higher than 8 days in 10 for Nov 5 — 61.2 m³/s at 12:15 PT."]);
  });

  it("a percentile alert on a day its line has none is not evaluated, nothing reported", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    try {
      const yearBand = Array.from({ length: 366 }, (_, i): Band | null => (i === 308 ? null : [10, 20, 40, 60, 80]));   // Nov 5 = day 309
      const p: GaugeAlertRule = { ...rule, threshold: { kind: "percentile", pct: 50, byDay: percentileByDay(yearBand, 50) } };
      const h = harness({ on: true, rules: [p], ruleState: { r1: { past: false, at: "x", value: 1 } } }, { obs: async () => obs(99) });
      await runBackgroundCheck(h.deps);
      expect([h.told, sink.mock.calls, h.settings.read().ruleState.r1]).toEqual([[], [],
        { past: false, at: "x", value: 1, skipped: "no-record", skippedSince: T0 }]);
    } finally { undo(); }
  });

  it("an alert switched off is not checked (nothing fetched for it)", async () => {
    const o = vi.fn(async () => obs(90));
    const h = harness({ on: true, rules: [{ ...rule, on: false }] }, { obs: o });
    await runBackgroundCheck(h.deps);
    expect([o.mock.calls.length, h.settings.read().runs.at(-1)!.result]).toEqual([0, "no alerts set"]);
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

describe("a new app version, once per version (T234 v1.1)", () => {
  it("told once for a newer version on our server, not again; again for the next; never when off", async () => {
    let file: unknown = { version: "0.6.0", build: 9, notes: ["Alerts"] };
    const h = harness({ on: true, appVersion: true }, { latestApp: async () => file });
    await runBackgroundCheck(h.deps);
    await runBackgroundCheck(h.deps);
    file = { version: "0.6.1", build: 10 };
    await runBackgroundCheck(h.deps);
    expect(h.told.map((n) => [n.channel, n.title, n.body, n.data])).toEqual([
      ["app", "A new version of the app is out", "Version 0.6.0 is ready. Tap to see what's new and update.", { kind: "app", version: "0.6.0" }],
      ["app", "A new version of the app is out", "Version 0.6.1 is ready. Tap to see what's new and update.", { kind: "app", version: "0.6.1" }],
    ]);
    expect(h.settings.read().runs.map((r) => r.result)).toEqual(["new app version", "nothing to tell", "new app version"]);
    const off = vi.fn(async () => file);
    const o = harness({ on: true, appVersion: false, updateReady: true, held: "r-1" }, { latestApp: off });
    await runBackgroundCheck(o.deps);
    expect(off).not.toHaveBeenCalled();
  });

  it("a version file naming no version is reported and logged", async () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    try {
      const h = harness({ on: true, appVersion: true }, { latestApp: async () => ({ build: 3 }) });
      await runBackgroundCheck(h.deps);
      expect(sink).toHaveBeenCalledTimes(1);
      expect(h.told).toEqual([]);
    } finally { undo(); }
  });
});

describe("the settings file (T234, T527)", () => {
  it("a v1 file reads into v3 whole: every above/below alert kept as a crossing, its state and switches too", () => {
    const v1 = JSON.stringify({ v: 1, on: true, updateReady: true, held: "r-3", toldRelease: "r-2", runs: [],
      rules: [{ id: "a", station: "08MH001", stationName: "Vedder", quantity: "discharge", direction: "above", value: 50,
                water: { item: "w1", name: "Chilliwack River" }, createdAt: 1 },
              { id: "b", station: "08MF005", stationName: "Hope", quantity: "level", direction: "below", value: 2.5,
                water: null, createdAt: 2 }],
      ruleState: { a: { past: true, at: "t", value: 60, firedAt: 9 } } });
    const st = createNotifySettings({ read: () => v1, write: () => {} }).read();
    expect(st).toMatchObject({ v: 3, on: true, updateReady: true, appVersion: false, held: "r-3", toldRelease: "r-2",
                               refused: [], toldAppVersion: null, ruleState: { a: { past: true, at: "t", value: 60, firedAt: 9 } } });
    expect(st.rules.map((r) => [r.id, r.direction, r.threshold, r.on]))
      .toEqual([["a", "above", { kind: "value", value: 50 }, true], ["b", "below", { kind: "value", value: 2.5 }, true]]);
  });

  it("a v2 file: above/below kept (switch and state), a ±% alert refused with its words, reported, its state gone", () => {
    const sink = vi.fn();
    const undo = setFailureSink(sink);
    try {
      let text: string | null = JSON.stringify({ v: 2, on: true, updateReady: false, appVersion: false, held: null,
        toldRelease: null, toldAppVersion: null, runs: [], waterFired: { w1: 5 },
        rules: [{ id: "a", station: "08MH001", stationName: "Vedder", quantity: "discharge", kind: "below", value: 12, gapH: 6,
                  on: false, water: { item: "w1", name: "Chilliwack River" }, createdAt: 1 },
                { id: "p", station: "08MH001", stationName: "Vedder", quantity: "discharge", kind: "rise", value: 50, gapH: null,
                  on: true, water: { item: "w1", name: "Chilliwack River" }, createdAt: 2 }],
        ruleState: { a: { past: false, at: "t", value: 14, firedAt: 3 }, p: { past: true, change: 60 } } });
      const st = createNotifySettings({ read: () => text, write: (t) => { text = t; } });
      const s = st.read();
      expect(s.rules.map((r) => [r.id, r.direction, r.threshold, r.on])).toEqual([["a", "below", { kind: "value", value: 12 }, false]]);
      expect(s.refused).toEqual([{ line: "Vedder · Flow rises 50 % or more within 24 h", why: "a change within a day is no longer an alert kind" }]);
      expect(s.ruleState).toEqual({ a: { past: false, at: "t", value: 14, firedAt: 3 } });
      expect("waterFired" in s).toBe(false);
      expect(sink).toHaveBeenCalled();
      // the first write keeps the note (v3 on disk), until the reader dismisses it
      st.update(setNotifyOn(true));
      expect(JSON.parse(text!)).toMatchObject({ v: 3, refused: [{ line: "Vedder · Flow rises 50 % or more within 24 h" }] });
      expect(st.read().refused).toHaveLength(1);
      st.update(dismissRefused);
      expect(st.read().refused).toEqual([]);
      expect(st.read().rules.map((r) => r.id)).toEqual(["a"]);
    } finally { undo(); }
  });

  it("an alert that cannot be read is refused and said — not dropped quietly (reported once a session, above)", () => {
    const text = JSON.stringify({ v: 2, rules: [{ id: "x", station: "s", value: 1 }], ruleState: {} });
    const s = createNotifySettings({ read: () => text, write: () => {} }).read();
    expect([s.rules, s.refused]).toEqual([[], [{ line: "A saved alert", why: "it could not be read" }]]);
  });

  it("one alert's switch never moves the master switch; on again it starts again, when it last fired kept", () => {
    const st = createNotifySettings(memory({ ...NOTIFY_DEFAULT, on: false, rules: [rule], ruleState: { r1: { past: true, firedAt: 7 } } }));
    st.update(setRuleOn("r1", false));
    expect([st.read().on, st.read().rules[0]!.on]).toEqual([false, false]);
    st.update(setRuleOn("r1", true));
    expect(st.read().ruleState).toEqual({ r1: { past: null, firedAt: 7 } });
  });

  it("opt-in by default; adding an alert turns notifications on and starts its state", () => {
    const st = createNotifySettings(memory());
    expect(st.read()).toEqual(NOTIFY_DEFAULT);
    expect(notifyActive(st.read())).toBe(false);
    st.update((s) => ({ ...s, ruleState: { r1: { past: true } } }));
    st.update(putRule(rule));
    expect(st.read().on).toBe(true);
    expect(st.read().ruleState).toEqual({});
    expect(notifyActive(st.read())).toBe(true);
    st.update(putRule({ ...rule, id: "r2" }));
    expect(st.read().rules.map((r) => r.id)).toEqual(["r1", "r2"]);
    st.update(removeRule("r1"));
    st.update(setUpdateReady(true));
    st.update(noteHeld("r-9"));
    expect(st.read()).toMatchObject({ rules: [{ id: "r2" }], updateReady: true, held: "r-9" });
    st.update(setNotifyOn(false));
    expect(notifyActive(st.read())).toBe(false);
  });

  it("an unreadable file reads as the defaults, with a note that says so (T554 item 2: settings.test.ts)", () => {
    expect(createNotifySettings({ read: () => "{nope", write: () => {} }).read())
      .toEqual({ ...NOTIFY_DEFAULT, refused: [{ line: "Your saved alerts", why: "the alerts file could not be read; a copy was kept aside on this phone" }] });
  });
});

// (T554 item 1) an alert on a gauge that stopped reporting said nothing: the skip was computed and dropped.
describe("an alert whose gauge is silent (T554 item 1)", () => {
  it("keeps why and since when, says so on its row and in the run line, tells once after a day, and clears", async () => {
    let rows: ObsFile = obs(40);
    const h = harness({ on: true, rules: [rule] }, { obs: async () => rows });
    await runBackgroundCheck(h.deps);                                    // a reading: evaluated
    rows = { ...obs(40), stations: {} };                                 // the gauge goes quiet
    h.tick(60 * 60_000);
    const quiet = await runBackgroundCheck(h.deps);
    const since = T0 + 60 * 60_000;
    expect(h.settings.read().ruleState.r1).toMatchObject({ skipped: "missing", skippedSince: since });
    expect(quiet.line).toBe("1 alert's gauge not reporting");
    expect(alertListLine(rule, h.settings.read().ruleState.r1))
      .toBe(`${rule.stationName} · ${alertLine(rule)} · not fired yet · gauge not reporting since ${localStamp(T0)}`);
    h.tick(ALERT_SILENT_TELL_MS - 1);
    await runBackgroundCheck(h.deps);
    expect(h.told).toEqual([]);                                          // a day not yet up
    h.tick(1);
    await runBackgroundCheck(h.deps);
    h.tick(15 * 60_000);
    await runBackgroundCheck(h.deps);
    expect(h.told.map((n) => n.body)).toEqual([                          // once
      `${rule.stationName} has not reported since ${localStamp(T0)}, so this alert cannot be checked. It is checked again as soon as the gauge reports.`]);
    rows = obs(41, (h.deps.now() - T0) / 60_000);                        // it reports again
    await runBackgroundCheck(h.deps);
    const st = h.settings.read().ruleState.r1!;
    expect([st.skipped, st.skippedSince, st.silentTold]).toEqual([null, null, null]);
    expect(alertListLine(rule, st)).not.toContain("not reporting");
  });
});
