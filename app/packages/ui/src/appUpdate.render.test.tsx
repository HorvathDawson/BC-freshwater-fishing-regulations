/**
 * (T390) useAppUpdate: asked on every opening and every return to the app, never offline; the answer is kept (shown at
 * once on the next opening); a failed check or
 * start is said and reported (the connection's never reported); the button is immediate for the strong prompt.
 * (T888) the popup on opening: once per version (Not now kept in the same file), the bell keeps the update, held while
 * the Shell says so, never without a platform way (the web); the strong prompt every opening.
 * Mounted (jsdom) because it is a hook.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { environmentError, setFailureSink, wallIso } from "@app/core";
import { useAppUpdate, type AppUpdateControl, type AppUpdatePort } from "./appUpdate";

afterEach(cleanup);
const mem = (t: string | null = null) => ({ read: () => t, write: (x: string) => { t = x; } });
const today = wallIso(new Date());
const flush = () => act(async () => { await new Promise((r) => setTimeout(r, 0)); });

function port(o: { check?: () => Promise<{ available: boolean; version: string | null }>;
                   start?: (x: { immediate: boolean }) => Promise<"downloading" | "opened" | "listing" | "none"> } = {}) {
  const calls = { check: 0, start: [] as boolean[] };
  const p: AppUpdatePort = {
    channel: "play",
    check: () => { calls.check++; return o.check ? o.check() : Promise.resolve({ available: true, version: "0.6.0" }); },
    start: (x) => { calls.start.push(x.immediate); return o.start ? o.start(x) : Promise.resolve("downloading"); },
  };
  return { p, calls };
}

function Probe({ p, store, offline = false, tooOld = false, resume = 0, held = false, out }: {
  p: AppUpdatePort | null; store: ReturnType<typeof mem>; offline?: boolean; tooOld?: boolean; resume?: number;
  held?: boolean; out: { c?: AppUpdateControl | null } }) {
  out.c = useAppUpdate(p, store, { offline, tooOld, resume, held });
  return null;
}

describe("useAppUpdate", () => {
  it("asks on opening and keeps the answer: the next opening shows it at once and asks again", async () => {
    const store = mem(), { p, calls } = port(), out: { c?: AppUpdateControl | null } = {};
    const first = render(<Probe p={p} store={store} out={out} />);
    await flush();
    expect(calls.check).toBe(1);
    expect(out.c!.vm).toMatchObject({ strength: "gentle", text: "Version 0.6.0 of the app is available." });
    expect(JSON.parse(store.read()!)).toEqual({ day: today, available: true, version: "0.6.0", notes: null, popped: null });
    first.unmount();
    const second = render(<Probe p={p} store={store} out={out} />);
    expect(out.c!.vm?.strength).toBe("gentle");   // the kept answer, before the new ask lands
    await flush();
    expect(calls.check).toBe(2);
    // (T390 11-05) each return to the app asks again, the same day: 0.6.0 missed 0.6.1 behind a daily cap
    second.rerender(<Probe p={p} store={store} resume={1} out={out} />);
    await flush();
    expect(calls.check).toBe(3);
  });
  it("offline: never asked, nothing said; back online it asks", async () => {
    const store = mem(JSON.stringify({ day: "2020-01-01", available: true, version: "0.6.0" })), { p, calls } = port();
    const out: { c?: AppUpdateControl | null } = {};
    const r = render(<Probe p={p} store={store} offline out={out} />);
    await flush();
    expect([calls.check, out.c!.vm]).toEqual([0, null]);
    r.rerender(<Probe p={p} store={store} out={out} />);
    await flush();
    expect(calls.check).toBe(1);
  });
  it("a failed check is said and reported; a dropped line is neither; nothing is kept for either", async () => {
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      const store = mem(), out: { c?: AppUpdateControl | null } = {};
      const { p } = port({ check: () => Promise.reject(new Error("ERR_CHECK_FOR_UPDATE")) });
      render(<Probe p={p} store={store} out={out} />);
      await flush();
      expect(out.c!.vm).toMatchObject({ text: "Could not check for a newer version of the app.", failed: true, button: null });
      expect(got).toHaveLength(1);
      expect(store.read()).toBeNull();
      cleanup();
      const store2 = mem(), out2: { c?: AppUpdateControl | null } = {};
      const off = port({ check: () => Promise.reject(environmentError(new Error("Network request failed"))) });
      render(<Probe p={off.p} store={store2} out={out2} />);
      await flush();
      expect([out2.c!.vm, store2.read(), got.length]).toEqual([null, null, 1]);
    } finally { undo(); }
  });
  it("(T595) a phone with no Play service: a known state, said as one and never reported", async () => {
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      const out: { c?: AppUpdateControl | null } = {};
      const { p } = port({ check: () => Promise.reject(new Error("Failed to bind to the service")) });
      render(<Probe p={p} store={mem()} out={out} />);
      await flush();
      expect(out.c!.vm).toMatchObject({ text: "This phone has no Google Play to ask, so the app cannot check for a newer version.",
                                       failed: false, button: null });
      expect(got).toEqual([]);
    } finally { undo(); }
  });
  it("the strong prompt: Update starts the immediate flow; a failed start is said and reported", async () => {
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      const store = mem(), out: { c?: AppUpdateControl | null } = {};
      const ok = port({ check: () => Promise.resolve({ available: false, version: null }) });
      const r = render(<Probe p={ok.p} store={store} tooOld out={out} />);
      await flush();
      expect(out.c!.vm).toMatchObject({ strength: "strong", button: { immediate: true } });
      act(() => out.c!.update());
      await flush();
      expect(ok.calls.start).toEqual([true]);
      expect(out.c!.vm).toMatchObject({ said: "Downloading the update. The app restarts when it is ready.", button: null });
      r.unmount();
      const bad = port({ start: () => Promise.reject(new Error("ERR_START_UPDATE")) });
      render(<Probe p={bad.p} store={store} tooOld out={out} />);
      act(() => out.c!.update());
      await flush();
      expect(out.c!.vm).toMatchObject({ said: "The update could not start.", failed: true, button: { label: "Update" } });
      expect(got).toHaveLength(1);
    } finally { undo(); }
  });
  it("no platform way (the web): only the strong prompt, with no button", async () => {
    const out: { c?: AppUpdateControl | null } = {};
    const r = render(<Probe p={null} store={mem()} out={out} />);
    expect(out.c).toBeNull();
    r.rerender(<Probe p={null} store={mem()} tooOld out={out} />);
    expect(out.c!.vm).toMatchObject({ strength: "strong", button: null });
  });
});

describe("(T888) the popup on opening", () => {
  const versions = (v: string) => port({ check: () => Promise.resolve({ available: true, version: v }) });
  it("shows once per version: Not now keeps it answered across openings, and the bell keeps the update", async () => {
    const store = mem(), out: { c?: AppUpdateControl | null } = {};
    const { p } = versions("0.8.2");
    const first = render(<Probe p={p} store={store} out={out} />);
    await flush();
    expect(out.c!.popup).toMatchObject({ key: "0.8.2", title: "Version 0.8.2 is out", strength: "gentle" });
    act(() => out.c!.dismiss());
    expect(out.c!.popup).toBeNull();
    // the bell keeps it, counted, with its one button
    expect(out.c!.vm).toMatchObject({ strength: "gentle", counts: true, button: { label: "Update" } });
    expect(JSON.parse(store.read()!).popped).toBe("0.8.2");
    first.unmount();
    // the next opening: the same version, asked again: no popup, the bell still has it
    render(<Probe p={p} store={store} out={out} />);
    expect(out.c!.popup).toBeNull();
    await flush();
    expect([out.c!.popup, out.c!.vm?.counts]).toEqual([null, true]);
    expect(JSON.parse(store.read()!).popped).toBe("0.8.2");   // a new check keeps the answer
    cleanup();
    // a newer version later: the popup again
    render(<Probe p={versions("0.8.3").p} store={store} out={out} />);
    await flush();
    expect(out.c!.popup).toMatchObject({ key: "0.8.3", title: "Version 0.8.3 is out" });
  });
  it("its Update starts the store's flow and, once under way, answers the version", async () => {
    const store = mem(), out: { c?: AppUpdateControl | null } = {};
    const { p, calls } = versions("0.8.2");
    render(<Probe p={p} store={store} out={out} />);
    await flush();
    act(() => out.c!.popupUpdate());
    expect(out.c!.popup).toMatchObject({ said: "Starting the update…", button: null });
    await flush();
    expect(calls.start).toEqual([false]);
    expect(out.c!.popup).toBeNull();
    expect(out.c!.vm).toMatchObject({ said: "Downloading the update. The app restarts when it is ready." });
    expect(JSON.parse(store.read()!).popped).toBe("0.8.2");
  });
  it("a start that fails stays up and says so; the version is not answered", async () => {
    const store = mem(), out: { c?: AppUpdateControl | null } = {};
    const bad = port({ check: () => Promise.resolve({ available: true, version: "0.8.2" }),
                       start: () => Promise.reject(environmentError(new Error("Network request failed"))) });
    render(<Probe p={bad.p} store={store} out={out} />);
    await flush();
    act(() => out.c!.popupUpdate());
    await flush();
    expect(out.c!.popup).toMatchObject({ key: "0.8.2", said: "No connection. Try again when you are online.", failed: true });
    expect(JSON.parse(store.read()!).popped).toBeNull();
  });
  it("waits while held (the first-run notice, the walkthrough) and comes up once it is not", async () => {
    const store = mem(), out: { c?: AppUpdateControl | null } = {};
    const { p } = versions("0.8.2");
    const r = render(<Probe p={p} store={store} held out={out} />);
    await flush();
    expect([out.c!.popup, out.c!.vm?.counts]).toEqual([null, true]);   // held: the bell has it already
    r.rerender(<Probe p={p} store={store} out={out} />);
    expect(out.c!.popup).toMatchObject({ key: "0.8.2" });
  });
  it("not when the app is current, nor on the web (no platform way)", async () => {
    const store = mem(), out: { c?: AppUpdateControl | null } = {};
    render(<Probe p={port({ check: () => Promise.resolve({ available: false, version: null }) }).p} store={store} out={out} />);
    await flush();
    expect([out.c!.popup, out.c!.vm]).toEqual([null, null]);
    cleanup();
    render(<Probe p={null} store={mem(JSON.stringify({ day: today, available: true, version: "0.8.2" }))} tooOld out={out} />);
    expect(out.c!.popup).toBeNull();
  });
  it("the strong prompt (new data needs this app) comes up on every opening; Not now hides it for this run only", async () => {
    const store = mem(), out: { c?: AppUpdateControl | null } = {};
    const { p } = port({ check: () => Promise.resolve({ available: false, version: null }) });
    const first = render(<Probe p={p} store={store} tooOld out={out} />);
    await flush();
    expect(out.c!.popup).toMatchObject({ strength: "strong", title: "New data needs the latest app",
                                         sub: "The app keeps using the data it has until it is updated." });
    act(() => out.c!.dismiss());
    expect(out.c!.popup).toBeNull();
    expect(out.c!.vm?.strength).toBe("strong");
    first.unmount();
    render(<Probe p={p} store={store} tooOld out={out} />);
    expect(out.c!.popup?.strength).toBe("strong");
  });
});
