/**
 * (T254) LOW-BANDWIDTH MODE AS HOOKS: the setting (stored, read back, anything else is Automatic), the platform's word
 * (`navigator.connection`), the meter, hysteresis, the mode handed to the data layer — and the quiet line low mode waits
 * for before it fetches what nobody asked for.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { createNet, type Net, type NetMeter } from "@app/data";
import type { Connectivity } from "./imagery";
import { connectionHints, NetContext, readDataSetting, useNetQuiet, useNetState, type NetState } from "./net";

afterEach(() => { cleanup(); vi.useRealTimers(); });

function fakeMeter(): NetMeter & { set(b: number | null): void } {
  let bps: number | null = null;
  const subs = new Set<() => void>();
  return {
    start: () => 1, chunk: () => {}, end: () => {}, bps: () => bps,
    subscribe: (on) => { subs.add(on); return () => { subs.delete(on); }; },
    set(b) { bps = b; subs.forEach((f) => f()); },
  };
}
function fakeConnectivity(h: Record<string, unknown> = {}) {
  let online = true, hints = h;
  const on = new Set<(o: boolean) => void>(), hon = new Set<() => void>();
  const c: Connectivity = {
    watches: true, probeWhileUp: false, online: () => online,
    subscribe: (f) => { on.add(f); return () => { on.delete(f); }; },
    hints: () => hints, watchHints: (f) => { hon.add(f); return () => { hon.delete(f); }; },
  };
  return { c, goOffline(o: boolean) { online = !o; on.forEach((f) => f(online)); },
           hint(x: Record<string, unknown>) { hints = x; hon.forEach((f) => f()); } };
}
function Probe({ net, c, store, out }: { net: Net; c: Connectivity; store: { read(): string | null; write(t: string): void };
                                         out: { s?: NetState } }) {
  out.s = useNetState(net, { connectivity: c, store });
  return null;
}

describe("the setting", () => {
  it("stored words are read back; anything else is Automatic", () => {
    expect(["auto", "on", "off", " on\n", null, "low", ""].map(readDataSetting)).toEqual(["auto", "on", "off", "on", "auto", "auto", "auto"]);
  });
  it("what a browser says, as hints", () => {
    expect(connectionHints({ onLine: true, connection: { saveData: true, effectiveType: "3g", downlink: 0.4, rtt: 600 } }))
      .toEqual({ online: true, saveData: true, effectiveType: "3g", downlinkMbps: 0.4, rttMs: 600 });
    expect(connectionHints(undefined)).toEqual({ online: true, saveData: false, effectiveType: null, downlinkMbps: null, rttMs: null });
    expect(connectionHints({ onLine: false, connection: { downlink: 0, rtt: 0 } })).toMatchObject({ online: false, downlinkMbps: null, rttMs: null });
  });
});

describe("useNetState", () => {
  it("Automatic follows the meter (with hysteresis) and the device; On / Off override; the data layer is told; the choice is stored", () => {
    const meter = fakeMeter();
    const net = createNet({ meter });
    const conn = fakeConnectivity({ effectiveType: "4g" });
    let stored: string | null = null;
    const store = { read: () => stored, write: (t: string) => { stored = t; } };
    const out: { s?: NetState } = {};
    render(<Probe net={net} c={conn.c} store={store} out={out} />);
    expect(out.s!.mode).toEqual({ low: false, why: null, offline: false });
    act(() => meter.set(50_000));                     // Slow 3G measured, whatever the browser says
    expect(out.s!.mode).toEqual({ low: true, why: "measured", offline: false });
    expect(net.mode().low).toBe(true);
    act(() => meter.set(180_000));                    // between the lines: stays low
    expect(out.s!.mode.low).toBe(true);
    act(() => meter.set(400_000));                    // well above: back
    expect(out.s!.mode.low).toBe(false);
    act(() => conn.goOffline(true));
    expect(out.s!.mode.offline).toBe(true);
    expect(net.mode().offline).toBe(true);
    act(() => conn.goOffline(false));
    act(() => out.s!.setSetting("on"));
    expect([out.s!.mode.why, stored]).toEqual(["setting", "on"]);
    act(() => { out.s!.setSetting("off"); meter.set(1000); });
    expect(out.s!.mode.low).toBe(false);
  });
  it("nothing measured: the platform's word decides", () => {
    const net = createNet({ meter: fakeMeter() });
    const conn = fakeConnectivity({ effectiveType: "4g" });
    const out: { s?: NetState } = {};
    render(<Probe net={net} c={conn.c} store={{ read: () => null, write: () => {} }} out={out} />);
    expect(out.s!.mode.low).toBe(false);
    act(() => conn.hint({ cellularGeneration: "3g" }));
    expect(out.s!.mode).toEqual({ low: true, why: "cellular", offline: false });
    act(() => conn.hint({ saveData: true }));
    expect(out.s!.mode.why).toBe("save-data");
  });
  it("the stored setting is where it starts", () => {
    const out: { s?: NetState } = {};
    render(<Probe net={createNet({ meter: fakeMeter() })} c={fakeConnectivity().c} store={{ read: () => "on", write: () => {} }} out={out} />);
    expect(out.s!.mode).toEqual({ low: true, why: "setting", offline: false });
  });
});

describe("useNetQuiet", () => {
  function Quiet({ out }: { out: { q?: boolean } }) { out.q = useNetQuiet(15_000); return null; }
  it("off low mode: quiet at once; low: only after 15 s with nothing downloading, and from then on", () => {
    vi.useFakeTimers();
    const net = createNet();
    const out: { q?: boolean } = {};
    const value = (low: boolean): NetState => ({ net, mode: { low, why: low ? "measured" : null, offline: false }, setting: "auto", setSetting: () => {} });
    const { rerender } = render(<NetContext.Provider value={value(false)}><Quiet out={out} /></NetContext.Provider>);
    expect(out.q).toBe(true);
    const low = render(<NetContext.Provider value={value(true)}><Quiet out={out} /></NetContext.Provider>);
    expect(out.q).toBe(false);
    act(() => net.activity.queued("water", 0, 1000));
    act(() => { vi.advanceTimersByTime(20_000); });
    expect(out.q).toBe(false);                         // still downloading
    act(() => net.activity.done("water"));
    act(() => { vi.advanceTimersByTime(14_000); });
    expect(out.q).toBe(false);
    act(() => { vi.advanceTimersByTime(1_500); });
    expect(out.q).toBe(true);
    act(() => net.activity.queued("salmon", 1, 10));
    expect(out.q).toBe(true);                          // latched
    low.unmount(); rerender(<NetContext.Provider value={value(false)}><Quiet out={out} /></NetContext.Provider>);
  });
});
