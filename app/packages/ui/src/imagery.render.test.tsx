/**
 * CAN THE SATELLITE IMAGERY BE REACHED (owner 2026-10-21: satellite is online-only). The device's word where it has
 * one (the web), an imagery tile failing, and a probe of one tile — on a phone with no connectivity source, the probe
 * is the only signal. The hook only reports; `mapModes` decides the ground from it.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { useImageryReach, type Connectivity, type ImageryReachInput } from "./imagery";

/** A device that can say whether it is online, and change its mind. */
function webDevice(online = true) {
  let cb: ((on: boolean) => void) | null = null;
  const c: Connectivity = {
    watches: true,
    online: () => online,
    subscribe: (on) => { cb = on; return () => { cb = null; }; },
  };
  return { c, set: (on: boolean) => { online = on; act(() => cb?.(on)); } };
}
/** A phone: no connectivity source (no NetInfo dependency), so it never says. */
const phone: Connectivity = { watches: false, online: () => true, subscribe: () => () => {} };

const flush = () => act(async () => { await Promise.resolve(); await Promise.resolve(); });

beforeEach(() => { vi.useFakeTimers(); });
afterEach(() => { vi.useRealTimers(); });

describe("useImageryReach", () => {
  it("web: offline is down, online is up again by itself — no probe needed to come back", async () => {
    const d = webDevice(true);
    const probe = vi.fn(async () => true);
    const r = renderHook((i: ImageryReachInput) => useImageryReach(i),
      { initialProps: { wanted: true, connectivity: d.c, probe } });
    await flush();
    expect(r.result.current.up).toBe(true);
    d.set(false);
    expect(r.result.current.up).toBe(false);
    d.set(true);
    expect(r.result.current.up).toBe(true);
  });

  it("web: a device already offline starts down", () => {
    const d = webDevice(false);
    const r = renderHook(() => useImageryReach({ wanted: true, connectivity: d.c, probe: async () => true }));
    expect(r.result.current.up).toBe(false);
  });

  it("a failing imagery tile is down while the device says online; a 404 (no imagery there) is not", async () => {
    const d = webDevice(true);
    let answers = true;
    const probe = vi.fn(async () => answers);
    const r = renderHook(() => useImageryReach({ wanted: true, connectivity: d.c, probe, recheckMs: 1000 }));
    await flush();
    act(() => r.result.current.tileFailed(404));
    expect(r.result.current.up).toBe(true);
    answers = false;
    act(() => r.result.current.tileFailed(undefined));
    expect(r.result.current.up).toBe(false);
    // it comes back by itself: the recheck probe answers
    answers = true;
    await act(async () => { vi.advanceTimersByTime(1000); });
    await flush();
    expect(r.result.current.up).toBe(true);
  });

  it("phone: the probe is the signal — down when it fails, up when a later probe answers", async () => {
    let answers = false;
    const probe = vi.fn(async () => answers);
    const r = renderHook(() => useImageryReach({ wanted: true, connectivity: phone, probe, recheckMs: 1000 }));
    await flush();
    expect(r.result.current.up).toBe(false);
    answers = true;
    await act(async () => { vi.advanceTimersByTime(1000); });
    await flush();
    expect(r.result.current.up).toBe(true);
  });

  it("asks nothing while satellite is not chosen", async () => {
    const probe = vi.fn(async () => true);
    renderHook(() => useImageryReach({ wanted: false, connectivity: phone, probe, recheckMs: 1000 }));
    await act(async () => { vi.advanceTimersByTime(5000); });
    expect(probe).not.toHaveBeenCalled();
  });

  it("web: no probe while the device says offline (the online event brings it back)", async () => {
    const d = webDevice(false);
    const probe = vi.fn(async () => true);
    renderHook(() => useImageryReach({ wanted: true, connectivity: d.c, probe, recheckMs: 1000 }));
    await act(async () => { vi.advanceTimersByTime(5000); });
    expect(probe).not.toHaveBeenCalled();
  });
});
