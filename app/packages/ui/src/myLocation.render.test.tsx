// @vitest-environment jsdom
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MY_LOCATION } from "@app/core";
import type { Locate } from "./mapRail";
import { useMyLocation, useOpenAtMe } from "./myLocation";

const fix = { lat: 49.1, lon: -122.3, accuracy: 30 };
const make = (here: Locate["here"]): Locate => ({ permission: async () => "prompt", here });

describe("(T303) my location", () => {
  it("has nothing without a locate, and asks nothing until pressed", () => {
    expect(renderHook(() => useMyLocation(null)).result.current).toBeNull();
    const here = vi.fn(async () => fix);
    const { result } = renderHook(() => useMyLocation(make(here)));
    expect(here).not.toHaveBeenCalled();
    expect(result.current).toMatchObject({ on: false, following: false, fix: null, notice: null });
  });

  it("steps off → on (centred) → following → on", async () => {
    const stop = vi.fn();
    const loc: Locate = { ...make(async () => fix), watch: vi.fn(() => stop) };
    const { result } = renderHook(() => useMyLocation(loc));
    await act(async () => { result.current!.press(); });
    expect(result.current).toMatchObject({ on: true, following: false, fix, centre: 1 });
    act(() => result.current!.press());
    expect(result.current).toMatchObject({ on: true, following: true, centre: 2 });
    expect(loc.watch).toHaveBeenCalledTimes(1);
    act(() => result.current!.press());
    expect(result.current).toMatchObject({ on: true, following: false });
    expect(stop).not.toHaveBeenCalled(); // (T308) the dot keeps moving; only unmount stops the watch
  });

  it("(T308) the dot moves with every fix without following; unmount stops the watch", async () => {
    const stop = vi.fn();
    let push: (f: typeof fix) => void = () => {};
    const loc: Locate = { ...make(async () => fix), watch: vi.fn((on) => { push = on; return stop; }) };
    const { result, unmount } = renderHook(() => useMyLocation(loc));
    await act(async () => { result.current!.press(); });
    expect(loc.watch).toHaveBeenCalledTimes(1);
    const next = { ...fix, lat: fix.lat + 0.001 };
    act(() => push(next));
    expect(result.current).toMatchObject({ following: false, fix: next, centre: 1 });
    unmount();
    expect(stop).toHaveBeenCalled();
  });

  it("a hand drag stops following; the dot stays", async () => {
    const { result } = renderHook(() => useMyLocation(make(async () => fix)));
    await act(async () => { result.current!.press(); });
    act(() => result.current!.press());
    act(() => result.current!.release());
    expect(result.current).toMatchObject({ on: true, following: false });
  });

  it("a refusal says how to turn it on, stays off, and a second tap retries", async () => {
    const here = vi.fn().mockRejectedValueOnce(new Error("location permission refused")).mockResolvedValueOnce(fix);
    const { result } = renderHook(() => useMyLocation(make(here)));
    await act(async () => { result.current!.press(); });
    expect(result.current).toMatchObject({ on: false, notice: MY_LOCATION.denied });
    await act(async () => { result.current!.press(); });
    expect(result.current).toMatchObject({ on: true, notice: null });
  });

  it("no fix (timeout) is said differently from a refusal", async () => {
    const { result } = renderHook(() => useMyLocation(make(async () => { throw { code: 3 }; })));
    await act(async () => { result.current!.press(); });
    expect(result.current!.notice).toBe(MY_LOCATION.noFix);
  });

  describe("(T864) far from B.C.", () => {
    const TORONTO = { lat: 43.65, lon: -79.38, accuracy: 20 };
    const NULL_ISLAND = { lat: 0, lon: 0, accuracy: 20 };
    const DATE_LINE = { lat: 52, lon: 179.95 };
    const BELLINGHAM = { lat: 48.75, lon: -122.48, accuracy: 20 };

    it("a far press (Toronto, null island, over the date line): no dot, no centring, the far line once per press", async () => {
      const fixes = [TORONTO, NULL_ISLAND, DATE_LINE];
      const loc: Locate = { ...make(async () => fixes.shift()!), watch: vi.fn(() => () => {}) };
      const { result } = renderHook(() => useMyLocation(loc));
      const seen: unknown[] = [];
      for (let i = 0; i < 3; i++) {
        await act(async () => { result.current!.press(); });
        const { on, following, fix: f, centre, notice } = result.current!;
        seen.push({ on, following, fix: f, centre, notice });
        act(() => result.current!.dismiss());
      }
      const far = { on: false, following: false, fix: null, centre: 0, notice: MY_LOCATION.far };
      // dismissed after each: the line is the press's, not a standing banner
      expect([seen, result.current!.notice]).toEqual([[far, far, far], null]);
      expect(loc.watch).not.toHaveBeenCalled();
    });

    it("a border town (Bellingham) works as before", async () => {
      const { result } = renderHook(() => useMyLocation(make(async () => BELLINGHAM)));
      await act(async () => { result.current!.press(); });
      expect(result.current).toMatchObject({ on: true, fix: BELLINGHAM, centre: 1, notice: null });
    });

    it("while the dot is on, a far reading from the watch (a GPS glitch to 0,0) is dropped; the dot stays", async () => {
      let push: (f: typeof fix) => void = () => {};
      const loc: Locate = { ...make(async () => fix), watch: vi.fn((on) => { push = on; return () => {}; }) };
      const { result } = renderHook(() => useMyLocation(loc));
      await act(async () => { result.current!.press(); });
      act(() => push(NULL_ISLAND));
      act(() => push(TORONTO));
      expect(result.current).toMatchObject({ on: true, fix, centre: 1, notice: null });
    });

    it("the open's own centring from far away: the map stays, the line is said once", async () => {
      const here = vi.fn(async () => TORONTO);
      const loc: Locate = { permission: async () => "granted", here };
      const { result, rerender } = renderHook(({ ready }) => {
        const me = useMyLocation(loc);
        useOpenAtMe(me, loc, null, true, ready);
        return me;
      }, { initialProps: { ready: true } });
      await act(async () => { await Promise.resolve(); });
      await act(async () => { await Promise.resolve(); });
      rerender({ ready: true });
      expect([here.mock.calls.length, result.current!.on, result.current!.centre, result.current!.notice])
        .toEqual([1, false, 0, MY_LOCATION.far]);
    });
  });
});
