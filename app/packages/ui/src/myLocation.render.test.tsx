// @vitest-environment jsdom
import { act, renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { MY_LOCATION } from "@app/core";
import type { Locate } from "./mapRail";
import { useMyLocation } from "./myLocation";

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
});
