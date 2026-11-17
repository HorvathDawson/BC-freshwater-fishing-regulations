/**
 * (T760) THE FIRST INSTALL'S "NOT MOVING" STATE, on a fake clock: true once the byte count has stood still for a minute,
 * false again the moment it moves, and never while no download is watched.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, renderHook } from "@testing-library/react";
import { useDownloadStill, STILL_MS } from "./boot";

describe("useDownloadStill (T760)", () => {
  beforeEach(() => { vi.useFakeTimers({ now: 0 }); });
  afterEach(() => { cleanup(); vi.useRealTimers(); });
  const at = (ms: number) => act(() => { vi.advanceTimersByTime(ms); });

  it("says nothing for 59 s of the same count, then 'not moving' at 60 s; a byte more clears it and starts the clock again", () => {
    expect(STILL_MS).toBe(60_000);
    const { result, rerender } = renderHook(({ b }) => useDownloadStill(b), { initialProps: { b: 1000 as number | null } });
    at(59_000);
    expect(result.current).toBe(false);
    rerender({ b: 1000 });                                        // the same count reported again (a poll of a paused job)
    at(1_000);
    expect(result.current).toBe(true);
    rerender({ b: 1001 });                                        // bytes move
    expect(result.current).toBe(false);
    at(59_999);
    expect(result.current).toBe(false);
    at(1);
    expect(result.current).toBe(true);
  });

  it("a start that never reported (0 bytes) is not moving either; no download watched (null) never is", () => {
    const { result, rerender } = renderHook(({ b }) => useDownloadStill(b), { initialProps: { b: 0 as number | null } });
    at(60_000);
    expect(result.current).toBe(true);
    rerender({ b: null });                                        // the second stage (setup) or another start-up stage
    expect(result.current).toBe(false);
    at(10 * 60_000);
    expect(result.current).toBe(false);
    rerender({ b: 0 });                                           // watched again: the clock starts over
    at(59_000);
    expect(result.current).toBe(false);
    at(1_000);
    expect(result.current).toBe(true);
  });
});
