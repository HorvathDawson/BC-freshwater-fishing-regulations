/**
 * (T1013) THE RUNS FEED READ AHEAD on a phone: the source's own `latest()` (the one the Runs tab asks), once, idle a few
 * seconds after the map is paired; never before it is paired, and not at all once the screen is gone.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, renderHook } from "@testing-library/react";
import { usePrewarmRuns } from "./prewarm.native";

afterEach(() => { cleanup(); vi.useRealTimers(); });

describe("T1013 usePrewarmRuns (phone)", () => {
  it("asks latest() once, idle after the map is paired", () => {
    vi.useFakeTimers();
    vi.stubGlobal("requestIdleCallback", (f: () => void) => setTimeout(f, 0) as unknown as number);
    vi.stubGlobal("cancelIdleCallback", (id: number) => clearTimeout(id));
    const latest = vi.fn(async () => null);
    const { rerender } = renderHook(({ p }: { p: boolean }) => usePrewarmRuns({ latest }, p), { initialProps: { p: false } });
    vi.advanceTimersByTime(10_000);
    expect(latest).not.toHaveBeenCalled();          // not before the map is paired
    rerender({ p: true });
    vi.advanceTimersByTime(3_000);
    expect(latest).not.toHaveBeenCalled();          // the map's own first paint goes first
    vi.advanceTimersByTime(2_000);
    expect(latest).toHaveBeenCalledTimes(1);
  });
  it("asks nothing once the screen is gone", () => {
    vi.useFakeTimers();
    vi.stubGlobal("requestIdleCallback", (f: () => void) => setTimeout(f, 0) as unknown as number);
    vi.stubGlobal("cancelIdleCallback", (id: number) => clearTimeout(id));
    const latest = vi.fn(async () => null);
    const { unmount } = renderHook(() => usePrewarmRuns({ latest }, true));
    unmount();
    vi.advanceTimersByTime(10_000);
    expect(latest).not.toHaveBeenCalled();
  });
});
