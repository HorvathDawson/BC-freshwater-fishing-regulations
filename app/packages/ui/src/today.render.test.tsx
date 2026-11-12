import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, renderHook } from "@testing-library/react";
import { wallIso } from "@app/core";
import { appBecameActive, useBcToday } from "./today";

// B.C. is UTC-7 all year from 2026-03-08: 2026-11-09 23:59:30 PT is 2026-11-10T06:59:30Z
const BEFORE = Date.parse("2026-11-10T06:59:30Z");

describe("useBcToday (T551)", () => {
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(BEFORE); });
  afterEach(() => { vi.useRealTimers(); });

  it("moves to the new B.C. day at midnight with no tap, and keeps one object until then", () => {
    const { result } = renderHook(() => useBcToday());
    const first = result.current;
    expect(wallIso(first)).toBe("2026-11-09");
    act(() => { vi.advanceTimersByTime(10_000); });
    expect(result.current).toBe(first);
    act(() => { vi.advanceTimersByTime(25_000); });     // 00:00:05 PT
    expect(wallIso(result.current)).toBe("2026-11-10");
  });

  it("a clock that jumped while timers were suspended (app in the background) is caught within the minute", () => {
    const { result } = renderHook(() => useBcToday());
    vi.setSystemTime(BEFORE + 3 * 3_600_000);
    act(() => { vi.advanceTimersByTime(60_000); });
    expect(wallIso(result.current)).toBe("2026-11-10");
  });

  it("(T605 item 3) back at the front after midnight: the new day at once, with no timer run; nothing moves on the same day", () => {
    const { result, unmount } = renderHook(() => useBcToday());
    const first = result.current;
    act(() => { appBecameActive(); });
    expect(result.current).toBe(first);
    vi.setSystemTime(BEFORE + 3 * 3_600_000);            // suspended timers never ran
    act(() => { appBecameActive(); });
    expect(wallIso(result.current)).toBe("2026-11-10");
    // and the midnight after it is armed from the new clock: 21 h on, the next day without another word
    act(() => { vi.advanceTimersByTime(21 * 3_600_000 + 60_000); });
    expect(wallIso(result.current)).toBe("2026-11-11");
    unmount();
    expect(() => appBecameActive()).not.toThrow();       // an unmounted hook is no longer told
  });
});
