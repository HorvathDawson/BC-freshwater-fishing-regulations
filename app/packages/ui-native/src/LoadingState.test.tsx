/** (T257) The one waiting state: no flash on a quick load, no flicker on a short one. */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, renderHook, screen } from "@testing-library/react";
import { LoadingState, SPINNER_TIMING, useDelayedShow } from "./LoadingState";
import { LIGHT } from "./theme";

beforeEach(() => vi.useFakeTimers());
afterEach(() => { cleanup(); vi.useRealTimers(); });
const T = SPINNER_TIMING;

describe("useDelayedShow (T257)", () => {
  it("a quick load never shows the spinner", () => {
    const { result, rerender } = renderHook(({ busy }) => useDelayedShow(busy), { initialProps: { busy: true } });
    expect(result.current).toEqual({ pending: true, shown: false });
    act(() => { vi.advanceTimersByTime(T.showAfterMs - 50); });
    rerender({ busy: false });
    expect(result.current).toEqual({ pending: false, shown: false });
    act(() => { vi.advanceTimersByTime(1000); });
    expect(result.current.shown).toBe(false);
  });

  it("a slow load shows it after the delay, and keeps it up for the hold once shown", () => {
    const { result, rerender } = renderHook(({ busy }) => useDelayedShow(busy), { initialProps: { busy: true } });
    act(() => { vi.advanceTimersByTime(T.showAfterMs); });
    expect(result.current).toEqual({ pending: true, shown: true });
    act(() => { vi.advanceTimersByTime(100); });
    rerender({ busy: false });
    // done 100 ms after it appeared: held until the hold is out
    expect(result.current).toEqual({ pending: true, shown: true });
    act(() => { vi.advanceTimersByTime(T.holdMs - 100 - 1); });
    expect(result.current.shown).toBe(true);
    act(() => { vi.advanceTimersByTime(1); });
    expect(result.current).toEqual({ pending: false, shown: false });
  });

  it("a long load lets go at once when it ends after the hold", () => {
    const { result, rerender } = renderHook(({ busy }) => useDelayedShow(busy), { initialProps: { busy: true } });
    act(() => { vi.advanceTimersByTime(T.showAfterMs + T.holdMs + 500); });
    rerender({ busy: false });
    expect(result.current).toEqual({ pending: false, shown: false });
  });

  it("immediate: a surface that already waited shows it at once", () => {
    const { result } = renderHook(() => useDelayedShow(true, { immediate: true }));
    expect(result.current.shown).toBe(true);
  });
});

describe("LoadingState (T257)", () => {
  it("holds the spinner's place empty, then draws the fish with its caption", () => {
    render(<LoadingState palette={LIGHT} size={64} label="Reading" style={{ alignItems: "center" }}>
      <span>caption</span>
    </LoadingState>);
    expect(screen.queryByRole("progressbar")).toBeNull();
    expect(screen.queryByText("caption")).toBeNull();
    const held = screen.getByTestId("loading-held");
    expect([held.style.width, held.style.height]).toEqual(["64px", "64px"]);
    act(() => { vi.advanceTimersByTime(T.showAfterMs); });
    expect(screen.getByRole("progressbar").getAttribute("aria-label")).toBe("Reading");
    expect(screen.getByText("caption")).toBeTruthy();
  });

  it("follows the caller's timing when handed one", () => {
    const { rerender } = render(<LoadingState palette={LIGHT} shown={false} />);
    expect(screen.queryByRole("progressbar")).toBeNull();
    rerender(<LoadingState palette={LIGHT} shown />);
    expect(screen.getByRole("progressbar")).toBeTruthy();
  });
});
