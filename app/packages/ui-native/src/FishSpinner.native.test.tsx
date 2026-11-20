/**
 * (T814) THE NATIVE FISH KEEPS SWIMMING: iPhone build 36's fish stood still after a reopen mid-download, and on a second
 * phone never moved on the first download. `FishSpinner.native.tsx` starts both platform-driven loops again when the app
 * comes to the front, on the view's first layout, and when a loop ends that nothing here stopped (a dropped loop).
 *
 * The native animation driver does not exist under jsdom, so `Animated.timing` and `AppState` are stand-ins that record
 * what was started, stopped and listened for; the view's `onLayout` is taken off the spinner's own view. (Owner
 * 2026-11-17) Reduce Motion stops nothing: the fish swims whatever it says.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { FISH_SPRITE } from "./fish-sprite.generated";
import { LIGHT } from "./theme";

interface Run { duration: number; iterations: number | undefined; native: boolean; stopped: boolean;
                end: ((r: { finished: boolean }) => void) | null }
const h = vi.hoisted(() => ({
  runs: [] as Run[],
  app: { state: "active", on: null as ((s: string) => void) | null },
  layouts: [] as (() => void)[],
}));

vi.mock("react-native", async (orig) => {
  const rn = await orig<typeof import("react-native")>();
  const { createElement, forwardRef } = await import("react");
  // the spinner's own view (the progressbar) hands its onLayout to the test
  const View = forwardRef((p: Record<string, unknown>, ref) => {
    if (p.accessibilityRole === "progressbar" && typeof p.onLayout === "function") h.layouts.push(p.onLayout as () => void);
    return createElement(rn.View, { ...p, ref } as never);
  });
  const timing = (_v: unknown, c: { duration: number; iterations?: number; useNativeDriver: boolean }) => {
    const run: Run = { duration: c.duration, iterations: c.iterations, native: c.useNativeDriver, stopped: false, end: null };
    return {
      start: (cb?: (r: { finished: boolean }) => void) => { run.end = cb ?? null; h.runs.push(run); },
      // the JS side calls the end callback at once on a stop, as React Native does
      stop: () => { run.stopped = true; run.end?.({ finished: false }); },
      reset: () => {},
    };
  };
  const AppState = {
    get currentState() { return h.app.state; },
    addEventListener: (_: string, fn: (s: string) => void) => { h.app.on = fn; return { remove: () => { h.app.on = null; } }; },
  };
  return { ...rn, View, AppState, Animated: { ...rn.Animated, timing } };
});

const { FishSpinner } = await import("./FishSpinner.native");

declare const setReduceMotion: (on: boolean) => void;

const live = () => h.runs.filter((r) => !r.stopped);
const flush = () => act(async () => { await Promise.resolve(); });
const app = (s: string) => act(() => { h.app.state = s; h.app.on?.(s); });

beforeEach(() => {
  h.runs.length = 0; h.layouts.length = 0; h.app.state = "active"; h.app.on = null;
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
});
afterEach(() => { cleanup(); setReduceMotion(false); vi.useRealTimers(); });

describe("<FishSpinner> native (T814)", () => {
  it("starts both loops on the platform driver, each one infinite timing (the native loop, with an end callback)", async () => {
    render(<FishSpinner palette={LIGHT} />);
    await flush();
    expect(live().map((r) => [r.duration, r.iterations, r.native]).sort())
      .toEqual([[FISH_SPRITE.cycleMs, -1, true], [FISH_SPRITE.orbitMs, -1, true]].sort());
  });

  it("starts them again on the view's first layout, and only the first", async () => {
    render(<FishSpinner palette={LIGHT} />);
    await flush();
    expect(h.layouts.length).toBeGreaterThan(0);
    act(() => h.layouts.at(-1)!());
    expect([h.runs.length, live().length]).toEqual([4, 2]);
    act(() => h.layouts.at(-1)!());
    expect([h.runs.length, live().length]).toEqual([4, 2]);
  });

  it("starts them again whenever the app comes to the front; going inactive or to the background starts nothing", async () => {
    render(<FishSpinner palette={LIGHT} />);
    await flush();
    app("inactive");
    app("background");
    expect([h.runs.length, live().length]).toEqual([2, 2]);
    app("active");
    expect([h.runs.length, live().length]).toEqual([4, 2]);
    app("active");
    expect([h.runs.length, live().length]).toEqual([6, 2]);
  });

  it("a loop the platform dropped is started again, once for both, a second later; no tight retry", async () => {
    render(<FishSpinner palette={LIGHT} />);
    await flush();
    const [a, b] = live();
    act(() => { a!.end!({ finished: false }); b!.end!({ finished: false }); });
    expect(h.runs.length).toBe(2);
    act(() => { vi.advanceTimersByTime(999); });
    expect(h.runs.length).toBe(2);
    act(() => { vi.advanceTimersByTime(1); });
    expect([h.runs.length, live().length]).toEqual([4, 2]);
  });

  it("dropped while in the background: started at the next \"active\", not before", async () => {
    render(<FishSpinner palette={LIGHT} />);
    await flush();
    app("background");
    act(() => { live()[0]!.end!({ finished: false }); });
    act(() => { vi.advanceTimersByTime(5000); });
    expect(h.runs.length).toBe(2);
    app("active");
    expect([h.runs.length, live().length]).toEqual([4, 2]);
  });

  it("unmounted: both stopped, the app listener gone, nothing started after", async () => {
    const { unmount } = render(<FishSpinner palette={LIGHT} />);
    await flush();
    unmount();
    act(() => { vi.advanceTimersByTime(5000); });
    expect([h.runs.length, live().length, h.app.on]).toEqual([2, 0, null]);
  });

  it("under Reduce Motion it swims all the same (owner 2026-11-17: \"we will ignore reduced motion for this\")", async () => {
    setReduceMotion(true);
    const { container } = render(<FishSpinner palette={LIGHT} />);
    await flush();
    expect([live().length, !!container.querySelector("[data-testid='fish-sprite']")]).toEqual([2, true]);
    act(() => h.layouts.at(-1)!());
    app("active");
    expect(live().length).toBe(2);
  });
});
