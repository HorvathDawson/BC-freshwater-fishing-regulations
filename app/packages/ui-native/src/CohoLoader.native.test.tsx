/**
 * (T814, owner 2026-11-17) THE NATIVE COHO KEEPS SWIMMING, as the circling fish does (`usePlatformLoops`): its one
 * platform-driven loop is started again when the app comes to the front, on the view's first layout, and when the loop
 * ends though nothing here stopped it (a dropped loop); Reduce Motion stops nothing.
 *
 * The native animation driver does not exist under jsdom, so `Animated.timing` and `AppState` are stand-ins that record
 * what was started, stopped and listened for (as `FishSpinner.native.test.tsx`); the view's `onLayout` is taken off the
 * loader's own view.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render } from "@testing-library/react";
import { COHO } from "./coho.generated";
import { COHO_WIDTH, cohoHeight } from "./coho";
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
  const View = forwardRef((p: Record<string, unknown>, ref) => {
    if (p.accessibilityRole === "progressbar" && typeof p.onLayout === "function") h.layouts.push(p.onLayout as () => void);
    return createElement(rn.View, { ...p, ref } as never);
  });
  const timing = (_v: unknown, c: { duration: number; iterations?: number; useNativeDriver: boolean }) => {
    const run: Run = { duration: c.duration, iterations: c.iterations, native: c.useNativeDriver, stopped: false, end: null };
    return {
      start: (cb?: (r: { finished: boolean }) => void) => { run.end = cb ?? null; h.runs.push(run); },
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

const { CohoLoader } = await import("./CohoLoader.native");

declare const setReduceMotion: (on: boolean) => void;

const live = () => h.runs.filter((r) => !r.stopped);
const flush = () => act(async () => { await Promise.resolve(); });
const app = (s: string) => act(() => { h.app.state = s; h.app.on?.(s); });

beforeEach(() => {
  h.runs.length = 0; h.layouts.length = 0; h.app.state = "active"; h.app.on = null;
  vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
});
afterEach(() => { cleanup(); setReduceMotion(false); vi.useRealTimers(); });

describe("<CohoLoader> native (T814)", () => {
  it("runs ONE loop of the whole sheet on the platform driver, infinite, with an end callback", async () => {
    render(<CohoLoader palette={LIGHT} />);
    await flush();
    expect(live().map((r) => [r.duration, r.iterations, r.native, typeof r.end])).toEqual([[COHO.loopMs, -1, true, "function"]]);
  });

  it("is said as a progress bar with the screen's words", async () => {
    const { getByLabelText } = render(<CohoLoader palette={LIGHT} label="Checking for the latest regulations…" />);
    expect(getByLabelText("Checking for the latest regulations…")).toBeTruthy();
  });

  it("clips one frame of the sheet, the sheet sized to the grid at the frame's own aspect", async () => {
    const { getByTestId } = render(<CohoLoader palette={LIGHT} width={150} />);
    const sheet = getByTestId("coho-sheet");
    const clip = sheet.parentElement!;
    expect([parseFloat(sheet.style.width), parseFloat(sheet.style.height), `${clip.style.overflowX} ${clip.style.overflowY}`,
            parseFloat(clip.style.width), parseFloat(clip.style.height)])
      .toEqual([150 * COHO.columns, cohoHeight(150) * COHO.rows, "hidden hidden", 150, cohoHeight(150)]);
    expect(cohoHeight(COHO_WIDTH)).toBe(80);
  });

  it("starts again on the view's first layout, and only the first", async () => {
    render(<CohoLoader palette={LIGHT} />);
    await flush();
    act(() => h.layouts.at(-1)!());
    act(() => h.layouts.at(-1)!());
    expect([h.runs.length, live().length]).toEqual([2, 1]);
  });

  it("starts again whenever the app comes to the front; inactive or background starts nothing", async () => {
    render(<CohoLoader palette={LIGHT} />);
    await flush();
    app("inactive");
    app("background");
    expect([h.runs.length, live().length]).toEqual([1, 1]);
    app("active");
    expect([h.runs.length, live().length]).toEqual([2, 1]);
  });

  it("a dropped loop is started again a second later; dropped in the background, at the next \"active\"", async () => {
    render(<CohoLoader palette={LIGHT} />);
    await flush();
    act(() => { live()[0]!.end!({ finished: false }); });
    act(() => { vi.advanceTimersByTime(999); });
    expect(h.runs.length).toBe(1);
    act(() => { vi.advanceTimersByTime(1); });
    expect([h.runs.length, live().length]).toEqual([2, 1]);
    app("background");
    act(() => { live()[0]!.end!({ finished: false }); });
    act(() => { vi.advanceTimersByTime(5000); });
    expect(h.runs.length).toBe(2);
    app("active");
    expect([h.runs.length, live().length]).toEqual([3, 1]);
  });

  it("unmounted: stopped, the app listener gone, nothing started after", async () => {
    const { unmount } = render(<CohoLoader palette={LIGHT} />);
    await flush();
    unmount();
    act(() => { vi.advanceTimersByTime(5000); });
    expect([h.runs.length, live().length, h.app.on]).toEqual([1, 0, null]);
  });

  it("swims under Reduce Motion too (owner 2026-11-17: \"we will ignore reduced motion for this\")", async () => {
    setReduceMotion(true);
    render(<CohoLoader palette={LIGHT} />);
    await flush();
    expect(live().length).toBe(1);
  });
});
