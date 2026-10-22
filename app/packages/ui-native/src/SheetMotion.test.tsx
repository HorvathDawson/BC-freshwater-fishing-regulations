/**
 * THE SHEET'S BACKDROP STAYS PUT (refinement pass, owner 2026-10-22: "when menus close the screen opacity slides down with
 * the menu instead of staying in the back then disappearing after menu slides down").
 *
 * Sampled every 20 ms through an open and a close, on the JS driver react-native-web runs (the native driver drives the
 * same values on a phone): the scrim's transform is never anything but none, while the sheet's moves; the scrim's opacity
 * only fades — in alongside the rise, out AFTER the sheet has gone; reduced motion slides nothing and fades quickly.
 */
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import { Sheet } from "./Sheet";
import { SHEET_MOTION } from "./motion";
import { LIGHT } from "./theme";

declare const setReduceMotion: (on: boolean) => void;

/*
 * THE REAL ANIMATED, not react-native-web's test double: under NODE_ENV=test its `Animated` is `AnimatedMock`, whose every
 * animation ends at once — which would pass a timing test on any order of events. It chooses once, when its module loads,
 * so NODE_ENV reads "development" while this file's imports load (hoisted above them) and is put back in `beforeAll`. The
 * real implementation runs on requestAnimationFrame + Date, both faked below: 20 ms of fake time is 20 ms of motion.
 */
const NODE_ENV = vi.hoisted(() => { const was = process.env.NODE_ENV; process.env.NODE_ENV = "development"; return was; });
beforeAll(() => { process.env.NODE_ENV = NODE_ENV; });

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date", "performance", "requestAnimationFrame", "cancelAnimationFrame", "setTimeout", "clearTimeout"] });
});
afterEach(() => { cleanup(); vi.useRealTimers(); setReduceMotion(false); });

const sheet = (open: boolean) => (
  <Sheet open={open} onClose={() => {}} title="Layers" palette={LIGHT}><Text>choices</Text></Sheet>);

interface Frame { t: number; scrimT: string; scrimO: string; sheetT: string; sheetO: string }
/** `ms` of motion, one row every 20 ms: the scrim's and the sheet's transform and opacity as drawn (null: not drawn) */
function sample(ms: number): (Frame | null)[] {
  const rows: (Frame | null)[] = [];
  for (let t = 0; t <= ms; t += 20) {
    if (t) act(() => { vi.advanceTimersByTime(20); });
    const scrim = screen.queryByTestId("sheet-scrim"), panel = screen.queryByTestId("sheet-panel");
    rows.push(scrim && panel ? { t, scrimT: scrim.style.transform, scrimO: scrim.style.opacity,
                                 sheetT: panel.style.transform, sheetO: panel.style.opacity } : null);
  }
  return rows;
}
const opacities = (rows: (Frame | null)[]) => rows.map((r) => (r ? Number(r.scrimO || 1) : null));
/** jsdom lays nothing out: the sheet and the window both measure 0, so "below the window" is the 1000 px fallback */
const OFF = "translateY(1000px)";
const rising = (xs: (number | null)[]) => xs.every((x, i) => i === 0 || (x ?? 0) >= (xs[i - 1] ?? 0));
const falling = (xs: (number | null)[]) => xs.every((x, i) => i === 0 || (x ?? 0) <= (xs[i - 1] ?? 0));

describe("the sheet's scrim stays in place and fades", () => {
  it("opening: the sheet rises from below the window, the scrim never moves and only fades in", () => {
    const { rerender } = render(sheet(false));
    expect(screen.queryByTestId("sheet-scrim")).toBeNull();
    rerender(sheet(true));
    const rows = sample(SHEET_MOTION.inMs + 60);
    expect([...new Set(rows.map((r) => r!.scrimT))]).toEqual([""]);
    expect([rows[0]!.sheetT, rows.at(-1)!.sheetT]).toEqual([OFF, "translateY(0px)"]);
    expect(new Set(rows.map((r) => r!.sheetT)).size).toBeGreaterThan(5);        // it slides, frame by frame
    const o = opacities(rows);
    expect([o[0], o.at(-1), rising(o)]).toEqual([0, 1, true]);
  });

  it("closing: the sheet slides away first; the scrim holds where it is, then fades, then the sheet is gone", () => {
    const { rerender } = render(sheet(true));
    act(() => { vi.advanceTimersByTime(1000); });
    rerender(sheet(false));
    const rows = sample(SHEET_MOTION.fadeOutDelayMs + SHEET_MOTION.fadeOutMs + 80);
    const drawn = rows.filter((r): r is Frame => r !== null);
    expect([...new Set(drawn.map((r) => r.scrimT))]).toEqual([""]);
    // the dim HOLDS, untouched, while the sheet slides away under it…
    const holding = drawn.filter((r) => r.t < SHEET_MOTION.fadeOutDelayMs);
    expect([[...new Set(opacities(holding))], new Set(holding.map((r) => r.sheetT)).size > 5]).toEqual([[1], true]);
    // …is still most of the way there when the sheet has left the window (the next frame after `outMs`), and goes last
    const left = drawn.find((r) => r.sheetT === OFF)!;
    expect([left.t <= SHEET_MOTION.outMs + 20, Number(left.scrimO) > 0.5, falling(opacities(drawn))]).toEqual([true, true, true]);
    // and then nothing is drawn: the Modal unmounts once the fade has ended
    expect(rows.at(-1)).toBeNull();
  });

  it("the content stays while it leaves (a closed sheet slides out with what it showed, not empty)", () => {
    const { rerender } = render(<Sheet open onClose={() => {}} title="Layers" palette={LIGHT}><Text>choices</Text></Sheet>);
    act(() => { vi.advanceTimersByTime(1000); });
    rerender(<Sheet open={false} onClose={() => {}} title="" palette={LIGHT}>{null}</Sheet>);
    act(() => { vi.advanceTimersByTime(100); });
    expect([screen.getByRole("heading").textContent, screen.getByText("choices").textContent]).toEqual(["Layers", "choices"]);
  });

  it("reopened while closing: it turns round from where it is, never jumping back below the window", () => {
    const { rerender } = render(sheet(true));
    act(() => { vi.advanceTimersByTime(1000); });
    rerender(sheet(false));
    act(() => { vi.advanceTimersByTime(100); });
    rerender(sheet(true));
    const rows = sample(SHEET_MOTION.inMs + 60);
    const ys = rows.map((r) => Number(/translateY\((-?[\d.]+)px\)/.exec(r!.sheetT)![1]));
    expect([ys[0]! > 0 && ys[0]! < 1000, ys.at(-1), [...new Set(rows.map((r) => r!.scrimT))]]).toEqual([true, 0, [""]]);
  });

  it("a dialog closed on a wide screen draws nothing when the window then narrows to a phone (no invisible overlay)", () => {
    // SheetHost keeps its last sheet mounted closed, so the Sheet's state outlives the close: a wide close must leave it down
    const dlg = (open: boolean, layout: "desktop" | "phone") => (
      <Sheet dialog layout={layout} open={open} onClose={() => {}} title="Rule" palette={LIGHT}><Text>body</Text></Sheet>);
    const { rerender } = render(dlg(true, "desktop"));
    act(() => { vi.advanceTimersByTime(1000); });
    rerender(dlg(false, "desktop"));
    // the wide dialog's Modal fades out on a CSS animation that jsdom never ends: end it as a browser would (the Modal
    // listens on its own element and ignores bubbled ends, so fire on each ancestor in turn; React picks the prefixed name
    // when jsdom lacks AnimationEvent, so send both)
    for (let el: HTMLElement | null = screen.getByTestId("sheet-dialog"); el && el !== document.body; el = el.parentElement)
      for (const type of ["animationend", "webkitAnimationEnd"]) fireEvent(el, new Event(type, { bubbles: true }));
    act(() => { vi.advanceTimersByTime(1000); });
    expect(screen.queryByText("body")).toBeNull();
    rerender(dlg(false, "phone"));
    act(() => { vi.advanceTimersByTime(2000); });
    expect([screen.queryByTestId("sheet-scrim"), screen.queryByTestId("sheet-panel"), screen.queryByText("body")])
      .toEqual([null, null, null]);
    // and it still opens as a phone sheet afterwards
    rerender(dlg(true, "phone"));
    act(() => { vi.advanceTimersByTime(1000); });
    expect(screen.getByTestId("sheet-panel").style.transform).toBe("translateY(0px)");
  });

  it("reduced motion: nothing slides; the sheet and the scrim fade together, quickly", async () => {
    setReduceMotion(true);
    const { rerender } = render(sheet(false));
    await act(async () => {});                       // the platform answers the reduce-motion question (a promise)
    rerender(sheet(true));
    const open = sample(SHEET_MOTION.reducedMs + 40);
    expect([...new Set(open.map((r) => r!.sheetT))]).toEqual(["translateY(0px)"]);
    expect([Number(open[0]!.sheetO), Number(open.at(-1)!.sheetO || 1), Number(open.at(-1)!.scrimO || 1)]).toEqual([0, 1, 1]);
    rerender(sheet(false));
    const shut = sample(SHEET_MOTION.reducedMs + 60);
    expect([...new Set(shut.filter((r) => r).map((r) => r!.sheetT))]).toEqual(["translateY(0px)"]);
    expect(shut.at(-1)).toBeNull();
  });
});
