/**
 * (T248, T257) The first-run walkthrough — coach marks on the real controls: once per device, skippable, sequenced behind the
 * first-run notice, re-openable; and the Help page drawn from the legend tokens.
 */
import { afterEach, describe, expect, it } from "vitest";
import { act, cleanup, fireEvent, render, renderHook, screen } from "@testing-library/react";
import { HELP_WORDS, WALKTHROUGH_VERSION, WALKTHROUGH_WORDS, helpVM } from "@app/core";
import { tidalColours } from "@app/map";
import { HelpPage } from "./menu/HelpPage";
import { useWalkthrough, WalkthroughSheet, type WalkthroughStore } from "./Walkthrough";
import { insideWindow, placeCallout, unionRect, useCoachRegistry, type CoachRegistry } from "./CoachMarks";
import { DARK, LIGHT } from "./theme";

afterEach(cleanup);
const memory = (v: string | null = null): WalkthroughStore & { v: string | null } => {
  const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } };
  return s;
};
const w = WALKTHROUGH_WORDS;

/** a registry with every control on screen at a made-up place (jsdom lays nothing out) */
function screenOf(missing: readonly string[] = []): CoachRegistry {
  const reg = useCoachRegistryForTest();
  const at: Record<string, [number, number, number, number]> = {
    days: [14, 70, 200, 34], modes: [230, 70, 290, 34], map: [0, 0, 1024, 700], "tab:conditions": [300, 720, 120, 48],
    "tab:runs": [420, 720, 120, 48],
    sync: [400, 10, 44, 44], bell: [356, 10, 44, 44],
  };
  for (const [k, r] of Object.entries(at)) if (!missing.includes(k))
    reg.set(k, { measureInWindow: (cb) => cb(...r) });
  return reg;
}
function useCoachRegistryForTest(): CoachRegistry {
  return renderHook(() => useCoachRegistry()).result.current;
}
const steps = (missing: readonly string[] = []) => w.cards.filter((c) => !c.targets.every((t) => missing.includes(t)));

describe("the walkthrough (T248, coach marks T257)", () => {
  it("opens on a new device and walks the steps on the real controls; the last stores the version and closes it", async () => {
    const store = memory();
    const reg = screenOf();
    const { result } = renderHook(() => useWalkthrough(store, false));
    expect(result.current.open).toBe(true);
    const { rerender } = render(<WalkthroughSheet state={result.current} palette={LIGHT} registry={reg} />);
    for (let i = 0; i < w.cards.length - 1; i++) {
      expect(await screen.findByTestId(`coach-step-${w.cards[i]!.key}`, {}, { timeout: 3000 })).toBeTruthy();
      expect(screen.getByTestId("coach-count").textContent).toBe(w.step(i + 1, w.cards.length));
      expect(screen.getByTestId("coach-spotlight")).toBeTruthy();
      act(() => { fireEvent.click(screen.getByText(w.next)); });
      rerender(<WalkthroughSheet state={result.current} palette={LIGHT} registry={reg} />);
    }
    expect(await screen.findByTestId("coach-step-sync", {}, { timeout: 3000 })).toBeTruthy();
    expect(screen.queryByText(w.skip)).toBeNull();
    expect(store.v).toBeNull();
    act(() => { fireEvent.click(screen.getByText(w.done)); });
    expect([store.v, result.current.open]).toEqual([WALKTHROUGH_VERSION, false]);
  });

  it("puts the spotlight on the control, and the callout beside it", async () => {
    const { result } = renderHook(() => useWalkthrough(memory(), false));
    render(<WalkthroughSheet state={result.current} palette={LIGHT} registry={screenOf()} />);
    await screen.findByTestId("coach-step-days", {}, { timeout: 3000 });
    const ring = screen.getByTestId("coach-spotlight");
    // the date bar at (14, 70) 200 × 34, with 6 px of room and the 2 px ring round it
    expect([ring.style.left, ring.style.top, ring.style.width, ring.style.height]).toEqual(["6px", "62px", "216px", "50px"]);
    // more room below it than above: the callout hangs under it
    expect(parseFloat(screen.getByTestId("coach-step-days").style.top)).toBeGreaterThan(70 + 34);
  });

  it("Back returns a step; a step whose control is not on screen is dropped (no Standard / Salmon offered)", async () => {
    const { result } = renderHook(() => useWalkthrough(memory(), false));
    render(<WalkthroughSheet state={result.current} palette={LIGHT} registry={screenOf(["modes"])} />);
    await screen.findByTestId("coach-step-days", {}, { timeout: 3000 });
    const n = steps(["modes"]).length;
    expect(screen.getByTestId("coach-count").textContent).toBe(w.step(1, n));
    act(() => { fireEvent.click(screen.getByText(w.next)); });
    expect(await screen.findByTestId("coach-step-map", {}, { timeout: 3000 })).toBeTruthy();
    act(() => { fireEvent.click(screen.getByText(w.back)); });
    expect(await screen.findByTestId("coach-step-days", {}, { timeout: 3000 })).toBeTruthy();
  });

  it("the bell and the refresh button are one step, the bell kept where it is beside the button", async () => {
    const { result } = renderHook(() => useWalkthrough(memory(), false));
    render(<WalkthroughSheet state={result.current} palette={LIGHT} registry={screenOf()} />);
    await screen.findByTestId("coach-step-days", {}, { timeout: 3000 });
    for (let i = 0; i < w.cards.length - 1; i++) act(() => { fireEvent.click(screen.getByText(w.next)); });
    await screen.findByTestId("coach-step-sync", {}, { timeout: 3000 });
    const ring = screen.getByTestId("coach-spotlight");
    // the union of the bell (356) and the button (400 + 44): 88 wide, plus room and ring
    expect([ring.style.left, ring.style.width]).toEqual(["348px", "104px"]);
  });

  it("is skippable from the first step, and a skip counts as seen", async () => {
    const store = memory();
    const { result } = renderHook(() => useWalkthrough(store, false));
    render(<WalkthroughSheet state={result.current} palette={LIGHT} registry={screenOf()} />);
    await screen.findByTestId("coach-step-days", {}, { timeout: 3000 });
    act(() => { fireEvent.click(screen.getByText(w.skip)); });
    expect([store.v, result.current.open]).toEqual([WALKTHROUGH_VERSION, false]);
  });

  it("stays down once seen, and with no store (a test, a screen alone)", () => {
    expect(renderHook(() => useWalkthrough(memory(WALKTHROUGH_VERSION), false)).result.current.open).toBe(false);
    expect(renderHook(() => useWalkthrough(null, false)).result.current.open).toBe(false);
    expect(renderHook(() => useWalkthrough(memory("walkthrough-1"), false)).result.current.open).toBe(true);
  });

  it("waits for the first-run notice (and the Map tab): held while blocked, there the moment it is not — never both", () => {
    const store = memory();
    const { result, rerender } = renderHook(({ blocked }) => useWalkthrough(store, blocked), { initialProps: { blocked: true } });
    expect(result.current.open).toBe(false);
    rerender({ blocked: false });
    expect(result.current.open).toBe(true);
  });

  it("re-opens on request, from the Help page, even on a device that has seen it — once nothing blocks it", () => {
    const store = memory(WALKTHROUGH_VERSION);
    const { result, rerender } = renderHook(({ blocked }) => useWalkthrough(store, blocked), { initialProps: { blocked: true } });
    act(() => result.current.reopen());
    expect(result.current.open).toBe(false);
    rerender({ blocked: false });
    expect(result.current.open).toBe(true);
    act(() => result.current.finish());
    expect(result.current.open).toBe(false);
  });
});

describe("coach-mark geometry (T257)", () => {
  it("the callout sits under a control near the top, over one near the foot, inside the gutters on a phone", () => {
    const top = placeCallout({ x: 10, y: 60, w: 100, h: 40 }, { w: 390, h: 844 });
    expect([top.below, top.top, top.left, top.width]).toEqual([true, 114, 16, 358]);
    const foot = placeCallout({ x: 100, y: 780, w: 80, h: 50 }, { w: 390, h: 844 });
    expect([foot.below, foot.bottom]).toEqual([false, 844 - 780 + 14]);
    const wide = placeCallout({ x: 1000, y: 10, w: 44, h: 44 }, { w: 1280, h: 800 });
    expect([wide.width, wide.left]).toEqual([360, 1022 - 180]);
    const edge = placeCallout({ x: 1230, y: 10, w: 44, h: 44 }, { w: 1280, h: 800 });
    expect(edge.left).toBe(1280 - 360 - 16);
  });
  it("keeps the spotlight inside the window, ring and all (a tab at the foot)", () => {
    expect(insideWindow({ x: 174, y: 788, w: 132, h: 60 }, { w: 390, h: 844 })).toEqual({ x: 174, y: 788, w: 132, h: 54 });
    expect(insideWindow({ x: -6, y: -6, w: 50, h: 50 }, { w: 390, h: 844 })).toEqual({ x: 2, y: 2, w: 42, h: 42 });
  });
  it("unions the rects it is given", () => {
    expect(unionRect([{ x: 10, y: 10, w: 10, h: 10 }, { x: 30, y: 5, w: 10, h: 10 }])).toEqual({ x: 10, y: 5, w: 30, h: 15 });
    expect(unionRect([])).toBeNull();
  });
});

describe("the Help page (T248)", () => {
  it("draws every section and row of helpVM, in order", () => {
    render(<HelpPage palette={LIGHT} theme="light" />);
    expect(screen.getAllByTestId(/^help-section-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(helpVM().map((s) => `help-section-${s.key}`));
    expect(screen.getAllByTestId(/^help-row-/).map((e) => e.getAttribute("data-testid")))
      .toEqual(helpVM().flatMap((s) => s.rows.map((r) => `help-row-${r.key}`)));
    expect(screen.getByText(HELP_WORDS.lead)).toBeTruthy();
  });

  it("swatches wear the map's own colours in each theme — resolved, not copied", () => {
    for (const [palette, theme] of [[LIGHT, "light"], [DARK, "dark"]] as const) {
      const { unmount } = render(<HelpPage palette={palette} theme={theme} />);
      const closed = screen.getByTestId("help-row-status-closed").querySelector("div[style*='height: 4px']") as HTMLElement;
      expect(closed.style.backgroundColor).toBe(hexToRgb(palette.waterStatus.closed));
      expect(screen.getByTestId("help-tidal").style.backgroundColor).toBe(hexToRgb(tidalColours(theme).fill));
      expect(screen.getByTestId("help-edge-dashed")).toBeTruthy();
      expect(screen.getByTestId("help-edge-solid")).toBeTruthy();
      unmount();
    }
  });

  it("the button shows the walkthrough again; with no handler there is no button", () => {
    let n = 0;
    const { rerender } = render(<HelpPage palette={LIGHT} theme="light" onWalkthrough={() => { n++; }} />);
    fireEvent.click(screen.getByText(w.again));
    expect(n).toBe(1);
    rerender(<HelpPage palette={LIGHT} theme="light" />);
    expect(screen.queryByTestId("help-walkthrough")).toBeNull();
  });
});

function hexToRgb(hex: string): string {
  const m = /^#([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i.exec(hex);
  if (!m) return hex;
  return `rgb(${parseInt(m[1]!, 16)}, ${parseInt(m[2]!, 16)}, ${parseInt(m[3]!, 16)})`;
}
