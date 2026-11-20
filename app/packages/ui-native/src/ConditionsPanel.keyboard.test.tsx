/**
 * (T723) A water's Conditions → Alert me: the value field takes a decimal pad, and on the iPhone simulator the pad
 * covered it (field 780-821 pt, keyboard top 566 pt). The panel's own scroll now carries the two props the menu pages
 * and the spot page carry: `automaticallyAdjustKeyboardInsets` (iOS insets the scroll by the keyboard and brings the
 * focused field above it) and `keyboardShouldPersistTaps="handled"` (a tap on a control with the keyboard up acts at
 * once). Both forms of the page's scroll (with the water's view swipe and without). The props reach React Native's
 * ScrollView, which the browser build drops, so the ScrollView is watched here; how it looks is the simulator's.
 */
import { createElement, forwardRef } from "react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { cleanup, render, waitFor } from "@testing-library/react";
import type { SectionId, WaterSource } from "@app/data";
import { clearPanelCache } from "@app/ui";
import { LIGHT } from "./theme";
import { WaterViewsContext, type WaterViews } from "./ViewSwipe";

const scrolls: Record<string, unknown>[] = [];
vi.mock("react-native", async (importOriginal) => {
  const rn = await importOriginal<typeof import("react-native")>();
  const Spy = forwardRef<unknown, Record<string, unknown>>(function ScrollView(p, ref) {
    scrolls.push(p);
    return createElement(rn.ScrollView as never, { ...p, ref });
  });
  return { ...rn, ScrollView: Spy };
});
const { ConditionsPanel } = await import("./ConditionsPanel");

afterEach(() => { cleanup(); clearPanelCache(); scrolls.length = 0; });

const SEC = 7 as SectionId;
const source = {
  panelsFor: async () => new Map(),
  panelRoutes: async () => [],
  lakeStationsFor: async () => new Map(),
  gaugeForSection: async () => null,
  traceToGauge: async () => [],
  gaugeSeries: async () => null,
  gaugeParameters: async () => [],
  gaugeNow: async () => null,
} as unknown as WaterSource;

/** the page's own scroll: the first ScrollView the panel draws (its body's are inside it) */
const pageScroll = () => scrolls.find((p) => typeof p.onScroll === "function");

describe("<ConditionsPanel> the Alert me field above the keyboard (T723)", () => {
  it("the page's scroll is inset by the keyboard and keeps a tap with the keyboard up", async () => {
    render(<ConditionsPanel source={source} section={SEC} palette={LIGHT} day={new Date(2026, 8, 4, 12)} />);
    await waitFor(() => expect(pageScroll()).toBeTruthy());
    expect([pageScroll()!.automaticallyAdjustKeyboardInsets, pageScroll()!.keyboardShouldPersistTaps]).toEqual([true, "handled"]);
  });

  it("the same in a water's views, where the body carries the view swipe", async () => {
    const views = { bodySwipe: { onSwipe: () => {} } } as unknown as WaterViews;
    render(<WaterViewsContext.Provider value={views}>
             <ConditionsPanel source={source} section={SEC} palette={LIGHT} day={new Date(2026, 8, 4, 12)} />
           </WaterViewsContext.Provider>);
    await waitFor(() => expect(pageScroll()).toBeTruthy());
    expect([pageScroll()!.automaticallyAdjustKeyboardInsets, pageScroll()!.keyboardShouldPersistTaps]).toEqual([true, "handled"]);
  });
});
