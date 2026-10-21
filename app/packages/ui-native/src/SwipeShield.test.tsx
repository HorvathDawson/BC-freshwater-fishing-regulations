/**
 * THE SHIELDED THINGS on a swiping body (reviewer 2026-10-21: the Conditions body swipes, its maps and chart do not).
 * `gestures.test.tsx` proves the shield itself; this pins that the minimap and the chart's plot wear one, so a pan on
 * the route map, the gauge's map or the plot is never taken for a change of view — while the chart's key and caption,
 * being text, still swipe.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { fireGestureHandler, getByGestureTestId } from "react-native-gesture-handler/jest-utils";
import { Text } from "react-native";
import { buildHydrograph } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("@app/map", async (orig) => ({ ...(await orig<Record<string, unknown>>()), Map: () => null }));
const { MiniMap } = await import("./MiniMap");
const { ViewSwipe } = await import("./ViewSwipe");
const { Hydrograph } = await import("./Hydrograph");
const SHAPE = buildHydrograph({ values: Array(72).fill(15), bands: Array(72).fill([8, 12, 18, 26, 40]),
                                xLabels: ["3d", "2d", "1d", "now"] });

afterEach(cleanup);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  window.dispatchEvent(new Event("resize"));
});

const drag = (pressOn: string | ((c: HTMLElement) => Element) | null) => {
  const release = vi.fn(() => null);
  render(
    <GestureHandlerRootView>
      <ViewSwipe swipe={{ enabled: true, starts: () => true, offset: (dx) => dx, release }}>
        <Text>the estimate</Text>
        <MiniMap at={{ atlas: "a", basemap: "b" } as never} palette={LIGHT} theme="light" hint="the map"
                 camera={{ lat: 49, lon: -122, zoom: 9 } as never} />
        <Hydrograph shape={SHAPE} palette={LIGHT} colour={LIGHT.live} caption="the caption" />
      </ViewSwipe>
    </GestureHandlerRootView>);
  const t = { identifier: 1, force: 1, clientX: 200, clientY: 100, pageX: 200, pageY: 100, target: null };
  const target = pressOn === null ? null
    : typeof pressOn === "string" ? screen.getByText(pressOn) : pressOn(document.body);
  if (target) act(() => { fireEvent.touchStart(target, { touches: [t], changedTouches: [t] }); });
  act(() => {
    fireGestureHandler(getByGestureTestId("view-swipe"), [
      { absoluteX: 200, translationX: 0, translationY: 0 },
      { absoluteX: 80, translationX: -120, translationY: 0, velocityX: 0 }]);
  });
  cleanup();
  return release.mock.calls.length;
};

describe("the minimap and the chart on a swiping body", () => {
  it("a drag that starts on the map or the plot is theirs; one on the text around them moves the view", () => {
    const plot = (c: HTMLElement) => c.querySelector("svg path")!;
    expect([drag("the map"), drag(plot), drag("the estimate"), drag("the caption"), drag(null)])
      .toEqual([0, 0, 1, 1, 1]);
  });
});
