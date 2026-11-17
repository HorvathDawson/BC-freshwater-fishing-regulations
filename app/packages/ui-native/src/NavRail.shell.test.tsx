/**
 * (T839) THE NAVIGATION RAIL: at the desktop breakpoint (1280) Map · Conditions · Spots · More are a left rail and the
 * bottom bar is not drawn; at 1279 and on a phone the bottom bar, as ever. Regs mode (a water's page) has neither, the
 * same rule `bottomBarShown` gives the bar. Through the real Shell on the fixture; the map renderer is a stub.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import type { ItemId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, LayoutWidthContext, openWater, reduce, tabsAsRail, type Route } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({ MapScreen: () => <View testID="map-stub"><Text>map</Text></View> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");
afterEach(cleanup);

function mount(start: Route, width: number) {
  render(
    <LayoutWidthContext.Provider value={width}><GestureHandlerRootView>
      <Shell source={makeFixtureSource()} loader={openRegsFixtureWithLines()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} systemBack={() => ({ sync: () => {}, dispose: () => {} })}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView></LayoutWidthContext.Provider>);
}
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
const names = () => screen.queryAllByRole("tab").map((t) => t.getAttribute("aria-label"));

describe("(T839) where the tabs are drawn", () => {
  it("only the desktop class is a rail", () => {
    expect(["phone", "tablet", "desktop"].map((c) => tabsAsRail(c as never))).toEqual([false, false, true]);
  });

  it("1280: a left rail with the tabs, the current one selected; no bottom bar", async () => {
    mount(HOME, 1280); await settle();
    const rail = screen.getByTestId("nav-rail");
    expect(rail.querySelectorAll('[role="tab"]').length).toBe(5);
    expect(names()).toEqual(["Map", "Conditions", "Runs", "Spots", "More"]);   // four, so the bottom bar is not also there
    expect(screen.getByLabelText("Map").getAttribute("aria-selected")).toBe("true");
    expect(screen.getByLabelText("Conditions").getAttribute("aria-selected")).toBe("false");
  });

  it("1280: pressing a rail tab changes the tab, as the bar's press does", async () => {
    mount(HOME, 1280); await settle();
    act(() => { fireEvent.click(screen.getByLabelText("Conditions")); });
    await settle();
    expect(screen.getByLabelText("Conditions").getAttribute("aria-selected")).toBe("true");
    act(() => { fireEvent.click(screen.getByLabelText("Spots")); });
    await settle();
    expect(screen.getByLabelText("Spots").getAttribute("aria-selected")).toBe("true");
  });

  it.each([390, 899, 1279])("%i: the bottom bar, no rail", async (w) => {
    mount(HOME, w); await settle();
    expect(screen.queryByTestId("nav-rail")).toBeNull();
    expect(names()).toEqual(["Map", "Conditions", "Runs", "Spots", "More"]);
  });

  it.each([1280, 390])("%i: regs mode (a water's page) has neither the rail nor the bar", async (w) => {
    mount(reduce(HOME, openWater({ item: "gnis:8634" as ItemId, part: 1, view: "regs" })), w); await settle();
    expect(screen.queryByTestId("nav-rail")).toBeNull();
    expect(names()).not.toContain("Spots");   // the water's own view tabs are not the app's
    expect(names()).not.toContain("More");
  });
});
