/**
 * (T1140) THE HEADS' LINE TO THE OTHER PARTS, wired by the Shell: the water frame's `onParts` (`WaterViewsContext`, what
 * the Regulations and Conditions heads press) moves the frame onto the Map with its list of parts open (`WaterFrame.parts`,
 * which `WaterModeHost` hands the map as `listOpen`). An unnamed reach has no Map, so no `onParts`. The water host is a stub
 * that says its frame and offers the frame's press.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { useContext } from "react";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Pressable, Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId, SectionId } from "@app/data";
import { HOME, openWater, reduce, type Route } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({ MapScreen: () => <Text testID="map">map</Text> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
vi.mock("./shell/WaterModeHost", async () => {
  const { WaterViewsContext } = await import("./ViewSwipe");
  return {
    WaterModeHost: (p: { frame: { view: string; parts?: true } }) => {
      const views = useContext(WaterViewsContext);
      return (
        <>
          <Text testID="host">{`${p.frame.view}${p.frame.parts ? " parts" : ""}`}</Text>
          {views?.onParts && <Pressable testID="line" onPress={views.onParts}><Text>line</Text></Pressable>}
        </>
      );
    },
  };
});
const { Shell } = await import("./Shell");
afterEach(cleanup);

const mount = (route: Route) => render(
  <GestureHandlerRootView>
    <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}} loader={null}
           tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={route} />
  </GestureHandlerRootView>);

describe("(T1140) the heads' line to the other parts", () => {
  it("a named water's Regulations view: the line opens its Map with the list of parts open", async () => {
    mount(reduce(HOME, openWater({ item: "gnis:8634" as ItemId, part: 1, view: "regs" })));
    await act(async () => {});
    expect(screen.getByTestId("host").textContent).toBe("regs");
    fireEvent.click(screen.getByTestId("line"));
    await act(async () => {});
    expect(screen.getByTestId("host").textContent).toBe("map parts");
  });

  it("an unnamed reach has no Map: no line to press", async () => {
    mount(reduce(HOME, openWater({ item: null, tapSid: 7 as SectionId, view: "regs" })));
    await act(async () => {});
    expect([screen.getByTestId("host").textContent, screen.queryByTestId("line")]).toEqual(["regs", null]);
  });
});
