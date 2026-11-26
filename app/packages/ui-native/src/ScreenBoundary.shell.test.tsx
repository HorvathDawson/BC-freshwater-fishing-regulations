/**
 * (T1164) THROUGH THE REAL SHELL: one screen throws while it draws (the Spots tab); the shell says so on that screen, reports it
 * once, and the tab bar still switches to another tab. Same setup as NavRail.shell.test.tsx (the fixture source and
 * regs loader; the map renderer is a stub); only the Spots screen is replaced, by one that throws.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { setFailureSink } from "@app/core";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, LayoutWidthContext } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({ MapScreen: () => <View testID="map-stub"><Text>map</Text></View> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
vi.mock("./SpotsScreen", () => ({ SpotsScreen: () => { throw new Error("the spots list fell over"); } }));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");

afterEach(cleanup);
const reports: unknown[] = [];
let restore: () => void;
beforeEach(() => {
  reports.length = 0;
  restore = setFailureSink((e) => { reports.push(e); });
  vi.spyOn(console, "error").mockImplementation(() => undefined);
});
afterEach(() => { restore(); vi.restoreAllMocks(); });

const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });

describe("(T1164) a screen's fault in the real Shell", () => {
  it("the Spots tab says it could not be drawn, reports once, and the tab bar still reaches the Map", async () => {
    render(
      <LayoutWidthContext.Provider value={390}><GestureHandlerRootView>
        <Shell source={makeFixtureSource()} loader={openRegsFixtureWithLines()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} systemBack={() => ({ sync: () => {}, dispose: () => {} })}
               feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
               spots={[]} onSaveSpot={async () => {}} />
      </GestureHandlerRootView></LayoutWidthContext.Provider>);
    await settle();
    expect(screen.getByTestId("map-stub").textContent).toBe("map");
    act(() => { fireEvent.click(screen.getByLabelText("Spots")); });
    await settle();
    expect(screen.getByRole("alert").textContent).toBe("Spots hit a problem and could not be drawn.");
    expect(reports).toHaveLength(1);
    expect((reports[0] as Error).message).toBe("the spots list fell over");
    act(() => { fireEvent.click(screen.getByLabelText("Map")); });
    await settle();
    expect(screen.queryByRole("alert")).toBeNull();
    expect(screen.getByLabelText("Map").getAttribute("aria-selected")).toBe("true");
    expect(screen.getByTestId("map-stub").textContent).toBe("map");
    expect(reports).toHaveLength(1);
  });
});
