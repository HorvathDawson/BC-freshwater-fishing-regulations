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
import { HOME, LayoutWidthContext, type Route } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({ MapScreen: () => <View testID="map-stub"><Text>map</Text></View> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
vi.mock("./SpotsScreen", () => ({ SpotsScreen: () => { throw new Error("the spots list fell over"); } }));
// the Sources page and a sheet of the Shell's own, each made to throw on a switch (the test's, below)
const boom = { sources: false, sheet: false };
vi.mock("./menu/InfoPages", async (orig) => ({ ...(await orig<object>()),
  SourcesPage: () => { if (boom.sources) throw new Error("the sources page fell over"); return <Text testID="sources-ok">sources</Text>; } }));
vi.mock("./ObstacleSheet", () => ({ ObstacleSheet: () => { if (boom.sheet) throw new Error("a sheet fell over"); return null; } }));
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
afterEach(() => { restore(); vi.restoreAllMocks(); boom.sources = false; boom.sheet = false; });

const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });

function mount(start: Route) {
  render(
    <LayoutWidthContext.Provider value={390}><GestureHandlerRootView>
      <Shell source={makeFixtureSource()} loader={openRegsFixtureWithLines()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} systemBack={() => ({ sync: () => {}, dispose: () => {} })}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView></LayoutWidthContext.Provider>);
}
const press = (name: string | RegExp) => act(() => { fireEvent.click(screen.getAllByLabelText(name)[0]!); });

describe("(T1164) a screen's fault in the real Shell", () => {
  it("the Spots tab says it could not be drawn, reports once, and the tab bar still reaches the Map", async () => {
    mount(HOME);
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

  it("a More page that throws shows its own line, reports once, and the map, the tab bar and Go back still work", async () => {
    boom.sources = true;
    mount({ ...HOME, tab: "more" });
    await settle();
    press("Sources and licences");
    await settle();
    expect(screen.getByRole("alert").textContent).toBe("This page hit a problem and could not be drawn.");
    expect(reports).toHaveLength(1);
    expect((reports[0] as Error).message).toBe("the sources page fell over");
    // the tab bar is whole: the map is one press away, and the page's own Go back puts the page away
    press("Go back");
    await settle();
    expect(screen.queryByRole("alert")).toBeNull();
    press("Map");
    await settle();
    expect(screen.getByTestId("map-stub").textContent).toBe("map");
    expect(reports).toHaveLength(1);
  });

  it("a sheet that throws shows a band at the foot, reports once, the map and tabs work, and Close puts it away", async () => {
    boom.sheet = true;
    mount(HOME);
    await settle();
    expect(screen.getByRole("alert").textContent).toBe("A panel hit a problem and could not be drawn.");
    expect(reports).toHaveLength(1);
    expect((reports[0] as Error).message).toBe("a sheet fell over");
    expect(screen.getByTestId("map-stub").textContent).toBe("map");
    press("Conditions");
    await settle();
    expect(screen.getByLabelText("Conditions").getAttribute("aria-selected")).toBe("true");
    boom.sheet = false;
    fireEvent.click(screen.getByTestId("screen-fault-close"));
    await settle();
    expect(screen.queryByRole("alert")).toBeNull();
    expect(reports).toHaveLength(1);
  });
});
