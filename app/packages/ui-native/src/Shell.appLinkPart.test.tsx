/**
 * (T1038 review) A LINK TO ANOTHER PART OF THE WATER ON SCREEN: its page start is the linked part's page's, never the page
 * still on screen while the open waits on the part check (`opener.item` → the water's part count, a real release read
 * here). The water host is a stub that takes a start as a page does (once, then `onStarted`) and logs which part took it.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { useEffect } from "react";
import { act, cleanup, render, waitFor } from "@testing-library/react";
import { Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import { HOME, openWater, reduce, type RegsStart, type Route } from "@app/ui";
import { LIGHT } from "./theme";

const took: { part: number | null; start: RegsStart }[] = [];
vi.mock("./MapScreen", () => ({ MapScreen: () => <Text testID="map">map</Text> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
vi.mock("./shell/WaterModeHost", () => ({
  WaterModeHost: (p: { start: RegsStart | null; frame: { part: number | null }; onStarted: () => void }) => {
    useEffect(() => { if (p.start) { took.push({ part: p.frame.part, start: p.start }); p.onStarted(); } }, [p.start]);   // eslint-disable-line react-hooks/exhaustive-deps
    return <Text testID="host">{String(p.frame.part)}</Text>;
  },
}));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");
afterEach(() => { cleanup(); took.length = 0; });

const CHIL = "gnis:8634" as ItemId;
type Req = { item: string; n: number; view?: "regs"; part?: number | null; regs?: RegsStart };

describe("(T1038 review) a link to another part of the water on screen", () => {
  it("its start goes to the linked part's page, not to the part still on screen while the open is checked", async () => {
    const routes: Route[] = [];
    const loader = openRegsFixtureWithLines();
    const tree = (openRequest: Req | null) => (
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}} loader={loader}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={reduce(HOME, openWater({ item: CHIL, part: 0, view: "regs" }))}
               onRoute={(r: Route) => routes.push(r)} openRequest={openRequest} />
      </GestureHandlerRootView>);
    const { rerender } = render(tree(null));
    await act(async () => {});
    const gear: RegsStart = { tab: "gear", sheet: null, scroll: 0 };
    rerender(tree({ item: CHIL, n: 1, view: "regs", part: 1, regs: gear }));
    await waitFor(() => expect(took.length).toBeGreaterThan(0), { timeout: 20_000 });
    await act(async () => { await new Promise((r) => setTimeout(r, 100)); });
    expect(took).toEqual([{ part: 1, start: gear }]);
  });

  it("an open overtaken while its part is checked hands its start to no page", async () => {
    const loader = openRegsFixtureWithLines();
    const tree = (openRequest: Req | null) => (
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}} loader={loader}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={reduce(HOME, openWater({ item: CHIL, part: 0, view: "regs" }))}
               openRequest={openRequest} />
      </GestureHandlerRootView>);
    const { rerender } = render(tree(null));
    await act(async () => {});
    rerender(tree({ item: CHIL, n: 11, view: "regs", part: 1, regs: { tab: "gear", sheet: null, scroll: 0 } }));
    rerender(tree({ item: CHIL, n: 12, view: "regs" }));                  // a tap at once: the part check is overtaken
    await act(async () => { await new Promise((r) => setTimeout(r, 1500)); });
    expect(took).toEqual([]);
  });
});
