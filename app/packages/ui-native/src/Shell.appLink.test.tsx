/**
 * (T1038) A PHONE LINK'S DAY, HORIZON, PAGE START AND `whole`, through the real Shell's one open request: they land in the
 * same state the web's start link sets at a page's start (`initialDay`, `initialHorizon`, `regsStart`, the frame's `whole`).
 * The map and the water host are stubs: the host draws the start and day it is handed.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { Horizon } from "@app/core";
import type { ItemId } from "@app/data";
import { HOME, waterFrame, type RegsStart, type Route } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({ MapScreen: () => <Text testID="map">map</Text> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
vi.mock("./shell/WaterModeHost", () => ({
  WaterModeHost: (p: { start: RegsStart | null; mapDay: Date | null }) =>
    <Text testID="host">{JSON.stringify({ start: p.start, day: p.mapDay?.toDateString() ?? null })}</Text>,
}));
const { Shell } = await import("./Shell");
afterEach(cleanup);

type Req = { item: string; n: number; view?: "regs" | "map"; day?: Date | null; horizon?: Horizon; regs?: RegsStart; whole?: boolean };
const CHIL = "gnis:8634" as ItemId;
const XMAS = new Date(2026, 11, 24, 12);

describe("(T1038) a link's keys through the Shell's open request", () => {
  it("day, horizon, page start and whole land where the web's start props put them; a later tap without them leaves them", async () => {
    const routes: Route[] = [];
    const addresses: { day: Date | null; horizon: Horizon }[] = [];
    const tree = (openRequest: Req | null) => (
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} onRoute={(r: Route) => routes.push(r)}
               onAddress={(a) => addresses.push(a)} openRequest={openRequest} />
      </GestureHandlerRootView>);
    const { rerender } = render(tree(null));
    await act(async () => {});
    rerender(tree({ item: CHIL, n: 1, view: "map", day: XMAS, horizon: 3, regs: { tab: "gear", sheet: "pos:pos", scroll: 40 }, whole: true }));
    await act(async () => { await Promise.resolve(); });
    const frame = waterFrame(routes[routes.length - 1]!);
    expect([frame?.item, frame?.view, frame?.whole]).toEqual([CHIL, "map", true]);
    expect(addresses[addresses.length - 1]).toEqual({ day: XMAS, horizon: 3 });
    expect(JSON.parse(screen.getByTestId("host").textContent!)).toEqual({ start: { tab: "gear", sheet: "pos:pos", scroll: 40 }, day: XMAS.toDateString() });
    // a notification's tap names no day, horizon or start: what the link set stands
    rerender(tree({ item: CHIL, n: 2, view: "regs" }));
    await act(async () => { await Promise.resolve(); });
    expect(addresses[addresses.length - 1]).toEqual({ day: XMAS, horizon: 3 });
    // a link naming today and Now puts them back (the web's start on no `date` / `horizon`)
    rerender(tree({ item: CHIL, n: 3, view: "regs", day: null, horizon: 0, regs: { tab: "keep", sheet: null, scroll: 0 } }));
    await act(async () => { await Promise.resolve(); });
    expect(addresses[addresses.length - 1]).toEqual({ day: null, horizon: 0 });
  });
});
