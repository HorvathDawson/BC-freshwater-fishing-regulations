/**
 * (T1038) A PHONE LINK'S DAY, HORIZON, PAGE START AND `whole`, through the real Shell's one open request: they land in the
 * same state the web's start link sets at a page's start (`initialDay`, `initialHorizon`, `regsStart`, the frame's `whole`).
 * The map and the water host are stubs: the host draws the start and day it is handed.
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
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
  WaterModeHost: (p: { start: RegsStart | null; mapDay: Date | null; frame: { item: string | null }; nav: { back: () => void } }) =>
    <Text testID="host" onPress={() => p.nav.back()}>{JSON.stringify({ item: p.frame.item, start: p.start, day: p.mapDay?.toDateString() ?? null })}</Text>,
}));
const { Shell } = await import("./Shell");
afterEach(cleanup);

type Req = { item: string | null; home?: boolean; n: number; view?: "regs" | "map"; day?: Date | null; horizon?: Horizon; regs?: RegsStart; whole?: boolean };
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
    expect(JSON.parse(screen.getByTestId("host").textContent!)).toEqual({ item: CHIL, start: { tab: "gear", sheet: "pos:pos", scroll: 40 }, day: XMAS.toDateString() });
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

describe("(T1038) the start is the linked water's, and a day-only link opens the map", () => {
  const VEDDER = "gnis:3062" as ItemId;
  const mount = (routes: Route[], addresses: { day: Date | null; horizon: Horizon }[]) => {
    const tree = (openRequest: Req | null) => (
      <GestureHandlerRootView>
        <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
               tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} onRoute={(r: Route) => routes.push(r)}
               onAddress={(a) => addresses.push(a)} openRequest={openRequest} />
      </GestureHandlerRootView>);
    return tree;
  };
  // (the Shell opens each request count once per run: the counts here go on from the tests above)
  const host = () => JSON.parse(screen.getByTestId("host").textContent!) as { item: string; start: unknown };

  it("a link's start is not handed to another water's page (back under the linked water: none)", async () => {
    const routes: Route[] = [];
    const tree = mount(routes, []);
    const { rerender } = render(tree(null));
    await act(async () => {});
    rerender(tree({ item: VEDDER, n: 11, view: "regs" }));
    await act(async () => { await Promise.resolve(); });
    rerender(tree({ item: CHIL, n: 12, view: "regs", regs: { tab: "gear", sheet: null, scroll: 0 } }));
    await act(async () => { await Promise.resolve(); });
    expect(host()).toEqual({ item: CHIL, start: { tab: "gear", sheet: null, scroll: 0 }, day: null });
    await act(async () => { fireEvent.click(screen.getByTestId("host")); });   // back: Vedder's page, the start unapplied
    expect(host()).toEqual({ item: VEDDER, start: null, day: null });
  });

  it("`/?date=`: the browse map on that day, whatever was open", async () => {
    const routes: Route[] = [], addresses: { day: Date | null; horizon: Horizon }[] = [];
    const tree = mount(routes, addresses);
    const { rerender } = render(tree(null));
    await act(async () => {});
    rerender(tree({ item: VEDDER, n: 21, view: "regs" }));
    await act(async () => { await Promise.resolve(); });
    rerender(tree({ item: null, home: true, n: 22, day: XMAS, horizon: 3 }));
    await act(async () => { await Promise.resolve(); });
    expect([routes[routes.length - 1], addresses[addresses.length - 1]]).toEqual([HOME, { day: XMAS, horizon: 3 }]);
  });
});
