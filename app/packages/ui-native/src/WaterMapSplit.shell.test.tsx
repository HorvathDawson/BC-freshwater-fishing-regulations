/**
 * (T752) THE MAP TAB'S WIDE SPLIT: from the tablet breakpoint (900) the water's Map is the part's text on the left and the
 * map on the right, inside the frame's column; below it (899) the phone's layout, unchanged. Through the real Shell on the
 * fixture. The map renderer is a stub. jsdom has no layout, so the order and the place of each control in the tree is what
 * is pinned: the steps are in the text column OUTSIDE the parts list (which scrolls by itself), the text column comes
 * before the map (tab order: tabs, text, map).
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import type { ItemId } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME, LayoutWidthContext, openWater, reduce, type Route } from "@app/ui";
import { LIGHT } from "./theme";

vi.mock("./MapScreen", () => ({ MapScreen: () => <View testID="map-stub"><Text>map</Text></View> }));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixtureWithLines } = await import("@app/data/release/node");

const CHILLIWACK = "gnis:8634" as ItemId;
let width = 390;
const size = (w: number, h: number) => {
  width = w;
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: w });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: h });
  window.dispatchEvent(new Event("resize"));
};
afterEach(() => { cleanup(); size(390, 844); });
beforeAll(() => size(390, 844));

function mount(start: Route) {
  render(
    <LayoutWidthContext.Provider value={width}><GestureHandlerRootView>
      <Shell source={makeFixtureSource()} loader={openRegsFixtureWithLines()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} systemBack={() => ({ sync: () => {}, dispose: () => {} })}
             feed={{ index: async () => ({ fetchedAt: new Date().toISOString(), stations: {} }) } as never}
             spots={[]} onSaveSpot={async () => {}} />
    </GestureHandlerRootView></LayoutWidthContext.Provider>);
}
const settle = () => act(async () => { await new Promise((r) => setTimeout(r, 260)); });
async function open(w: number, h: number, part = 1) {
  size(w, h);
  mount(reduce(HOME, openWater({ item: CHILLIWACK, part, view: "map" })));
  await waitFor(() => expect(screen.queryByTestId("water-map-bar")).not.toBeNull(), { timeout: 20_000 });
  await settle();
}
const where = () => screen.getByTestId("water-map-where").textContent;

describe("(T752) the water's Map tab is a split from the tablet breakpoint up", () => {
  it("899 px is the phone's layout; 900 px is the split", async () => {
    await open(899, 900);
    expect(screen.queryByTestId("water-map-text")).toBeNull();
    expect(screen.queryByTestId("water-map-pane")).toBeNull();
    cleanup();
    await open(900, 900);
    expect(screen.getByTestId("water-map-text")).not.toBeNull();
    expect(screen.getByTestId("water-map-pane")).not.toBeNull();
  }, 60_000);

  it("the text column holds the name, the steps, Show regulations and the report link, and comes before the map", async () => {
    await open(1280, 720);
    const text = screen.getByTestId("water-map-text"), pane = screen.getByTestId("water-map-pane");
    expect(text.compareDocumentPosition(pane) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    for (const id of ["water-map-name", "water-map-regs", "report-link-water-map"]) expect(within(text).getByTestId(id)).not.toBeNull();
    expect(within(text).getAllByRole("button", { name: /stream|part/i }).length).toBeGreaterThan(1);
    expect(within(pane).getByTestId("map-stub")).not.toBeNull();
    expect(within(pane).getByTestId("water-map-whole")).not.toBeNull();
  }, 60_000);

  it("the parts list opens in place and scrolls inside the column; a click steps to the part and the steps stay outside it", async () => {
    await open(1280, 720);
    const text = screen.getByTestId("water-map-text");
    fireEvent.click(screen.getByTestId("water-map-parts-toggle"));
    await settle();
    const list = screen.getByTestId("water-map-parts-list");
    expect(text.contains(list)).toBe(true);
    const name = screen.getByTestId("water-map-name");
    // the steps are the column's, not the list's: the list scrolling never carries them away
    const steps = within(text).getAllByRole("button").filter((b) => /stream/i.test(b.getAttribute("aria-label") ?? ""));
    expect(steps.length).toBeGreaterThan(0);
    for (const s of steps) expect(list.contains(s)).toBe(false);
    expect(list.contains(name)).toBe(false);
    const before = where();
    const rows = within(list).getAllByRole("radio");
    expect(rows.length).toBeGreaterThan(1);
    const other = rows.find((r) => r.getAttribute("aria-checked") !== "true")!;
    fireEvent.click(other);
    await settle();
    expect(where()).not.toBe(before);
    expect(screen.getByTestId("water-map-bar")).not.toBeNull();
    expect(within(screen.getByTestId("water-map-text")).getAllByRole("button", { name: /stream/i }).length).toBeGreaterThan(0);
  }, 60_000);

  it("the phone (390) draws the map and the bar stacked, as before", async () => {
    await open(390, 844);
    expect(screen.queryByTestId("water-map-text")).toBeNull();
    const map = screen.getByTestId("map-stub"), bar = screen.getByTestId("water-map-bar");
    expect(map.compareDocumentPosition(bar) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  }, 60_000);
});
