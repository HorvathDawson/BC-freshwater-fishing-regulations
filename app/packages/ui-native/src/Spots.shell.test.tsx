/**
 * YOUR SPOTS, SWITCHED FROM THE LAYERS SHEET, THROUGH THE REAL SHELL (owner 2026-10-22: "toggle our spots on and off in
 * the layer menu").
 *
 *   - every saved spot is a pin on the Map tab and on the Conditions map, by default;
 *   - the Layers sheet on either map has a Spots switch; off, NO map is handed a spot pin (a searched town's pin is
 *     not a spot and stays);
 *   - the choice is kept: a reload over the same store opens with Spots off and no pins.
 *
 * The map renderer is replaced by a line listing the pins the Shell handed it (no WebGL in jsdom) — the props the real
 * `MapScreen` passes to `@app/map`'s `<Map pins>`. Everything else is the app's, on the fixture.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { Spot } from "@app/data/spots";
import type { ItemId } from "@app/data";
import { HOME, type BrowseTab, type LayerStore, type Route } from "@app/ui";
import { LIGHT } from "./theme";

type Pin = { lat: number; lon: number; tone?: string; title?: string };
type Stub = { onLayers?: () => void; pins?: readonly Pin[]; layersChanged?: number };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: Stub) => (
    <View>
      {p.onLayers && <Pressable accessibilityRole="button" accessibilityLabel="Choose layers"
                                onPress={p.onLayers}><Text>layers</Text></Pressable>}
      <Text testID="map-pins">{(p.pins ?? []).map((x) => `${x.title} @${x.lat},${x.lon} ${x.tone}`).join(" | ") || "none"}</Text>
      <Text testID="map-changed">{`changed ${p.layersChanged ?? 0}`}</Text>
    </View>),
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");
const { openRegsFixture } = await import("@app/data/release/node");

afterEach(cleanup);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

const VISIT = new Date(2026, 7, 30, 23, 30).getTime();
const spot = (over: Partial<Spot>): Spot => ({
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null,
  waterName: null, title: "Tamihi run", notes: "", photos: [], reading: null, weather: null, trace: null, panel: null,
  ...over,
});
const SPOTS = [spot({}), spot({ id: "s2", title: "", waterName: "Vedder River", lat: 49.1, lon: -122.0 })];

function memory(): LayerStore & { text: string | null } {
  const m = { text: null as string | null, read: () => m.text, write: (t: string) => { m.text = t; } };
  return m;
}

function mount(tab: BrowseTab, store: LayerStore) {
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={{ ...HOME, tab }} spots={SPOTS}
             layerStore={store} />
    </GestureHandlerRootView>);
}
const pins = () => screen.getByTestId("map-pins").textContent;
const spotsSwitch = () => {
  const s = screen.getByRole("switch", { name: "Spots" });
  return `Spots ${s.getAttribute("aria-checked") === "true" ? "on" : "off"}`;
};
const ON = `Tamihi run @49.0974,-121.9675 ${LIGHT.accent} | Vedder River @49.1,-122 ${LIGHT.accent}`;

describe("Spots on and off, from the Layers sheet", () => {
  it.each(["map", "conditions"] as const)("on the %s map: every spot pinned by default; off, none; on again, back",
    (tab) => {
      mount(tab, memory());
      const before = pins();
      act(() => { fireEvent.click(screen.getByLabelText("Choose layers")); });
      const row = spotsSwitch();
      act(() => { fireEvent.click(screen.getByRole("switch", { name: "Spots" })); });
      const off = [spotsSwitch(), pins(), screen.getByTestId("map-changed").textContent];
      act(() => { fireEvent.click(screen.getByRole("switch", { name: "Spots" })); });
      expect([before, row, off, pins()]).toEqual([ON, "Spots on", ["Spots off", "none", "changed 1"], ON]);
    });

  it("KEPT ACROSS A RELOAD: off on the Map tab, then a fresh Shell over the same store opens with no spots, on "
     + "either map", () => {
    const store = memory();
    mount("map", store);
    act(() => { fireEvent.click(screen.getByLabelText("Choose layers")); });
    act(() => { fireEvent.click(screen.getByRole("switch", { name: "Spots" })); });
    cleanup();
    mount("map", store);
    const mapAfter = pins();
    act(() => { fireEvent.click(screen.getByLabelText("Choose layers")); });
    const rowAfter = spotsSwitch();
    cleanup();
    mount("conditions", store);
    expect([mapAfter, rowAfter, pins(), JSON.parse(store.text!).on["overlay.spots"]])
      .toEqual(["none", "Spots off", "none", false]);
  });

  it("A WATER'S MAP VIEW is handed the same pins: every spot with Spots on; none with it kept off", async () => {
    const onWaterMap = async (store: LayerStore) => {
      const start: Route = { mode: "water", from: { ...HOME, tab: "map" },
                             stack: [{ item: "gnis:8634" as ItemId, part: 1, tapSid: null, at: null, view: "map" }] };
      render(
        <GestureHandlerRootView>
          <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
                 tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={start} loader={openRegsFixture()}
                 spots={SPOTS} layerStore={store} />
        </GestureHandlerRootView>);
      await waitFor(() => expect(screen.getByTestId("water-map-where")).toBeTruthy(), { timeout: 20_000 });
      const seen = pins();
      cleanup();
      return seen;
    };
    const off = memory();
    off.text = JSON.stringify({ on: { "overlay.spots": false } });
    expect([await onWaterMap(memory()), await onWaterMap(off)]).toEqual([ON, "none"]);
  }, 60_000);
});
