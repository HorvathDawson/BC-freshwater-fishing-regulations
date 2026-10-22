/**
 * THE MAP RAIL THROUGH THE REAL SHELL (owner 2026-10-21): spots moved off the bottom bar onto the maps' side rail; a long
 * press drops a provisional pin and asks before anything is made; every way of adding goes into the ONE spot capture
 * at a point; Android's back closes each new sheet before it does anything else, and leaves the app at the root.
 *
 * The map renderer is replaced by buttons that press and hold it, open the rail's sheets, and a line saying what pin
 * and what framing the Shell handed it (no WebGL in jsdom). Everything else is the app's, on the fixture.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { Spot } from "@app/data/spots";
import { HOME, type Locate, type Route, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

type Stub = { onLongPress?: (lat: number, lon: number) => void; onSpots?: () => void; onLayers?: () => void;
              marker?: { lat: number; lon: number } | null; fit?: { key: string } | null;
              camera: { lat: number; lon: number; zoom: number } };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: Stub) =>
    <View>
      <Pressable accessibilityRole="button" accessibilityLabel="hold the map"
                 onPress={() => p.onLongPress?.(49.0974, -121.9675)}><Text>hold</Text></Pressable>
      {p.onSpots && <Pressable accessibilityRole="button" accessibilityLabel="Your spots"
                               onPress={p.onSpots}><Text>spots</Text></Pressable>}
      {p.onLayers && <Pressable accessibilityRole="button" accessibilityLabel="Choose layers"
                                onPress={p.onLayers}><Text>layers</Text></Pressable>}
      <Text testID="map-state">{`pin ${p.marker ? `${p.marker.lat},${p.marker.lon}` : "none"} · fit ${p.fit?.key ?? "none"}` +
                                ` · camera ${p.camera.lat},${p.camera.lon},${p.camera.zoom}`}</Text>
    </View>,
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(cleanup);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

const VISIT = new Date(2026, 7, 30, 23, 30).getTime();
const TAMIHI: Spot = {
  id: "s1", createdAt: VISIT, visitedAt: VISIT, updatedAt: 0, lat: 49.0974, lon: -121.9675, item: null,
  waterName: null, title: "Tamihi run", notes: "", photos: [], reading: null, weather: null, trace: null, panel: null,
};

/** Android's back, as `BackHandler` reaches the one back: what it returns is whether the app handled it. */
let androidBack: () => boolean = () => false;
const android: SystemBack = (back) => { androidBack = back; return { sync: () => {}, dispose: () => {} }; };

function mount(o: { start?: Route; locate?: Locate | null; spots?: Spot[] } = {}) {
  const saved: Spot[] = [];
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={o.start ?? HOME}
             spots={o.spots ?? [TAMIHI]} onSaveSpot={(s) => { saved.push(s); }} systemBack={android}
             locate={o.locate ?? null} />
    </GestureHandlerRootView>);
  return saved;
}
const press = (name: string) => act(() => { fireEvent.click(screen.getByLabelText(name)); });
/** A closed sheet slides out on react-native-web and is gone at the end of its animation, which jsdom never plays. */
const slidOut = () => act(() => {
  for (const el of document.querySelectorAll("div"))
    for (const type of ["animationend", "webkitAnimationEnd"]) el.dispatchEvent(new Event(type, { bubbles: true }));
});
/** What a reader sees: the bottom bar, the map's pin and framing, and which sheet or screen is up. */
const look = () => {
  slidOut();
  const bar = screen.queryAllByRole("tab").map((t) => t.getAttribute("aria-label")).join(" ") || "no bar";
  const map = screen.queryByTestId("map-state")?.textContent ?? "no map";
  const up = screen.queryByText("New spot") ? "drop sheet"
    : screen.queryByText("Add a spot here") ? "spots menu"
    : screen.queryByText("Pick the water") ? `capture: ${screen.queryByText(/near the pin/) ? "near the pin" : "pick"}`
    : screen.queryByText("Tamihi run") ? "spot screen" : "nothing";
  return `${bar} | ${map} | ${up}`;
};

describe("the map rail, through the Shell", () => {
  it("three tabs (Spots moved to the rail), and the rail on both maps — Layers on the Map tab only", () => {
    mount();
    const map = [look(), screen.queryByLabelText("Choose layers") !== null, screen.queryByLabelText("Your spots") !== null];
    cleanup();
    mount({ start: { ...HOME, tab: "conditions" } });
    expect([map, screen.queryByLabelText("Choose layers") !== null, screen.queryByLabelText("Your spots") !== null])
      .toEqual([["Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing", true, true],
                false, true]);
  });

  it("a long press drops a provisional pin and asks; Cancel removes it and nothing is saved", () => {
    const saved = mount();
    press("hold the map");
    const asked = look();
    press("Cancel");
    expect([asked, look(), saved]).toEqual([
      "Map Search Conditions | pin 49.0974,-121.9675 · fit none · camera 49.15,-121.85,9.4 | drop sheet",
      "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing", []]);
  });

  it("Android's back closes the drop sheet (and its pin) first, then at the root leaves the app", () => {
    mount();
    press("hold the map");
    let handled = false;
    act(() => { handled = androidBack(); });
    const after = look();
    let atRoot = true;
    act(() => { atRoot = androidBack(); });
    expect([handled, after, atRoot]).toEqual([
      true, "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing", false]);
  });

  it("Continue goes into the one spot capture, at the pin: pick the water near it; back leaves it, nothing saved", async () => {
    const saved = mount();
    press("hold the map");
    press("Add a spot here");
    const capture = look();
    let handled = false;
    act(() => { handled = androidBack(); });
    expect([capture, handled, look(), saved]).toEqual([
      "no bar | pin 49.0974,-121.9675 · fit none · camera 49.0974,-121.9675,14 | capture: near the pin", true,
      "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing", []]);
  });

  it("the Spots menu: a spot opens over the map and back returns to it; Show frames it; back closes the menu first", () => {
    mount();
    press("Your spots");
    const menu = look();
    act(() => { androidBack(); });
    const closed = look();
    press("Your spots");
    press("Tamihi run");
    const opened = look();
    act(() => { androidBack(); });
    const back = look();
    press("Your spots");
    press("Show Tamihi run on the map");
    expect([menu, closed, opened, back, look()]).toEqual([
      "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | spots menu",
      "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing",
      "Map Search Conditions | no map | spot screen",
      "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing",
      "Map Search Conditions | pin 49.0974,-121.9675 · fit spot:s1:1 · camera 49.15,-121.85,9.4 | nothing",
    ]);
  });

  it("Show's pin and framing belong to that map: a spot opened, or another tab, forgets them (no re-framing on return)", () => {
    mount();
    press("Your spots");
    press("Show Tamihi run on the map");
    const shown = look();
    press("Your spots");
    press("Tamihi run");
    act(() => { androidBack(); });
    const returned = look();
    press("Your spots");
    press("Show Tamihi run on the map");
    act(() => { fireEvent.click(screen.getByRole("tab", { name: "Conditions" })); });
    expect([shown, returned, look()]).toEqual([
      "Map Search Conditions | pin 49.0974,-121.9675 · fit spot:s1:1 · camera 49.15,-121.85,9.4 | nothing",
      "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing",
      "Map Search Conditions | pin none · fit none · camera 49.15,-121.85,9.4 | nothing",
    ]);
  });

  it("Add a spot here (the map's middle) and At my location both start the one capture at their point", async () => {
    const locate: Locate = { permission: async () => "granted", here: async () => ({ lat: 50.1, lon: -120.2 }) };
    mount({ locate });
    press("Your spots");
    press("Add a spot here");
    const middle = look();
    act(() => { androidBack(); });
    press("Your spots");
    await waitFor(() => expect(screen.queryByLabelText("At my location")).not.toBeNull());
    await act(async () => { fireEvent.click(screen.getByLabelText("At my location")); });
    expect([middle, look()]).toEqual([
      "no bar | pin 49.15,-121.85 · fit none · camera 49.15,-121.85,14 | capture: near the pin",
      "no bar | pin 50.1,-120.2 · fit none · camera 50.1,-120.2,14 | capture: near the pin",
    ]);
  });
});
