/**
 * SPOTS AND THE RAIL THROUGH THE REAL SHELL (T48, owner 2026-10-22): Spots is a bottom-bar TAB again (its list, open,
 * Show on the map, add) and the side rail is Layers alone; a long press on either map still drops a provisional pin
 * and asks before anything is made; adding offers exactly two ways (owner 2026-10-22) — At my location and Pick on the
 * map (the Map tab under a banner until a tap places the pin; that tap never opens a water); every way of adding goes
 * into the ONE spot capture at a point; Android's back closes each sheet (and pick mode) before it does anything else,
 * goes from a tab to the Map tab, and leaves the app at the root.
 *
 * The map renderer is replaced by buttons that press and hold it, tap a water on it (the place AND the feature, as both
 * renderers report a tap), open the Layers sheet, and a line saying what pin and what framing the Shell handed it (no
 * WebGL in jsdom). Everything else is the app's, on the fixture.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import type { Spot } from "@app/data/spots";
import { HOME, type Locate, type Route, type SystemBack } from "@app/ui";
import { LIGHT } from "./theme";

/** The section the stub's "tap a water" lands on: a reach of the Chilliwack (set in `beforeAll`). */
const TAP = { sid: 0 };
type Stub = { onLongPress?: (lat: number, lon: number) => void; onLayers?: () => void;
              onMapPoint?: (lat: number, lon: number) => void;
              onPressFeature?: (layer: string, id: number, lat?: number, lon?: number) => void;
              marker?: { lat: number; lon: number } | null; fit?: { key: string } | null;
              camera: { lat: number; lon: number; zoom: number } };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: Stub) =>
    <View>
      <Pressable accessibilityRole="button" accessibilityLabel="hold the map"
                 onPress={() => p.onLongPress?.(49.0974, -121.9675)}><Text>hold</Text></Pressable>
      {/* both renderers report a tap as the place, then the feature under it (Map.native onPress; Map.web clicks) */}
      <Pressable accessibilityRole="button" accessibilityLabel="tap a water"
                 onPress={() => { p.onMapPoint?.(49.2, -122.1); void p.onPressFeature?.("streams", TAP.sid, 49.2, -122.1); }}>
        <Text>tap</Text></Pressable>
      {p.onLayers && <Pressable accessibilityRole="button" accessibilityLabel="Choose layers"
                                onPress={p.onLayers}><Text>layers</Text></Pressable>}
      <Text testID="map-state">{`pin ${p.marker ? `${p.marker.lat},${p.marker.lon}` : "none"} · fit ${p.fit?.key ?? "none"}` +
                                ` · camera ${p.camera.lat},${p.camera.lon},${p.camera.zoom}`}</Text>
    </View>,
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(cleanup);
beforeAll(async () => {
  TAP.sid = (await makeFixtureSource().water("gnis:8634" as ItemId))!.sections[1]!;
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
  const on = screen.queryAllByRole("tab").find((t) => t.getAttribute("aria-selected") === "true")?.getAttribute("aria-label");
  const map = screen.queryByTestId("map-state")?.textContent ?? "no map";
  const up = screen.queryByText("New spot") ? "drop sheet"
    : screen.queryByTestId("spot-pick-banner") ? `pick banner${screen.queryByTestId("top-bar") ? " + search" : ""}`
    : screen.queryByTestId("spots-screen") ? "spots tab"
    : screen.queryByText("Pick the water") ? `capture: ${screen.queryByText(/near the pin/) ? "near the pin" : "pick"}`
    : screen.queryByText("Tamihi run") ? "spot screen" : "nothing";
  return `${bar}${on ? ` (${on})` : ""} | ${map} | ${up}`;
};
const BAR = "Map Conditions Spots More";
const ROOT = `${BAR} (Map) | pin none · fit none · camera 49.15,-121.85,10 | nothing`;
const tab = (name: string) => act(() => { fireEvent.click(screen.getByRole("tab", { name })); });

describe("spots and the rail, through the Shell (T48)", () => {
  it("four tabs, Spots among them; the rail is Layers alone on both maps — no Spots button anywhere on a map", () => {
    mount();
    const map = [look(), screen.queryByLabelText("Choose layers") !== null, screen.queryByLabelText("Your spots")];
    cleanup();
    mount({ start: { ...HOME, tab: "conditions" } });
    expect([map, screen.queryByLabelText("Choose layers") !== null, screen.queryByLabelText("Your spots")])
      .toEqual([[ROOT, true, null], true, null]);
  });

  it("a long press drops a provisional pin and asks; Cancel removes it and nothing is saved", () => {
    const saved = mount();
    press("hold the map");
    const asked = look();
    press("Cancel");
    expect([asked, look(), saved]).toEqual([
      `${BAR} (Map) | pin 49.0974,-121.9675 · fit none · camera 49.15,-121.85,10 | drop sheet`, ROOT, []]);
  });

  it("Android's back closes the drop sheet (and its pin) first, then at the root leaves the app", () => {
    mount();
    press("hold the map");
    let handled = false;
    act(() => { handled = androidBack(); });
    const after = look();
    let atRoot = true;
    act(() => { atRoot = androidBack(); });
    expect([handled, after, atRoot]).toEqual([true, ROOT, false]);
  });

  it("Continue goes into the one spot capture, at the pin: pick the water near it; back leaves it, nothing saved", async () => {
    const saved = mount();
    press("hold the map");
    press("Continue");
    const capture = look();
    let handled = false;
    act(() => { handled = androidBack(); });
    expect([capture, handled, look(), saved]).toEqual([
      "no bar | pin 49.0974,-121.9675 · fit none · camera 49.0974,-121.9675,14 | capture: near the pin", true, ROOT, []]);
  });

  it("the Spots tab: a spot opens over it and back returns to it; Show goes to the Map tab framed on it; back from the " +
     "tab is the Map tab, then Android leaves", () => {
    mount();
    tab("Spots");
    const list = look();
    press("Tamihi run");
    const opened = look();
    act(() => { androidBack(); });
    const back = look();
    press("Show Tamihi run on the map");
    const shown = look();
    tab("Spots");
    let handled = false, left = true;
    act(() => { handled = androidBack(); });
    const toMap = look();
    act(() => { left = androidBack(); });
    expect([list, opened, back, shown, handled, toMap, left]).toEqual([
      `${BAR} (Spots) | no map | spots tab`,
      `${BAR} (Spots) | no map | spot screen`,
      `${BAR} (Spots) | no map | spots tab`,
      `${BAR} (Map) | pin 49.0974,-121.9675 · fit spot:s1:1 · camera 49.15,-121.85,10 | nothing`,
      true, ROOT, false,
    ]);
  });

  it("a spot's own Show is the same Show: the Map tab, framed on it with its pin", () => {
    mount({ start: { ...HOME, tab: "conditions", spot: "s1" } });
    press("Show on map");
    expect(look()).toBe(`${BAR} (Map) | pin 49.0974,-121.9675 · fit spot:s1:1 · camera 49.0974,-121.9675,14 | nothing`);
  });

  it("Show's pin and framing belong to that map: a spot opened, or another tab, forgets them (no re-framing on return)", () => {
    mount();
    tab("Spots");
    press("Show Tamihi run on the map");
    const shown = look();
    tab("Spots");
    press("Tamihi run");
    act(() => { androidBack(); });
    tab("Map");
    const returned = look();
    tab("Spots");
    press("Show Tamihi run on the map");
    tab("Conditions");
    expect([shown, returned, look()]).toEqual([
      `${BAR} (Map) | pin 49.0974,-121.9675 · fit spot:s1:1 · camera 49.15,-121.85,10 | nothing`, ROOT,
      `${BAR} (Conditions) | pin none · fit none · camera 49.15,-121.85,10 | nothing`,
    ]);
  });

  it("adding offers exactly two ways: At my location (granted) starts the one capture where the device is", async () => {
    const locate: Locate = { permission: async () => "granted", here: async () => ({ lat: 50.1, lon: -120.2 }) };
    mount({ locate, spots: [] });
    tab("Spots");
    await waitFor(() => expect(screen.queryByLabelText("At my location")).not.toBeNull());
    const offered = screen.getAllByRole("button").map((b) => b.getAttribute("aria-label")).filter((n) => n !== "Refresh spots");
    await act(async () => { fireEvent.click(screen.getByLabelText("At my location")); });
    expect([offered, look()]).toEqual([
      ["At my location", "Pick on the map"],
      "no bar | pin 50.1,-120.2 · fit none · camera 50.1,-120.2,14 | capture: near the pin",
    ]);
  });

  it("At my location: the permission is asked on the tap, and refused there it is said and nothing starts; with no " +
     "location module (native today) it is not offered and the tab says so", async () => {
    const locate: Locate = { permission: async () => "prompt", here: async () => { throw new Error("denied"); } };
    const saved = mount({ locate, spots: [] });
    tab("Spots");
    await waitFor(() => expect(screen.queryByText("Asks for your location first")).not.toBeNull());
    await act(async () => { fireEvent.click(screen.getByLabelText("At my location")); });
    const refused = [look(), screen.getByRole("alert").textContent, saved];
    cleanup();
    mount({ locate: null, spots: [] });
    tab("Spots");
    expect([refused, screen.queryByLabelText("At my location"), screen.getByTestId("spots-note").textContent]).toEqual([
      [`${BAR} (Spots) | no map | spots tab`, "Your location could not be read, so no spot was started.", []],
      null, "Your location is not available here, so At my location is not offered."]);
  });

  it("Pick on the map: the Map tab under a banner (in the search field's place); a tap on a water places the pin and " +
     "asks — it does NOT open the water; Continue is the one capture at that point; back leaves it, nothing saved",
     async () => {
    const saved = mount();
    tab("Spots");
    press("Pick on the map");
    const picking = [look(), screen.getByTestId("spot-pick-banner").textContent];
    press("tap a water");
    await act(async () => { for (let i = 0; i < 8; i++) await Promise.resolve(); });
    const tapped = look();
    press("Continue");
    const capture = look();
    act(() => { androidBack(); });
    expect([picking, tapped, capture, look(), saved]).toEqual([
      [`${BAR} (Map) | pin none · fit none · camera 49.15,-121.85,10 | pick banner`, "Tap where your spot isCancel"],
      `${BAR} (Map) | pin 49.2,-122.1 · fit none · camera 49.15,-121.85,10 | drop sheet`,
      "no bar | pin 49.2,-122.1 · fit none · camera 49.2,-122.1,14 | capture: near the pin",
      ROOT, []]);
  });

  it("outside pick mode the same tap opens the water (the one water screen) — so the check above is a real one", async () => {
    mount();
    press("tap a water");
    await waitFor(() => expect(screen.queryByText("Chilliwack River")).not.toBeNull());
    // the water's own views strip, not the bottom bar: the water screen is up, and no map under it
    expect(look()).toBe("Regulations Conditions Map (Regulations) | no map | nothing");
  });

  it("pick mode is a back layer: Android's back leaves it one step (the Map tab as it was), the next leaves the app; " +
     "the banner's Cancel, Escape, a tab, and the drop sheet's Cancel all leave nothing", () => {
    const saved = mount();
    const enter = () => { tab("Spots"); press("Pick on the map"); };
    enter();
    let handled = false, left = true;
    act(() => { handled = androidBack(); });
    const backed = look();
    act(() => { left = androidBack(); });
    enter();
    press("Cancel");
    const cancelled = look();
    enter();
    act(() => { window.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape" })); });
    const escaped = look();
    enter();
    tab("Conditions");
    const away = look();
    enter();
    press("tap a water");
    press("Cancel");
    expect([handled, backed, left, cancelled, escaped, away, look(), saved]).toEqual([
      true, ROOT, false, ROOT, ROOT, `${BAR} (Conditions) | pin none · fit none · camera 49.15,-121.85,10 | nothing`,
      ROOT, []]);
  });
});

describe("the rail's Layers badge (layers menu B)", () => {
  it("counts the choices that differ from the defaults, on the button and aloud; none draws no badge", async () => {
    const { MapRail } = await import("./MapRail");
    const seen = (n: number) => {
      render(<MapRail palette={LIGHT} onLayers={() => {}} layersChanged={n} />);
      const out = [screen.getAllByRole("button").map((b) => b.getAttribute("aria-label")),
                   screen.queryByTestId("rail-badge-layers")?.textContent ?? null];
      cleanup();
      return out;
    };
    expect([seen(0), seen(2)]).toEqual([
      [["Choose layers"], null],
      [["Choose layers, 2 changed"], "2"],
    ]);
  });
});
