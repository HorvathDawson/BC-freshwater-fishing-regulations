/**
 * SATELLITE IS ONLINE-ONLY, THROUGH THE REAL SHELL (owner 2026-10-21): Esri World Imagery while the device can reach
 * it; offline, the drawn map, the Layers sheet saying why (the Map tile tagged SHOWING, the reason line), and the
 * reader's choice kept — so it comes back by itself. Esri's credit is the map's own attribution, from the style
 * (`tools/imagery.test.ts`): the Layers sheet carries no credits any more (owner 2026-10-21 late).
 *
 * The map renderer is replaced by a line saying which ground the Shell handed it (no WebGL in jsdom); the device's
 * connectivity and the one-tile probe are replaced by switches. Everything else is the app's, on the fixture.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME } from "@app/ui";
import { LIGHT } from "./theme";

// the sheet's credit block is gone: no credit line is drawn anywhere in the Shell's Layers sheet, satellite or not
const ESRI = "Satellite imagery: Powered by Esri";

type Stub = { onLayers?: () => void; basemap?: string; onImageryError?: (status: number | undefined) => void;
              layersChanged?: number };
const map: { imageryError?: (status: number | undefined) => void } = {};
vi.mock("./MapScreen", () => ({
  MapScreen: (p: Stub) => {
    map.imageryError = p.onImageryError;
    return (
      <View>
        {p.onLayers && <Pressable accessibilityRole="button" accessibilityLabel="Choose layers"
                                  onPress={p.onLayers}><Text>layers</Text></Pressable>}
        <Text testID="ground">{`ground ${p.basemap ?? "unset"}`}</Text>
        <Text testID="changed">{`changed ${p.layersChanged ?? "unset"}`}</Text>
      </View>);
  },
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
/** The device: online until a test says otherwise, and its events. */
const device: { online: boolean; cb: ((on: boolean) => void) | null } = { online: true, cb: null };
vi.mock("./connectivity", () => ({
  connectivity: { watches: true, probeWhileUp: false, online: () => device.online,
                  subscribe: (on: (o: boolean) => void) => { device.cb = on; return () => { device.cb = null; }; } },
}));
/** The one-tile probe: answers while `reachable`. */
const probe = { reachable: true, calls: 0 };
vi.mock("./imageryProbe", () => ({
  imageryProbe: () => async () => { probe.calls += 1; return probe.reachable; },
}));
const { Shell } = await import("./Shell");

afterEach(() => { cleanup(); device.online = true; device.cb = null; probe.reachable = true; probe.calls = 0; });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

const CREDITS = ["Basemap © OpenStreetMap contributors · © Protomaps", "Weather from Open-Meteo.com (CC BY 4.0)"];
const NOTE = "Satellite needs a connection. Showing the map until you are back online; satellite returns by itself.";

function mount() {
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={HOME} attribution={CREDITS} />
    </GestureHandlerRootView>);
}
const flush = () => act(async () => { await Promise.resolve(); await Promise.resolve(); });
const setDevice = (on: boolean) => act(() => { device.online = on; device.cb?.(on); });
/** What a reader sees: the ground drawn, the Map choice, the sheet's reason line, and whether the Map tile says SHOWING. */
const look = () => ({
  ground: screen.getByTestId("ground").textContent,
  chosen: screen.getAllByRole("radio").filter((r) => ["Map", "Satellite"].includes(r.getAttribute("aria-label") ?? ""))
    .find((r) => r.getAttribute("aria-checked") === "true")?.getAttribute("aria-label") ?? null,
  note: screen.queryByTestId("basemap-note")?.textContent ?? null,
  showing: (screen.getByRole("radio", { name: "Map" }).textContent ?? "").endsWith("SHOWING"),
});

describe("satellite, online-only, through the Shell", () => {
  it("on: Esri's imagery; offline: the drawn map and the reason, the choice kept; online: satellite back", async () => {
    mount();
    act(() => { fireEvent.click(screen.getByLabelText("Choose layers")); });
    expect(look()).toEqual({ ground: "ground map", chosen: "Map", note: null, showing: false });

    act(() => { fireEvent.click(screen.getByRole("radio", { name: "Satellite" })); });
    await flush();
    expect(look()).toEqual({ ground: "ground satellite", chosen: "Satellite", note: null, showing: false });

    setDevice(false);
    expect(look()).toEqual({ ground: "ground map", chosen: "Satellite", note: NOTE, showing: true });

    setDevice(true);
    await flush();
    expect(look()).toEqual({ ground: "ground satellite", chosen: "Satellite", note: null, showing: false });
    expect(document.body.textContent?.includes(ESRI)).toBe(false);
    // the rail's badge: one choice differs from the defaults (the ground), handed to the map screen
    expect(screen.getByTestId("changed").textContent).toBe("changed 1");
  });

  it("an imagery tile that fails with no response draws the map; a 404 hole does not", async () => {
    mount();
    act(() => { fireEvent.click(screen.getByLabelText("Choose layers")); });
    act(() => { fireEvent.click(screen.getByRole("radio", { name: "Satellite" })); });
    await flush();
    act(() => map.imageryError?.(404));
    expect(look().ground).toBe("ground satellite");
    act(() => map.imageryError?.(undefined));
    expect(look()).toEqual({ ground: "ground map", chosen: "Satellite", note: NOTE, showing: true });
  });

  it("the drawn map chosen: no probe, no note, no SHOWING tag, whatever the device says", async () => {
    mount();
    act(() => { fireEvent.click(screen.getByLabelText("Choose layers")); });
    setDevice(false);
    await flush();
    expect({ ...look(), probes: probe.calls })
      .toEqual({ ground: "ground map", chosen: "Map", note: null, showing: false, probes: 0 });
  });
});
