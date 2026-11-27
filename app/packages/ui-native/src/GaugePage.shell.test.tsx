/**
 * (T1194, review 2026-11-27) A GAUGE'S PAGE OVER THE KEPT CONDITIONS MAP, THROUGH THE REAL SHELL: tap a dot -> the page -> Back, and the
 * map is the SAME map instance throughout (mounted once, never unmounted, never rebuilt: its tiles, colours and GL context are kept,
 * as under a water's page). The renderer is a stub that counts its own mounts and unmounts; everything else is the Shell's.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Pressable, Text, View } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import { HOME } from "@app/ui";
import { LIGHT } from "./theme";

const life = vi.hoisted(() => ({ mounts: 0, unmounts: 0, gaugeTap: null as null | ((s: string) => void) }));
vi.mock("./MapScreen", async () => {
  const { useEffect } = await import("react");
  return {
    MapScreen: (p: { onPressGauge?: (s: string) => void }) => {
      useEffect(() => { life.mounts += 1; return () => { life.unmounts += 1; }; }, []);
      life.gaugeTap = p.onPressGauge ?? null;
      return <View><Text testID="map-stub">map</Text><Pressable accessibilityRole="button" accessibilityLabel="Tap a gauge dot"
                                                             onPress={() => p.onPressGauge?.("08MH103")}><Text>dot</Text></Pressable></View>;
    },
  };
});
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(() => { cleanup(); life.mounts = 0; life.unmounts = 0; life.gaugeTap = null; });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

describe("(T1194) the Conditions map survives a gauge's page", () => {
  it("tap a dot: the page opens over the map; Back: the map is back, the SAME instance (one mount, no unmount)", async () => {
    render(<GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={{ ...HOME, tab: "conditions" }} spots={[]} />
    </GestureHandlerRootView>);
    await act(async () => { await new Promise((r) => setTimeout(r, 100)); });
    expect(life.mounts).toBe(1);
    expect(life.gaugeTap).toBeTypeOf("function");                              // the Conditions tab hands the map the gauge tap
    act(() => { fireEvent.click(screen.getByRole("button", { name: "Tap a gauge dot" })); });
    await act(async () => { await new Promise((r) => setTimeout(r, 100)); });
    expect(screen.getByTestId("gauge-screen")).toBeTruthy();                   // the page is up over it
    expect(life.unmounts).toBe(0);                                              // ...and the map was not torn down for it
    act(() => { fireEvent.click(screen.getByTestId("gauge-back")); });
    await act(async () => { await new Promise((r) => setTimeout(r, 100)); });
    expect(screen.queryByTestId("gauge-screen")).toBeNull();
    expect(screen.getByTestId("map-stub")).toBeTruthy();
    expect({ mounts: life.mounts, unmounts: life.unmounts }).toEqual({ mounts: 1, unmounts: 0 });
  });
});
