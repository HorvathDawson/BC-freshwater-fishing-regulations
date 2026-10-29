/**
 * (owner 2026-10-29, T269 follow-up) THE MAP MADE READY BEHIND A LANDING, through the real Shell: a page that started on a
 * water (`/water/<slug>`: `initialRoute` the water, `arriveAt` it) mounts the map behind the water once the page is idle —
 * unseen, untouchable, framed on the water — and back shows THAT map (not a new one: its tiles and colours are in). A
 * water tapped from the map, and an ordinary start, mount nothing extra. The map renderer is a stub that counts mounts.
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { act, cleanup, fireEvent, render, screen } from "@testing-library/react";
import { Text } from "react-native";
import { useEffect } from "react";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import { HOME, openWater, reduce, type Route } from "@app/ui";
import { LIGHT } from "./theme";

const mounts = { n: 0 };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: { fit?: { key: string } | null }) => {
    useEffect(() => { mounts.n++; }, []);
    return <Text testID="map">{`map fit ${p.fit?.key ?? "-"}`}</Text>;
  },
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(() => { cleanup(); mounts.n = 0; vi.useRealTimers(); });
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

const VEDDER = "gnis:3062" as ItemId;
const onVedder: Route = reduce(HOME, openWater({ item: VEDDER, part: null, view: "regs" }));
function mount(initialRoute: Route, arriveAt: ItemId | null) {
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={initialRoute} arriveAt={arriveAt} />
    </GestureHandlerRootView>);
}
const idle = async () => { await act(async () => { vi.advanceTimersByTime(2500); }); await act(async () => {}); };

describe("the map behind a landing", () => {
  it("a landing: no map at first; once idle, ONE map behind the water, hidden from readers and touch; back shows that same map", async () => {
    vi.useFakeTimers();
    mount(onVedder, VEDDER);
    expect(screen.queryByTestId("map")).toBeNull();                       // the page's own load is not competed with
    await idle();
    const behind = screen.getByTestId("map");
    expect(mounts.n).toBe(1);
    expect(behind.closest('[aria-hidden="true"]')).not.toBeNull();       // unseen by a screen reader, under the water
    expect(screen.getByTestId("water-back")).toBeTruthy();               // the water is still what is on screen
    await act(async () => { fireEvent.click(screen.getByTestId("water-back")); });
    expect([screen.getByTestId("map").closest('[aria-hidden="true"]'), mounts.n]).toEqual([null, 1]);   // the same map, shown
  });

  it("a water tapped from the map (no landing) and an ordinary start mount no map behind anything", async () => {
    vi.useFakeTimers();
    mount(onVedder, null);
    await idle();
    expect(screen.queryByTestId("map")).toBeNull();
    cleanup();
    mount(HOME, null);
    await idle();
    expect(mounts.n).toBe(1);                                            // the map tab's own map, as before
  });
});
