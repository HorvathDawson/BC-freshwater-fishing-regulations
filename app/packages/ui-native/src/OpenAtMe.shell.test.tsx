/**
 * (T384, owner 2026-11-04: "when we open the app default should be the persons current location") THE OPEN AT MY
 * LOCATION, through the real Shell: permission already given → the map is told to centre on the fix (at the open's
 * zoom, a town's); never asked → asked once (the store says so after); asked before, refused, or no location → today's
 * default, nothing read; a start link (a water) → the link, nothing read. The map renderer is a line saying what the
 * Shell handed it (no WebGL in jsdom).
 */
import { afterEach, beforeAll, describe, expect, it, vi } from "vitest";
import { cleanup, render, screen, waitFor } from "@testing-library/react";
import { Text } from "react-native";
import { GestureHandlerRootView } from "react-native-gesture-handler";
import { makeFixtureSource } from "@app/data/fixture";
import type { ItemId } from "@app/data";
import { HOME, OPEN_ZOOM, openWater, reduce, type Locate, type LocatePermission, type LocationAskStore, type Route } from "@app/ui";
import { LIGHT } from "./theme";

type Stub = { camera: { lat: number; lon: number; zoom: number };
              me?: { on: boolean; fix: { lat: number; lon: number } | null; centre: number; zoom?: number | null } | null };
vi.mock("./MapScreen", () => ({
  MapScreen: (p: Stub) =>
    <Text testID="map-state">{`camera ${p.camera.lat},${p.camera.lon},${p.camera.zoom} · me ` +
      (p.me?.on && p.me.fix ? `${p.me.fix.lat},${p.me.fix.lon} centre ${p.me.centre} zoom ${p.me.zoom}` : "off")}</Text>,
}));
vi.mock("./FishSpinner", () => ({ FishSpinner: () => null }));
const { Shell } = await import("./Shell");

afterEach(cleanup);
beforeAll(() => {
  Object.defineProperty(document.documentElement, "clientWidth", { configurable: true, value: 390 });
  Object.defineProperty(document.documentElement, "clientHeight", { configurable: true, value: 844 });
});

const FIX = { lat: 49.1, lon: -122.3, accuracy: 20 };
const locateAs = (p: LocatePermission) => {
  const here = vi.fn(async () => {
    if (p === "denied") throw Object.assign(new Error("location permission refused"), { environment: true });
    return FIX;
  });
  return { permission: vi.fn(async () => p), here } satisfies Locate;
};
const memory = (v: string | null = null): LocationAskStore & { v: string | null } => {
  const s = { v, read: () => s.v, write: (t: string) => { s.v = t; } };
  return s;
};

function mount(o: { start?: Route; locate: Locate; ask?: LocationAskStore | null }) {
  render(
    <GestureHandlerRootView>
      <Shell source={makeFixtureSource()} palette={LIGHT} theme="light" themeName="light" onTheme={() => {}}
             tiles={{ atlas: "a", basemap: "b" } as never} initialRoute={o.start}
             locate={o.locate} locationAsk={o.ask ?? null} />
    </GestureHandlerRootView>,
  );
}
const state = () => screen.getByTestId("map-state").textContent;

describe("(T384) the app opens on the reader's location", () => {
  it("permission already given: the map is told to centre on the fix, at the open's zoom", async () => {
    const loc = locateAs("granted");
    mount({ locate: loc });
    await waitFor(() => expect(state()).toContain(`me ${FIX.lat},${FIX.lon} centre 1 zoom ${OPEN_ZOOM}`));
    expect(loc.here).toHaveBeenCalledTimes(1);
  });

  it("never asked: asked once on this open (kept), and the map waits for nothing meanwhile", async () => {
    const loc = locateAs("prompt"), ask = memory();
    mount({ locate: loc, ask });
    expect(screen.getByTestId("map-state")).toBeTruthy();   // drawn before any answer
    await waitFor(() => expect(state()).toContain(`centre 1 zoom ${OPEN_ZOOM}`));
    expect([loc.here.mock.calls.length, ask.v]).toEqual([1, "asked"]);
  });

  it("asked on an earlier open, refused for good, or no store: today's default, nothing read", async () => {
    for (const [loc, ask] of [[locateAs("prompt"), memory("asked")], [locateAs("denied"), memory()],
                              [locateAs("unavailable"), memory()], [locateAs("prompt"), null]] as const) {
      mount({ locate: loc, ask });
      await waitFor(() => expect(loc.permission).toHaveBeenCalled());
      await new Promise((r) => setTimeout(r, 0));
      expect([loc.here.mock.calls.length, state()]).toEqual([0, expect.stringContaining("me off")]);
      cleanup();
    }
  });

  it("a refusal on the ask: today's default stays; the rail's line says why", async () => {
    const loc = locateAs("prompt");
    loc.here.mockRejectedValueOnce(Object.assign(new Error("location permission refused"), { environment: true }));
    mount({ locate: loc, ask: memory() });
    await waitFor(() => expect(loc.here).toHaveBeenCalled());
    expect(state()).toContain("me off");
  });

  it("a start link (a water) wins: nothing is asked or read", async () => {
    const loc = locateAs("granted");
    mount({ locate: loc, start: reduce(HOME, openWater({ item: "gnis:8634" as ItemId, part: null, view: "regs" })) });
    await new Promise((r) => setTimeout(r, 20));
    expect([loc.permission.mock.calls.length, loc.here.mock.calls.length]).toEqual([0, 0]);
  });
});
