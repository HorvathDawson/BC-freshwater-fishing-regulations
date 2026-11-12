/**
 * (T605 items 3 and 4) THE MAP'S DAY KEEPS UP WITH THE B.C. CLOCK. With no day picked the map's status is handed
 * B.C.'s today (`useMapColouring` → `statusFeed`): it moves to the new day at B.C. midnight with no tap, and at once when
 * the app comes back to the front the morning after (`useResumed` → `appBecameActive`; the web hears the page's
 * visibility through react-native-web's AppState, a phone its own). A picked day never moves.
 *
 * MUTATION PINS: `mapDay ?? today` read from a clock frozen at mount (the T551 bug) fails the midnight step; the shell
 * not telling the clock on return fails the foreground step (the heartbeat alone needs up to a minute).
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, cleanup, renderHook } from "@testing-library/react";
import { wallIso, type StatusIndex } from "@app/core";
import { parseManifest } from "@app/data";
import { makeFixtureSource } from "@app/data/fixture";
import type { PairingInput, StatusSource } from "@app/ui";
import { useMapColouring } from "./useMapColouring";
import { useResumed } from "./useResumed";

const H = "d768d6e135e62b99";
const INDEX = { handles: H, reachDigest: "", sections: 0, waters: 0, profiles: 0, codeOn: () => "base",
                profileOf: () => null, profileCodeOn: () => "base", waterCodeOn: () => "base" } as unknown as StatusIndex;
const STATUS: StatusSource = { key: "t605-status", load: async () => INDEX };
const PAIRING: PairingInput = {
  kind: "release",
  manifest: parseManifest({
    format: "app-manifest/2", data_release: "r-t605",
    tiles: { compatible: [`${H}.*`], assets: null, mask: null,
             current: { id: `${H}.aaa`, url: `tiles/${H}.aaa/atlas.pmtiles`, bytes: 1, sha256: "x" } },
    basemap: null, files: {}, regulations: { valid_until: null, versions: null },
  }),
  held: { tiles: null, basemap: null, release: "r-1", files: {} },
};
// B.C. is UTC-7 all year from 2026-03-08: 2026-11-09 23:59:30 PT is 2026-11-10T06:59:30Z
const BEFORE = Date.parse("2026-11-10T06:59:30Z");
const source = makeFixtureSource();
const TILES = { atlas: "a", basemap: "b" } as never;

/** The hook as the Shell holds it, beside the shell's return signal. */
const mount = (mapDay: Date | null = null) => renderHook(() => {
  useResumed();
  return useMapColouring({ source, status: STATUS, pairing: PAIRING, tiles: TILES, mapTab: "map", mapDay, epoch: 0 });
});
/** Let the pair, the index and the feeds settle (promises and their timers) without moving the clock a second. */
const settle = async () => { for (let i = 0; i < 20; i++) await act(async () => { await vi.advanceTimersByTimeAsync(10); }); };
/** The page hidden or shown, as a browser says it (react-native-web's AppState reads it). */
const visible = (on: boolean) => act(() => {
  Object.defineProperty(document, "visibilityState", { configurable: true, value: on ? "visible" : "hidden" });
  document.dispatchEvent(new Event("visibilitychange"));
});

describe("the map's status day follows the B.C. clock (T605)", () => {
  beforeEach(() => { vi.useFakeTimers(); vi.setSystemTime(BEFORE); });
  afterEach(() => { cleanup(); visible(true); vi.useRealTimers(); });

  it("(item 4) no day picked: the status is handed the new day at B.C. midnight, with no tap", async () => {
    const { result } = mount();
    await settle();
    const day = () => (result.current.mapStatus ? wallIso(result.current.mapStatus.day) : "no status");
    const seen = [day()];
    const handed = result.current.mapStatus;
    await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });      // 23:59:40
    seen.push(day(), String(result.current.mapStatus === handed));              // the same feed until the day turns
    await act(async () => { await vi.advanceTimersByTimeAsync(25_000); });      // 00:00:05
    seen.push(day());
    expect(seen).toEqual(["2026-11-09", "2026-11-09", "true", "2026-11-10"]);
  });

  it("(item 3) back at the front the morning after: the new day at once, before any timer runs", async () => {
    const { result } = mount();
    await settle();
    const day = () => (result.current.mapStatus ? wallIso(result.current.mapStatus.day) : "no status");
    const seen = [day()];
    visible(false);
    vi.setSystemTime(BEFORE + 8 * 3_600_000);         // the phone slept: no timer fired
    seen.push(day());
    visible(true);
    seen.push(day());
    expect(seen).toEqual(["2026-11-09", "2026-11-09", "2026-11-10"]);
  });

  it("a picked day is the day handed, whatever the clock does", async () => {
    const picked = new Date(2026, 10, 14);
    const { result } = mount(picked);
    await settle();
    await act(async () => { await vi.advanceTimersByTimeAsync(40_000); });
    expect(result.current.mapStatus?.day).toBe(picked);
  });
});
