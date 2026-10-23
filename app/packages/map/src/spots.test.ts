/**
 * T131 — a tap on a spot mark, as BOTH renderers carry it out (`pressSpot`, the one function `Map.web.tsx` and
 * `Map.native.tsx` call): a spot opens through the app; a group eases the camera to the zoom that parts it, at its
 * middle; a group no zoom parts goes to the app's list. Each is ONE call, and nothing else is called.
 */
import { describe, expect, it } from "vitest";
import { spotMarkers, type SpotPoint } from "@app/core";
import { SPOT_EASE_MS, pressSpot, spotsKey } from "./spots";

const A: SpotPoint = { id: "s1", lat: 49.0974, lon: -121.9675, title: "Tamihi run" };
const B: SpotPoint = { id: "s2", lat: 49.1, lon: -122.0, title: "Vedder River" };
const TWIN: SpotPoint = { ...A, id: "s4", title: "Tamihi again" };

function rig() {
  const calls: unknown[] = [];
  return { calls, spots: { onOpen: (id: string) => calls.push(["open", id]),
                           onList: (ids: readonly string[]) => calls.push(["list", ids]) },
           ease: (c: unknown, o: unknown) => calls.push(["ease", c, o]) };
}

describe("pressSpot", () => {
  it("a spot opens; a group eases in to where it parts; an unpartable group lists", () => {
    const [alone] = spotMarkers([A], 12, null), [group] = spotMarkers([A, B], 9, null),
          [twin] = spotMarkers([A, TWIN], 16, null);
    const r = rig();
    for (const m of [alone!, group!, twin!]) pressSpot(m, r.spots, r.ease);
    expect(r.calls).toEqual([
      ["open", "s1"],
      ["ease", { lat: group!.lat, lon: group!.lon, zoom: 9.5 }, { duration: SPOT_EASE_MS }],
      ["list", ["s1", "s4"]]]);
  });
});

describe("spotsKey: what makes a map regroup its spots", () => {
  it("a renamed, moved, added or newly selected spot changes it; a new array of the same spots does not", () => {
    const k = spotsKey([A, B], null);
    expect([spotsKey([{ ...A }, { ...B }], null) === k, spotsKey([{ ...A, title: "Renamed" }, B], null) === k,
            spotsKey([{ ...A, lat: 49.2 }, B], null) === k, spotsKey([A, B, TWIN], null) === k,
            spotsKey([A, B], "s2") === k, spotsKey(undefined, null)]).toEqual([true, false, false, false, false, ""]);
  });
});
