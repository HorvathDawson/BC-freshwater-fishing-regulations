/**
 * (T817, owner 2026-11-16 on Harrison Lake: "the map looks super weird and not visible") A GAUGED LAKE'S ROUTE MAP. A lake
 * has no donors, so its map held "you are here" alone and framed a point in open water. With its own station (`own`) the
 * map pins it, says where its reading stands, draws it live and frames it with the place.
 *
 * MUTATION: drop `...ownAt` from the camera's points in `donorView` -> the camera is the tap's alone (zoom unchanged).
 */
import { describe, expect, it } from "vitest";
import { ownGaugeMap } from "@app/core";
import { donorView } from "./donor";

const DAY = new Date(2026, 10, 16, 12);
const HERE = { lat: 49.62, lon: -121.95 };              // the middle of Harrison Lake (the frame's part)
const GAUGE = { lat: 49.3092613220215, lon: -121.773727416992 };   // 08MG012, at the lake's south end
const lake = { answer: { ok: false as const, why: "own-gauge" as const }, rows: [], areaKm2: null, routesReady: true };
const opts = { day: DAY, horizon: 0 as const, from: HERE, size: { width: 360, height: 210 }, selected: null,
               pickable: true, quantity: "level" as const };

describe("a gauged lake's route map (T817)", () => {
  it("pins the lake's own gauge, live, with its standing, and frames it with you", () => {
    const own = { ...GAUGE, name: "Harrison Lake Near Harrison Hot Springs", live: true,
                  ...ownGaugeMap("08MG012", { percentile: 0.9, observedAt: "2026-11-16T11:30:00-08:00" }, { ref: DAY.getTime() }) };
    const before = donorView(lake, opts);
    const after = donorView(lake, { ...opts, own });
    if (!before.map || !after.map) throw new Error("expected a map");
    expect(before.map.pins.map((p) => p.kind)).toEqual(["here"]);
    expect(after.map.pins.map((p) => [p.kind, p.row, p.title, p.label])).toEqual([
      ["gauge", 0, "Harrison Lake Near Harrison Hot Springs", "Much above normal"],
      ["here", null, "you are here", "you are here"],
    ]);
    expect(after.map.keys).toEqual([{ row: null, label: "you are here" }, { row: 0, label: "08MG012 · this lake's own gauge" }]);
    expect(after.map.liveRow).toBe(0);
    expect(after.map.bbox).toEqual([HERE.lon, GAUGE.lat, GAUGE.lon, HERE.lat]);
    // the frame reaches the gauge: wider than the tap alone
    expect(after.map.camera.zoom).toBeLessThan(before.map.camera.zoom);
  });

  it("a lake gauge with no reading is drawn quiet", () => {
    const vm = donorView(lake, { ...opts, own: { ...GAUGE, name: "G", live: false, ...ownGaugeMap("08MG012", { percentile: null, observedAt: null }, { ref: DAY.getTime() }) } });
    expect([vm.map?.liveRow, vm.map?.pins[0]?.label]).toEqual([null, "not reporting"]);
  });
});
