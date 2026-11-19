/**
 * (T881, owner 2026-11-18: "maybe larger fit only") THE ROUTE MAP OPENS WITH EVERY MARK WHOLE INSIDE IT. On the Vedder
 * the four gauges run east up the valley and the rightmost one's "10%" pill was cut by the map's edge: the camera framed
 * the points, not what is drawn at them. Each gauge now has room for its share pill on any side and the spot for its pin
 * and words (core `ROUTE_MARK_PX`), at the map's laid-out size, centred with even margins.
 *
 * The projection is the renderer's own (512 px tiles, Web Mercator), as in `panel.camera.test.ts`.
 * MUTATION: drop the `px` from the gauges in `donorView`'s camera -> the east pill reaches past the right edge.
 */
import { describe, expect, it } from "vitest";
import { ROUTE_MARK_PX, type MarkPx } from "@app/core";
import type { Panel } from "@app/data";
import { answerFrom, donorView, type PanelAnswer } from "../panel";

const DAY = new Date("2026-10-21T05:00:00Z");
/** The Vedder's spot and its four donors, west to east (08MH001 at Vedder Crossing, then up the Chilliwack). */
const HERE = { lat: 49.103, lon: -121.985 };
const AT: Record<string, { lat: number; lon: number }> = {
  "08MH001": { lat: 49.097, lon: -121.962 }, "08MH103": { lat: 49.086, lon: -121.85 },
  "08MH105": { lat: 49.112, lon: -121.73 }, "08MH016": { lat: 49.096, lon: -121.6 },
};
const member = (station: string, areaKm2: number) => ({ station: station as never, role: "up" as const, areaKm2,
  years: { discharge: 40, level: 40 }, regulated: false, sameRiver: true });
const PANEL: Panel = { areaKm2: 988, members: [member("08MH001", 1230), member("08MH103", 700),
                                               member("08MH105", 600), member("08MH016", 330)] };
const READS = { listed: Object.keys(AT), stations: Object.fromEntries(Object.keys(AT).map((s) =>
  [s, { percentile: 0.2, parameter: "discharge" as const, discharge: 0.2, observedAt: "2026-10-20T20:30:00-08:00" }])) };
const answer = (): PanelAnswer => {
  const a = answerFrom(PANEL, READS, "discharge", 0);
  return { ...a, routesReady: true, rows: a.rows.map((r) => ({ ...r, route: {
    station: r.station, name: r.station, ...AT[r.station]!, role: "up" as const, path: [1, 2] as never } })) } as PanelAnswer;
};

const toY = (lat: number) => {
  const φ = (lat * Math.PI) / 180;
  return (1 - Math.log(Math.tan(φ) + 1 / Math.cos(φ)) / Math.PI) / 2;
};
/** A mark's drawn box on the map, px from the map's top-left. */
function boxOf(cam: { lat: number; lon: number; zoom: number }, p: { lat: number; lon: number }, e: MarkPx,
               size: { width: number; height: number }) {
  const world = 512 * 2 ** cam.zoom;
  const x = size.width / 2 + ((p.lon - cam.lon) / 360) * world;
  const y = size.height / 2 + (toY(p.lat) - toY(cam.lat)) * world;
  return { l: x - e.l, r: x + e.r, t: y - e.t, b: y + e.b };
}

describe("the route map's opening frame holds every mark whole (T881)", () => {
  for (const size of [{ width: 358, height: 380 }, { width: 640, height: 608 }, { width: 200, height: 380 }]) {
    it(`every gauge with its pill, and the spot with its words, inside a ${size.width}×${size.height} map`, () => {
      const vm = donorView(answer(), { day: DAY, horizon: 0, from: HERE, size, selected: null, pickable: true });
      if (!vm.ok || !vm.map) throw new Error("expected a map");
      const pins = vm.map.pins;
      expect(pins.filter((p) => p.kind === "gauge")).toHaveLength(4);
      const boxes = pins.map((p) => boxOf(vm.map!.camera, p, p.kind === "here" ? ROUTE_MARK_PX.here : ROUTE_MARK_PX.gauge,
                                          size));
      for (const b of boxes) {
        expect(b.l).toBeGreaterThanOrEqual(0);
        expect(b.r).toBeLessThanOrEqual(size.width);
        expect(b.t).toBeGreaterThanOrEqual(0);
        expect(b.b).toBeLessThanOrEqual(size.height);
      }
      // EVEN MARGINS, and the frame filled on its tight axis (east–west here): 10 px each side
      const l = Math.min(...boxes.map((b) => b.l)), r = Math.max(...boxes.map((b) => b.r));
      expect(l).toBeCloseTo(size.width - r, 3);
      expect(l).toBeCloseTo(10, 3);
    });
  }

  it("is fitted to the size the map is laid out at: a wider map opens closer", () => {
    const at = (width: number) => donorView(answer(), { day: DAY, horizon: 0, from: HERE,
      size: { width, height: Math.round(width * 0.95) }, selected: null, pickable: true }).map!.camera.zoom;
    expect(at(640)).toBeGreaterThan(at(358));
  });
});
