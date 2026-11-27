/** (T1194) The gauge tap's one rule: a 44 px box centred on the DOT, a pill at its own drawn size, the pill first, the nearest dot, no station no gauge. */
import { describe, expect, it, vi } from "vitest";
import type * as maplibregl from "maplibre-gl";
import { GAUGE_DOT_LAYER, GAUGE_DOT_REACH, GAUGE_PILL_LAYER, GAUGE_TAP_PX, dotBox, gaugeHit, pillBox, queryGauge, type GaugeFeature } from "./gauge-hit";
import { hitGauge } from "./web/gaugeHit";

const pt = (station: unknown, lon: number, lat: number): GaugeFeature => ({ properties: { station }, geometry: { type: "Point", coordinates: [lon, lat] } });

/**
 * A SCREEN OF GAUGES, in pixels, answering `queryRenderedFeatures` as an engine does: a feature is found when its drawn shape meets
 * the box. A dot is a circle (`GAUGE_DOT_REACH` from its centre), a pill the rectangle it is drawn as.
 */
function screen(dots: { st: string; x: number; y: number }[], pills: { st: string; x0: number; x1: number; y0: number; y1: number }[]) {
  const meets = (box: [[number, number], [number, number]], x0: number, y0: number, x1: number, y1: number) =>
    box[0][0] <= x1 && box[1][0] >= x0 && box[0][1] <= y1 && box[1][1] >= y0;
  return (box: [[number, number], [number, number]], layer: string): GaugeFeature[] =>
    layer === GAUGE_DOT_LAYER
      ? dots.filter((d) => meets(box, d.x - GAUGE_DOT_REACH, d.y - GAUGE_DOT_REACH, d.x + GAUGE_DOT_REACH, d.y + GAUGE_DOT_REACH))
          .map((d) => pt(d.st, -122 + d.x / 1e4, 49 - d.y / 1e4))
      : pills.filter((p) => meets(box, p.x0, p.y0, p.x1, p.y1)).map((p) => pt(p.st, -122 + p.x0 / 1e4, 49 - p.y0 / 1e4));
}
const at = (x: number, y: number): [number, number] => [-122 + x / 1e4, 49 - y / 1e4];

describe("(T1194) the boxes", () => {
  it("the dot's target is 44 px square around its centre: the query box is 44 less the dot's own reach, each way; the pill is asked at the point", () => {
    expect(GAUGE_TAP_PX).toBe(44);
    const r = GAUGE_TAP_PX / 2 - GAUGE_DOT_REACH;
    expect(dotBox(100, 200)).toEqual([[100 - r, 200 - r], [100 + r, 200 + r]]);
    expect(pillBox(100, 200)).toEqual([[99, 199], [101, 201]]);
    expect([GAUGE_DOT_LAYER, GAUGE_PILL_LAYER]).toEqual(["gauge-dot", "gauge-label"]);
  });
});

describe("(T1194) what a tap opens: the dot, the pill, or the river (review 2026-11-27)", () => {
  const dots = [{ st: "08MH001", x: 100, y: 100 }, { st: "08MH002", x: 150, y: 100 }];
  const pills = [{ st: "08MH001", x0: 110, x1: 210, y0: 90, y1: 110 }];          // A's pill is 100 px wide and runs over B's dot
  const ask = (x: number, y: number) => queryGauge(screen(dots, pills), x, y, at(x, y));

  it("a tap on the dot opens the gauge; so does one inside its 44 px box (up to 22 px from the centre)", async () => {
    expect(await ask(100, 100)).toBe("08MH001");
    expect(await ask(100 - 22, 100)).toBe("08MH001");
    expect(await ask(100, 100 + 22)).toBe("08MH001");
  });
  it("a tap on a river beside a dot, 30 px from it, opens the WATER (no gauge)", async () => {
    expect(await ask(100 - 30, 100)).toBeNull();
    expect(await ask(100, 100 - 30)).toBeNull();
    expect(await ask(100 - 23, 100)).toBeNull();                       // one pixel past the box
  });
  it("a tap on pill A with dot B nearer opens A, not B (the pill is what is under the finger)", async () => {
    expect(Math.hypot(140 - 150, 0)).toBeLessThan(Math.hypot(140 - 100, 0));   // B's dot IS the nearer one
    expect(await ask(140, 100)).toBe("08MH001");
  });
  it("a tap on a river just under the long pill's edge but outside its rectangle opens the water; on the pill, the pill's gauge", async () => {
    expect(await ask(180, 90 - 8)).toBeNull();
    expect(await ask(180, 100)).toBe("08MH001");
  });
  it("two dots under one finger (no pill): the nearer; equal: the first listed", async () => {
    const two = [{ st: "A", x: 100, y: 100 }, { st: "B", x: 118, y: 100 }];
    expect(await queryGauge(screen(two, []), 112, 100, at(112, 100))).toBe("B");
    expect(await queryGauge(screen(two, []), 99, 100, at(99, 100))).toBe("A");
  });
});

describe("(T1194) a feature with no station is no gauge (never guessed)", () => {
  it("none, or unnamed, finds none", () => {
    expect(gaugeHit([], [], [-122, 49])).toBeNull();
    expect(gaugeHit([pt(undefined, -122, 49)], [pt("", -122, 49), pt(7, -122, 49)], [-122, 49])).toBeNull();
  });
});

describe("(T1194) the web half asks the pill at the point and the dots in the box, on the layers present", () => {
  const map = (layers: string[], found: (layer: string) => GaugeFeature[]) => {
    const q = vi.fn((_box: unknown, o: { layers: string[] }) => found(o.layers[0]!));
    return { q, m: { getLayer: (l: string) => (layers.includes(l) ? {} : undefined), queryRenderedFeatures: q } as unknown as maplibregl.Map };
  };
  it("a pill's station wins over a dot's", () => {
    const { q, m } = map(["gauge-dot", "gauge-label"], (l) => (l === "gauge-label" ? [pt("PILL", -122, 49)] : [pt("DOT", -122, 49)]));
    expect(hitGauge(m, 50, 60, { lat: 49, lng: -122 })).toBe("PILL");
    expect(q).toHaveBeenCalledWith(pillBox(50, 60), { layers: ["gauge-label"] });
    expect(q).toHaveBeenCalledWith(dotBox(50, 60), { layers: ["gauge-dot"] });
  });
  it("no gauge layer on the map (the Map tab): no query, no gauge", () => {
    const { q, m } = map([], () => [pt("X", -122, 49)]);
    expect(hitGauge(m, 50, 60, { lat: 49, lng: -122 })).toBeNull();
    expect(q).not.toHaveBeenCalled();
  });
});
