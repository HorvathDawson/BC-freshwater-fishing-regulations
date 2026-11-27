/** (T1194) The gauge tap's one rule: a 44 px box, the nearest dot, no station no gauge. Both engines read these. */
import { describe, expect, it, vi } from "vitest";
import type * as maplibregl from "maplibre-gl";
import { GAUGE_TAP_LAYERS, GAUGE_TAP_PX, gaugeBox, gaugeHit, type GaugeFeature } from "./gauge-hit";
import { hitGauge } from "./web/gaugeHit";

const dot = (station: unknown, lon: number, lat: number): GaugeFeature =>
  ({ properties: { station }, geometry: { type: "Point", coordinates: [lon, lat] } });

describe("(T1194) the gauge hit box", () => {
  it("is 44 px square around the press, no more and no less (a finger, not a pixel; not the water's 24)", () => {
    expect(GAUGE_TAP_PX).toBe(44);
    expect(gaugeBox(100, 200)).toEqual([[78, 178], [122, 222]]);
    const [[x0, y0], [x1, y1]] = gaugeBox(0, 0);
    expect([x1 - x0, y1 - y0]).toEqual([44, 44]);
  });
  it("asks the dot and its label pill, and nothing else", () => {
    expect([...GAUGE_TAP_LAYERS]).toEqual(["gauge-dot", "gauge-label"]);
  });
});

describe("(T1194) the station under a press", () => {
  const at: [number, number] = [-122, 49];
  it("takes the NEAREST of two dots, whatever the listing order, never a list", () => {
    const near = dot("08MH001", -122.001, 49.001), far = dot("08MH002", -122.01, 49.01);
    expect(gaugeHit([far, near], at)).toBe("08MH001");
    expect(gaugeHit([near, far], at)).toBe("08MH001");
  });
  it("none found, or a feature with no station, is no gauge (never guessed)", () => {
    expect(gaugeHit([], at)).toBeNull();
    expect(gaugeHit([dot(undefined, -122, 49), dot("", -122, 49), dot(7, -122, 49)], at)).toBeNull();
  });
  it("a pill's feature (the same station) is the dot's gauge", () => {
    expect(gaugeHit([dot("08MH024", -122.0005, 49.0002)], at)).toBe("08MH024");
  });
});

describe("(T1194) the web half asks the shared box over the gauge layers present", () => {
  const map = (layers: string[], found: GaugeFeature[]) => {
    const q = vi.fn(() => found);
    return { q, m: { getLayer: (l: string) => (layers.includes(l) ? {} : undefined), queryRenderedFeatures: q } as unknown as maplibregl.Map };
  };
  it("queries gaugeBox(x, y) on both layers and returns the nearest station", () => {
    const { q, m } = map(["gauge-dot", "gauge-label"], [dot("08MH024", -122, 49)]);
    expect(hitGauge(m, 50, 60, { lat: 49, lng: -122 })).toBe("08MH024");
    expect(q).toHaveBeenCalledWith(gaugeBox(50, 60), { layers: ["gauge-dot", "gauge-label"] });
  });
  it("no gauge layer on the map (the Map tab): no query, no gauge", () => {
    const { q, m } = map([], [dot("08MH024", -122, 49)]);
    expect(hitGauge(m, 50, 60, { lat: 49, lng: -122 })).toBeNull();
    expect(q).not.toHaveBeenCalled();
  });
});
