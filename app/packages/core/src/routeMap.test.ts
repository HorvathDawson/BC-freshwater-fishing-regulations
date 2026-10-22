/** The route map's words, whole. */
import { describe, expect, it } from "vitest";
import { donorMapKey, donorMapLabel, hereMapLabel, ROUTE_MAP_WORDS, routeLegend } from "./routeMap";

describe("the route map's words", () => {
  it("labels a gauge with its share, in the panel's own words", () => {
    expect(donorMapLabel({ weight: 0.62, percentile: 0.3 })).toBe("62%");
    expect(donorMapLabel({ weight: 0.004, percentile: 0.3 })).toBe("<1%");
    expect(donorMapLabel({ weight: 0, percentile: null })).toBe("not reporting");
    expect(donorMapLabel({ weight: 0, percentile: null, realtime: true })).toBe("not reporting");
    expect(donorMapLabel({ weight: 0, percentile: null, realtime: false })).toBe("not a real-time gauge");
  });

  it("keys a gauge with the same words its dot carries", () => {
    expect(donorMapKey({ station: "08MH001", weight: 0.62, percentile: 0.3 }))
      .toBe("08MH001 · 62%");
    expect(donorMapKey({ station: "08MH024", weight: 0, percentile: null }))
      .toBe("08MH024 · not reporting");
    expect(donorMapKey({ station: "08MH024", weight: 0, percentile: null, realtime: false }))
      .toBe("08MH024 · not a real-time gauge");
  });

  it("names the spot as its key does", () => {
    expect(hereMapLabel()).toBe("you are here");
  });

  it("keys only the roles drawn, downstream first", () => {
    expect(routeLegend(["up", "down", "up"])).toEqual([
      { role: "down", label: "gauge downstream: your water flows on to it" },
      { role: "up", label: "gauge upstream: its water flows down to you" },
    ]);
    expect(routeLegend(["up"])).toEqual([
      { role: "up", label: "gauge upstream: its water flows down to you" }]);
    expect(routeLegend([])).toEqual([]);
  });

  it("keeps the map's control words in one table", () => {
    expect(ROUTE_MAP_WORDS).toEqual({
      down: "gauge downstream: your water flows on to it",
      up: "gauge upstream: its water flows down to you",
      quiet: "not reporting",
      notRealtime: "not a real-time gauge",
      fit: "Fit",
      fitLabel: "Fit the map to you and the gauges",
      larger: "Larger map",
      smaller: "Smaller map",
    });
  });
});
