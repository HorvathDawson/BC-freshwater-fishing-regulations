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

  it("a gauge that reports but cannot be ranked is said as that — never 'not reporting' beside a table that says it reports", () => {
    const ref = Date.parse("2026-10-21T05:00:00Z");
    const r = { station: "08MH024", weight: 0, percentile: null, observedAt: "2026-10-20T20:35:00-08:00" };
    expect(donorMapLabel(r, { ref })).toBe("reporting, not ranked");
    expect(donorMapKey(r, { ref })).toBe("08MH024 · reporting, not ranked");
    // A report more than a day old is silence, as `donorReads` says it.
    expect(donorMapLabel(r, { ref: ref + 2 * 86_400_000 })).toBe("not reporting");
  });

  it("a day ahead with no forecast says no forecast — not 'not reporting' (T123)", () => {
    const r = { station: "08MH024", weight: 0, percentile: null };
    expect([donorMapLabel(r, { horizon: 3 }), donorMapLabel(r, { horizon: "change" }), donorMapKey(r, { horizon: 3 }),
            donorMapLabel({ ...r, realtime: false }, { horizon: 3 }), donorMapLabel({ ...r, percentile: 0.4 }, { horizon: 3 })])
      .toEqual(["no forecast", "no forecast", "08MH024 · no forecast", "not a real-time gauge", "0%"]);
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
      unranked: "reporting, not ranked",
      notRealtime: "not a real-time gauge",
      noForecast: "no forecast",
      fit: "Fit",
      fitLabel: "Fit the map to you and the gauges",
      larger: "Larger map",
      smaller: "Smaller map",
    });
  });
});
