/** The route map's words, whole. */
import { describe, expect, it } from "vitest";
import { donorMapKey, donorMapLabel, donorRelation, hereMapLabel, joinMapLabel, ROUTE_MAP_WORDS, routeLegend, stationWater } from "./routeMap";

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
      flowOnly: "flow forecast only",
      noRecord: "no record to compare with",
      fit: "Fit",
      fitLabel: "Fit the map to you and the gauges",
      larger: "Larger map",
      smaller: "Smaller map",
    });
  });
});

describe("donorRelation (owner 2026-10-28)", () => {
  const base = { station: "08MH006", name: "NORTH ALOUETTE RIVER AT 232ND STREET, MAPLE RIDGE", role: "up" as const,
                 reaches: 5, joined: true, km: 12.4, apart: "45× apart" };
  it("names the donor's water, says where it joins and that it is scaled by drainage", () => {
    expect(donorRelation(base)).toBe(
      "08MH006 is on North Alouette River, which joins this water below here (the gauge is about 12 km away); scaled by drainage (45× apart).");
  });
  it("says a gauge on this water's line by its reaches, and an unjoined one by its country", () => {
    expect(donorRelation({ ...base, joined: false, role: "down", reaches: 3 })).toContain("2 reaches downstream on this water's line");
    expect(donorRelation({ ...base, joined: false, reaches: 0 })).toContain("no mapped path joins it to this water");
  });
  it("names the station alone when its name has no water to split off", () => {
    expect(stationWater("SOMEWHERE")).toBeNull();
    expect(donorRelation({ ...base, name: null, km: null })).toMatch(/^08MH006, which joins this water below here;/);
  });
  // T252: a drawn path says its distance along the river and where the waters meet; the straight line only without one
  it("says a joined gauge's distance along the river and the confluence, not the straight line", () => {
    expect(donorRelation({ ...base, alongKm: 34.2, joinWater: "Alouette River" })).toBe(
      "08MH006 is on North Alouette River, which joins this water at the mouth of Alouette River, about 34 km along the river; scaled by drainage (45× apart).");
    expect(donorRelation({ ...base, alongKm: 4.26, joinWater: null })).toContain("which joins this water below here, about 4.3 km along the river;");
    expect(donorRelation({ ...base, joined: false, role: "down", reaches: 3, alongKm: 7.04 }))
      .toContain("about 7.0 km downstream on this water's line");
    // no path: the straight line stays, whatever lengths there are
    expect(donorRelation({ ...base, joined: false, reaches: 0, alongKm: 50 })).toContain("about 12 km away: no mapped path");
  });
  it("labels the confluence on the map with the same distance", () => {
    expect(joinMapLabel("Alouette River", 34.2)).toBe("joins at the mouth of Alouette River · about 34 km along the river");
    expect(joinMapLabel(null, 2.04)).toBe("joins here · about 2.0 km along the river");
    expect(joinMapLabel("Alouette River", null)).toBe("joins at the mouth of Alouette River");
  });
  // T252 item 3: the bundle's names come cased by the shared caser (T113); only an old release's capitals are re-cased
  it("keeps a cased station name as it comes (McGregor, not Mcgregor)", () => {
    expect(stationWater("McGregor River at Lower Canyon")).toBe("McGregor River");
    expect(stationWater("North Alouette River at 232nd Street, Maple Ridge")).toBe("North Alouette River");
    expect(stationWater("NORTH ALOUETTE RIVER AT 232ND STREET")).toBe("North Alouette River");
  });
});
