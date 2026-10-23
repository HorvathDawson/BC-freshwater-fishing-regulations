/**
 * The gauge marks on the Conditions map.
 *
 * A DOT ALONE SAYS "SOMETHING IS HERE", which is not what this view is for. Every test here
 * checks that a mark either carries its reading or is not drawn at all.
 */
import { describe, expect, it } from "vitest";
import { zoomForMagnitude } from "@app/core";
import type { Horizon } from "@app/data";
import { gaugeGeoJSON, gaugeLabel } from "./gaugePoints";

const pts = [{ station: "08MH001", name: "Chilliwack R", lon: -121.9, lat: 49.1 },
             { station: "08QUIET", name: "Quiet Ck", lon: -122.0, lat: 49.2 }];
const idx = (stations: Record<string, { percentile: number | null }>) =>
  ({ fetchedAt: "t", stations } as never);

describe("gaugeLabel", () => {
  it("pairs the reading with where it sits against normal", () => {
    expect(gaugeLabel({ discharge: 15.7, level: null }, 0.04)).toBe("15.7 m³/s · p4th");
  });

  it("uses metres for a station that measures stage, not m³/s", () => {
    // 237 BC stations never measure discharge. Labelling a level as m³/s would be a claim
    // about the whole river made from one cross-section.
    expect(gaugeLabel({ discharge: null, level: 1.48, parameter: "level" }, 0.5))
      .toBe("1.48 m · p50th");
  });

  it("writes the percentile the way a person reads one", () => {
    expect(gaugeLabel(undefined, 0.03)).toBe("p3rd");
    expect(gaugeLabel(undefined, 0.02)).toBe("p2nd");
    expect(gaugeLabel(undefined, 0.11)).toBe("p11th");
    expect(gaugeLabel(undefined, 0.21)).toBe("p21st");
  });

  it("is null when there is nothing to say", () => {
    expect(gaugeLabel({ discharge: null, level: null }, null)).toBeNull();
  });
});

describe("gaugeGeoJSON", () => {
  it("draws a station that is reading", () => {
    const g = JSON.parse(gaugeGeoJSON(pts, idx({ "08MH001": { percentile: 0.04 } }))!);
    expect(g.features).toHaveLength(1);
    expect(g.features[0].properties.label).toBe("p4th");
    expect(g.features[0].geometry.coordinates).toEqual([-121.9, 49.1]);
  });

  it("draws nothing for a station that is not transmitting", () => {
    const g = JSON.parse(gaugeGeoJSON(pts, idx({ "08MH001": { percentile: 0.04 } }))!);
    expect(g.features.map((f: { properties: { station: string } }) => f.properties.station))
      .not.toContain("08QUIET");
  });

  it("drops a bare dot rather than drawing one with no number", () => {
    // A dot with no reading invites the reader to assume the map failed, rather than that
    // the station has nothing to say today.
    expect(gaugeGeoJSON(pts, idx({ "08MH001": { percentile: null } }))).toBeNull();
  });

  it("returns null when the feed could not be reached", () => {
    expect(gaugeGeoJSON(pts, null)).toBeNull();
  });
});

/*
 * T126: THE DOTS FOLLOW THE HORIZON, like the rivers. They always showed now, so under +5 a dot said today beside a river
 * coloured for Friday. Whole feature properties at every horizon, read through `reading` (the panels' one reader).
 *
 * Mutations: pass `0` instead of `horizon` to `reading` in gaugeGeoJSON -> the +1/+3/+5/+10/Change cases fail (ramp 40,
 * label "p40th" everywhere); drop the horizon from `toRamp` -> Change's ramp is 69.15, not 269.15 (painted on the
 * percentile ramp); drop `changeDotLabel` -> Change's label is "p69th".
 */
describe("gaugeGeoJSON at a horizon", () => {
  const row = {
    percentile: 0.4, parameter: "discharge", discharge: 0.4, level: 0.7,
    ahead: { "1": { discharge: 0.5, model: "CLEVER" }, "3": { discharge: 0.9, level: 0.2, model: "ELF" } },
    change: { discharge: 0.6915, models: ["CLEVER"], dir: "rise", ratio: 2, day: "2026-10-25", shape: "spike" },
  };
  const at = (horizon: Horizon, quantity: "flow" | "level" = "flow") => {
    const g = gaugeGeoJSON([pts[0]!], { fetchedAt: "t", stations: { "08MH001": row } } as never, quantity, horizon);
    return g === null ? null : JSON.parse(g).features.map((f: { properties: object }) => f.properties);
  };
  const minz = zoomForMagnitude(null);
  const dot = (label: string, ramp: number) => [{ station: "08MH001", name: "Chilliwack R", label, ramp, minz }];

  it("Now is the station's reading", () => {
    expect([at(0), at(0, "level")]).toEqual([dot("p40th", 40), dot("p70th", 70)]);
  });

  it("a day ahead is that day's forecast percentile, in that quantity", () => {
    expect([at(1), at(3), at(3, "level")]).toEqual([dot("p50th", 50), dot("p90th", 90), dot("p20th", 20)]);
  });

  it("a day no run reaches draws no dot — never today's", () => {
    expect([at(5), at(10), at(1, "level")]).toEqual([null, null, null]);
  });

  it("Change is the expected change, on the change scale, said as a ratio to today", () => {
    expect(at("change")).toEqual([{ station: "08MH001", name: "Chilliwack R", label: "2× today",
                                    ramp: expect.closeTo(269.15, 6), minz }]);
    expect(at("change", "level")).toBeNull();          // Change is flow only
  });

  it("Change's label is the station's own ratio, not the clamped number the colour rides", () => {
    // 08HB025's COFFEE run, 544x: coded, it sits at the 0.995 clamp, which decodes to about 36x
    const big = { ...row, change: { ...row.change, discharge: 0.995, ratio: 544 } };
    const g = gaugeGeoJSON([pts[0]!], { fetchedAt: "t", stations: { "08MH001": big } } as never, "flow", "change");
    expect(JSON.parse(g!).features[0].properties.label).toBe("100×+ today");
  });
});
