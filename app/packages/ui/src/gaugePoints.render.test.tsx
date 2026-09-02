/**
 * The gauge marks on the Conditions map.
 *
 * A DOT ALONE SAYS "SOMETHING IS HERE", which is not what this view is for. Every test here
 * checks that a mark either carries its reading or is not drawn at all.
 */
import { describe, expect, it } from "vitest";
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
