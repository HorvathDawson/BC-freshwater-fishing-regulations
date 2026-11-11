/**
 * (T636, owner 2026-11-11: "the level only tag in the conditions map is a lot") its words left the map: the ring says it.
 * T586 (T580 F10, owner 2026-11-10: "it should be muted for the gauge") — a station silent in the asked quantity that
 * reports the other today KEEPS ITS DOT on the browse map, muted, with the words its row and pin use. Whole properties.
 */
import { describe, expect, it } from "vitest";
import { gaugeGeoJSON } from "./gaugePoints";

const pts = [{ station: "08MH024", name: "Fraser R at Mission", lon: -122.3, lat: 49.1 },
             { station: "08KA004", name: "Fraser R at Hansard", lon: -120.4, lat: 53.9 },
             { station: "QUIET", name: "Quiet Ck", lon: -122.0, lat: 49.2 }];
const idx = {
  fetchedAt: "t",
  stations: {
    "08MH024": { percentile: 0.66, parameter: "level", level: 0.66 },                       // level only
    "08KA004": { percentile: 0.27, parameter: "discharge", discharge: 0.27, level: 0.3 },  // both
    "QUIET": { percentile: null },                                                           // reads nothing
  },
} as never;
const props = (quantity: "flow" | "level", horizon = 0 as never) => {
  const g = gaugeGeoJSON(pts, idx, quantity, horizon);
  return g ? (JSON.parse(g).features as { properties: Record<string, unknown> }[])
             .map((f) => f.properties).sort((a, b) => String(a.station).localeCompare(String(b.station))) : [];
};

describe("the browse map's dot for a station silent in the asked quantity", () => {
  it("under Flow: Mission keeps its dot, muted, with NO words on the map (T636: its ring says it); Hansard is coloured", () => {
    const p = props("flow");
    expect(p.map((x) => x.station)).toEqual(["08KA004", "08MH024"]);
    expect(p[1]).toEqual({ station: "08MH024", name: "Fraser R at Mission", ramp: null, muted: true, only: "level", minz: expect.any(Number) });
    expect(p[1]).not.toHaveProperty("label");
    expect(p[0]).toMatchObject({ station: "08KA004", label: "7 in 10 days higher", ramp: 27 });
    expect(p[0]).not.toHaveProperty("muted");
  });

  it("under Level: Mission is coloured and Hansard (which reports both) too; nobody is muted", () => {
    const p = props("level");
    expect(p.map((x) => x.station)).toEqual(["08KA004", "08MH024"]);
    expect(p.some((x) => x.muted)).toBe(false);
  });

  it("a flow-only station is muted under Level the same way: no label, ringed", () => {
    const g = JSON.parse(gaugeGeoJSON([pts[1]!], { fetchedAt: "t", stations: { "08KA004": { percentile: 0.27, parameter: "discharge", discharge: 0.27 } } } as never, "level", 0)!);
    expect(g.features[0].properties).toMatchObject({ muted: true, only: "discharge", ramp: null });
    expect(g.features[0].properties).not.toHaveProperty("label");
  });

  it("a day ahead stays without a dot where no run reaches it (the river has no colour there either)", () => {
    expect(props("flow", 3 as never)).toEqual([]);
  });
});
