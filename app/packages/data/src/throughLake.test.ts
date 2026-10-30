/**
 * T296 — A GAUGE REACHED THROUGH A LAKE (owner 2026-10-29): "if no gauge is downstream I agree that is fine but if there
 * are other gauges closer they should take almost full amount of the prediction because through a lake acts as a natural
 * dampening system for the measurement."
 *
 * The upper Pitt (above Pitt Lake) as the bundle holds it: panel 1381 — its own gauge 08MH035 Pitt River near Port
 * Coquitlam (lead 1, level only, BELOW the lake) and North Alouette 08MH006 (a tributary joining below the lake). Both
 * reach the upper Pitt through Pitt Lake; neither has a rival with no lake between, so both keep full weight and the
 * sheet cautions. The mixed case puts 08MH017 Pitt River near Alvin (above the lake, discontinued — no panel holds it
 * today) back on the upper Pitt: it takes almost the whole answer, and the own gauge across the lake stops leading.
 */
import { describe, expect, it } from "vitest";
import { THROUGH_LAKE_FACTOR, THROUGH_LAKE_WORDS, throughLakeNote } from "@app/core";
import type { Panel, SectionId, StationId } from "./index";
import { answerFrom, contributionsOf, throughLakeStations, withThroughLake } from "./panel";
import { placesOf, routesOf } from "./sql/shared";

const s = (n: number) => n as unknown as SectionId;
const st = (id: string) => id as StationId;

describe("a route through a lake (routesOf)", () => {
  // the upper Pitt (1) flows into Pitt Lake (2), out to the lower Pitt (3, 4: 08MH035's reach); the Alouette (11) joins
  // at 3 below North Alouette's reach (10). 5 an unnamed lake on the Alouette above 11.
  const next = new Map<number, number>([[1, 2], [2, 3], [3, 4], [10, 5], [5, 11], [11, 3]]);
  const name = new Map<number, string>([[1, "Pitt River"], [2, "Pitt Lake"], [3, "Pitt River"], [4, "Pitt River"],
                                        [10, "North Alouette River"], [11, "Alouette River"]]);
  const down = async (f: SectionId) => (next.get(f as unknown as number) ?? null) as SectionId | null;
  const nameOf = async (x: SectionId) => {
    const n = name.get(x as unknown as number);
    return n ? { name: n, kind: n.endsWith("Lake") ? "lake" : "stream" } : null;
  };
  const places = placesOf([
    { station: "08MH006", name: "North Alouette River at 232nd Street", sid: 10, lon: -122.58, lat: 49.24 },
    { station: "08MH035", name: "Pitt River near Port Coquitlam", sid: 4, lon: -122.7, lat: 49.24 },
  ]);
  const members = [{ station: st("08MH035"), role: "down" as const }, { station: st("08MH006"), role: "up" as const }];

  it("marks both of the upper Pitt's gauges through Pitt Lake, below it, by the water's kind (a release before `lake`)",
     async () => {
    const [pitt, alouette] = await routesOf(s(1), members, places, down, { nameOf });
    expect(pitt).toMatchObject({ throughLake: true, lakeName: "Pitt Lake", lakeSide: "below" });
    expect(alouette).toMatchObject({ throughLake: true, lakeName: "Pitt Lake", lakeSide: "below" });
  });

  it("reads `section_down.lake` first: an unnamed lake counts, and a 0 overrules the kind", async () => {
    const lakeOf = async (x: SectionId) => [2, 5].includes(x as unknown as number);
    // below the lake, the Alouette's gauge crosses only the unnamed lake on its own water: above it, nameless
    const [, a] = await routesOf(s(3), members, places, down, { nameOf, lakeOf });
    expect(a!.throughLake).toBe(true);
    expect(a!.lakeName).toBeUndefined();
    expect(a!.lakeSide).toBe("above");
    // the lower Pitt to its own gauge: no lake between
    const [p] = await routesOf(s(3), members, places, down, { nameOf, lakeOf });
    expect(p!.throughLake).toBeUndefined();
    // the release says Pitt Lake's reach is no lake: the kind is not asked
    const [q] = await routesOf(s(1), members, places, down, { nameOf, lakeOf: async () => false });
    expect(q!.throughLake).toBeUndefined();
  });

  it("does not count the spot's own reach or the gauge's", async () => {
    // a spot IN the lake, to the gauge below it: nothing between is a lake
    const [r] = await routesOf(s(2), members, places, down, { nameOf });
    expect(r!.throughLake).toBeUndefined();
  });
});

describe("the weighting (answerFrom, core's lakeDamped)", () => {
  const UPPER_PITT: Panel = { areaKm2: 1191.3, members: [
    { station: st("08MH035"), role: "up", areaKm2: 1191.3, years: { discharge: 0, level: 8 }, regulated: false,
      sameRiver: true, lead: 1 },
    { station: st("08MH006"), role: "up", areaKm2: 26.4, years: { discharge: 69, level: 15 }, regulated: false,
      sameRiver: false },
  ] };
  const ALVIN = { station: st("08MH017"), role: "up" as const, areaKm2: 508.3, years: { discharge: 30, level: 30 },
                  regulated: false, sameRiver: true };
  const INDEX = { stations: {
    "08MH035": { percentile: 0.05, parameter: "level" as const, level: 0.05 },
    "08MH006": { percentile: 0.2, parameter: "discharge" as const, discharge: 0.2, level: 0.3 },
    "08MH017": { percentile: 0.8, parameter: "level" as const, discharge: 0.8, level: 0.8 },
  } };
  const lakes = new Set([st("08MH035"), st("08MH006")]);
  const shares = (p: Panel | undefined, q: "discharge" | "level") =>
    Object.fromEntries(answerFrom(p, INDEX, q).rows.map((r) => [r.station, r.weight]));

  it("leaves the upper Pitt as it is: every gauge crosses the lake, so each keeps its weight", () => {
    for (const q of ["discharge", "level"] as const)
      expect(answerFrom(withThroughLake(UPPER_PITT, lakes), INDEX, q)).toEqual(answerFrom(UPPER_PITT, INDEX, q));
    // the own gauge still leads Level, across the lake: it is the best there is
    expect(shares(withThroughLake(UPPER_PITT, lakes), "level")["08MH035"]).toBeGreaterThanOrEqual(0.7);
  });

  it("hands almost the whole answer to a gauge with no lake between, and the own gauge across the lake stops leading",
     () => {
    const mixed = withThroughLake({ ...UPPER_PITT, members: [...UPPER_PITT.members, ALVIN] }, lakes)!;
    const w = shares(mixed, "level");
    expect(w["08MH017"]).toBeGreaterThan(0.9);
    expect(w["08MH035"]! + w["08MH006"]!).toBeLessThan(0.1);
    // and the arithmetic is the one rule: each through-lake vote at THROUGH_LAKE_FACTOR of its usual weight, no lead
    const plain = answerFrom({ ...UPPER_PITT, members: [...UPPER_PITT.members.map(({ lead: _l, ...m }) => m), ALVIN] },
                             INDEX, "level").rows;
    const raw = Object.fromEntries(plain.map((r) => [r.station, r.factors.share * r.factors.role * r.factors.record]));
    const total = raw["08MH017"]! + THROUGH_LAKE_FACTOR * (raw["08MH035"]! + raw["08MH006"]!);
    expect(w["08MH017"]).toBeCloseTo(raw["08MH017"]! / total, 9);
    const a = answerFrom(mixed, INDEX, "level").answer;
    if (!a.ok) throw new Error("expected an estimate");
    expect(a.value.percentile).toBeGreaterThan(0.7);    // Alvin's 80th, barely moved by the 5th and 30th across the lake
    expect(contributionsOf(mixed, INDEX, "level", 0).map((c) => c.throughLake ?? false)).toEqual([true, true, false]);
  });

  it("knows nothing of lakes until the routes are walked (the map's colour, a saved spot)", () => {
    expect(withThroughLake(UPPER_PITT, null)).toBe(UPPER_PITT);
    expect(withThroughLake(UPPER_PITT, new Set())).toBe(UPPER_PITT);
    expect(throughLakeStations(null)).toBeNull();
  });
});

describe("the caution line (core's throughLakeNote)", () => {
  type Route = { path: readonly unknown[]; throughLake?: boolean; lakeName?: string; lakeSide?: "below" | "above" };
  const row = (station: string, route: Route | undefined, percentile: number | null = 0.3) => ({
    station, role: "up" as const, percentile, weight: percentile === null ? 0 : 0.5, areaRatio: 1, areaKm2: 100,
    years: 20, regulated: false, factors: { share: 1, role: 1, record: 1 }, ...(route ? { route } : {}),
  });
  const below = { path: [1, 2, 3], throughLake: true, lakeName: "Pitt Lake", lakeSide: "below" as const };

  it("says where the gauges stand when every counted one crosses one named lake", () => {
    expect(throughLakeNote([row("08MH035", below), row("08MH006", below)]))
      .toBe("Measured below Pitt Lake — the lake smooths changes, so read it loosely.");
    expect(THROUGH_LAKE_WORDS.named("below", "Pitt Lake")).toContain("Pitt Lake");
  });

  it("says it generally across an unnamed lake or two lakes, and nothing beside a gauge with no lake between", () => {
    expect(throughLakeNote([row("A", { path: [1, 2, 3], throughLake: true })])).toBe(THROUGH_LAKE_WORDS.general);
    expect(throughLakeNote([row("A", below), row("B", { ...below, lakeName: "Alouette Lake" })]))
      .toBe(THROUGH_LAKE_WORDS.general);
    expect(throughLakeNote([row("A", below), row("B", { path: [1, 2] })])).toBeNull();
    // a quiet gauge with no lake between does not count, so the caution stands
    expect(throughLakeNote([row("A", below), row("B", { path: [1, 2] }, null)])).not.toBeNull();
    expect(throughLakeNote([row("A", undefined)])).toBeNull();
  });
});
