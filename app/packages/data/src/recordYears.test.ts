/**
 * T110 — THE RECORD IS PER QUANTITY. A Level answer weighs each donor by its LEVEL record, a Flow answer by its
 * DISCHARGE record, and a basin's Flow / Level field likewise. 07ED001 has 59 years of discharge and 4 of level; the
 * bundle used to ship one `years` (the longest, 59), so its level reading was weighted as a 59-year record.
 *
 * Mutation pins (each fails the named test):
 *   · `recordYears` returning `Math.max(record.discharge, record.level)`        → "07ED001's level weight uses 4 years"
 *   · panel.ts `answerFrom` reading `m.years.discharge` whatever the quantity   → "07ED001's level weight uses 4 years"
 *   · panel.ts `contributionsOf` passing the discharge record                  → "the map's Level standing weighs …"
 *   · standing.ts `basinStandings` passing the discharge record                → "a basin's Level field weighs …"
 *   · trust.ts `weightFactors` dropping the `MIN_RECORD_YEARS` floor           → "a record under the floor ranks nothing"
 *   · trust.ts `basinStanding` computing its own `years / RECORD_FULL_YEARS`    → "a basin vote under the floor weighs
 *     nothing" and tools/record-factor.test.ts (adversarial review: the basin field had its own copy, which the floor missed)
 */
import { describe, expect, it } from "vitest";
import { basinStanding, estimate, MIN_RECORD_YEARS, recordFactor, recordYears, weightFactors, weightFor } from "@app/core";
import type { BasinMember, Panel, StationId } from "./index";
import { answerFrom, contributionsOf, panelStanding } from "./panel";
import { basinStandings } from "./standing";

const st = (s: string) => s as StationId;
const both = (p: number) => ({ percentile: p, parameter: "discharge" as const, discharge: p, level: p });
const feed = (stations: Record<string, ReturnType<typeof both>>) => ({ stations });

/** 07ED001 (59 years of discharge, 4 of level) beside a neighbour with 20 of each, both the reach's own size. */
const PANEL: Panel = { areaKm2: 100, members: [
  { station: st("07ED001"), role: "up", areaKm2: 100, years: { discharge: 59, level: 4 }, regulated: false, sameRiver: true },
  { station: st("OTHER"), role: "up", areaKm2: 100, years: { discharge: 20, level: 20 }, regulated: false, sameRiver: true },
] };
const IDX = feed({ "07ED001": both(0.1), OTHER: both(0.9) });

describe("recordYears", () => {
  it("is the record of the quantity asked, never the longest", () => {
    const r = { discharge: 59, level: 4 };
    expect([recordYears(r, "discharge"), recordYears(r, "level")]).toEqual([59, 4]);
  });
});

describe("a panel's weights, per quantity", () => {
  it("07ED001's level weight uses 4 years", () => {
    const level = answerFrom(PANEL, IDX, "level").rows.find((r) => r.station === "07ED001")!;
    expect([level.years, level.factors]).toEqual([4, weightFactors(1, "up", 4, true)]);
    expect(level.factors.record).toBeCloseTo(4 / 20, 12);
    // the whole answer: 4/20 against 20/20, the two shares that follow from it
    const w = [weightFor(1, "up", 4), weightFor(1, "up", 20)];
    expect(answerFrom(PANEL, IDX, "level").rows.map((r) => [r.station, Number(r.weight.toFixed(6))]))
      .toEqual([["OTHER", Number((w[1]! / (w[0]! + w[1]!)).toFixed(6))],
                ["07ED001", Number((w[0]! / (w[0]! + w[1]!)).toFixed(6))]]);
  });

  it("Flow still weighs the 59-year discharge record (unchanged)", () => {
    const flow = answerFrom(PANEL, IDX, "discharge").rows.find((r) => r.station === "07ED001")!;
    expect([flow.years, flow.factors.record]).toEqual([59, 1]);
  });

  it("the map's Level standing weighs the level record — the sheet's answer, from the same contributions", () => {
    const c = contributionsOf(PANEL, IDX, "level", 0);
    expect(c.map((x) => x.years)).toEqual([4, 20]);
    const a = estimate(100, c);
    expect(a.ok).toBe(true);
    if (a.ok) expect(panelStanding(PANEL, IDX, "level", 0)).toBe(a.value.percentile);
    expect(contributionsOf(PANEL, IDX, "discharge", 0).map((x) => x.years)).toEqual([59, 20]);
  });
});

describe("the basin field, per quantity", () => {
  it("a basin's Level field weighs the level record, its Flow field the discharge record", () => {
    const basins = new Map<string, readonly BasinMember[]>([["08LD", [
      { station: st("07ED001"), areaKm2: 100, years: { discharge: 59, level: 4 } },
      { station: st("OTHER"), areaKm2: 100, years: { discharge: 20, level: 20 } },
    ]]]);
    const level = basinStandings(basins, IDX, "level", 0).get("08LD");
    const flow = basinStandings(basins, IDX, "discharge", 0).get("08LD");
    expect(level).toBe(basinStanding([{ percentile: 0.1, areaKm2: 100, years: 4 },
                                      { percentile: 0.9, areaKm2: 100, years: 20 }]));
    expect(flow).toBe(basinStanding([{ percentile: 0.1, areaKm2: 100, years: 59 },
                                     { percentile: 0.9, areaKm2: 100, years: 20 }]));
    expect(level).not.toBe(flow);
  });
});

describe("the record floor", () => {
  it("a record under the floor ranks nothing, at the floor it counts floor/20 — the pipeline's MIN_RECORD_YEARS", () => {
    expect(MIN_RECORD_YEARS).toBe(3);
    expect(weightFactors(1, "up", MIN_RECORD_YEARS - 1).record).toBe(0);
    expect(weightFactors(1, "up", 0).record).toBe(0);
    expect(weightFactors(1, "up", MIN_RECORD_YEARS).record).toBeCloseTo(MIN_RECORD_YEARS / 20, 12);
  });
});

describe("one record factor (adversarial review, T110)", () => {
  it("a basin vote under the floor weighs nothing — the field holds the panels' floor", () => {
    const alone = basinStanding([{ percentile: 0.9, areaKm2: 100, years: 20 }]);
    expect(basinStanding([{ percentile: 0.1, areaKm2: 100, years: MIN_RECORD_YEARS - 1 },
                          { percentile: 0.9, areaKm2: 100, years: 20 }])).toBe(alone);
    expect(basinStanding([{ percentile: 0.1, areaKm2: 100, years: MIN_RECORD_YEARS - 1 }])).toBeNull();
    // at the floor it counts floor/20, as a panel donor does
    expect(recordFactor(MIN_RECORD_YEARS)).toBe(weightFactors(1, "up", MIN_RECORD_YEARS).record);
  });
});
