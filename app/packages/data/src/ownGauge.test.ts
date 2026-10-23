/**
 * THE PITT, as the sheet and the map read it (owner 2026-10-22: "the pitt could use its own gauge"; "we dont want to
 * mix flow and level").
 *
 * The lower Pitt's panel as the bundle holds it (sid 308351, target 1,181.2 km²), and the feed of 2026-10-21: its own
 * gauge 08MH035 Pitt River near Port Coquitlam records LEVEL only (5th percentile); North Alouette 08MH006 reports
 * both (discharge 16.6th, level 27.1th); Widgeon and Smiling creeks are quiet. Flow is North Alouette's alone and the
 * own gauge does not touch it; Level is led by the own gauge at 70%.
 */
import { describe, expect, it } from "vitest";
import { probit } from "@app/core";
import type { Panel, StationId } from "./index";
import { answerFrom, contributionsOf, panelStanding } from "./panel";

const st = (s: string) => s as StationId;
const PITT: Panel = { areaKm2: 1181.2, members: [
  { station: st("08MH035"), role: "down", areaKm2: 1181.2, years: { discharge: 8, level: 8 }, regulated: false, sameRiver: true, lead: 1 },
  { station: st("08MH006"), role: "up", areaKm2: 26.4, years: { discharge: 69, level: 69 }, regulated: false, sameRiver: false },
  { station: st("08MH010"), role: "up", areaKm2: 200.9, years: { discharge: 4, level: 4 }, regulated: false, sameRiver: false },
  { station: st("08MH092"), role: "up", areaKm2: 1.4, years: { discharge: 5, level: 5 }, regulated: false, sameRiver: false },
] };
const UNLED: Panel = { ...PITT, members: PITT.members.map(({ lead: _lead, ...m }) => m) };

const INDEX = { stations: {
  "08MH035": { percentile: 0.05, parameter: "level" as const, level: 0.05 },
  "08MH006": { percentile: 0.166, parameter: "discharge" as const, discharge: 0.166, level: 0.271 },
} };

const shares = (p: Panel, q: "discharge" | "level") =>
  answerFrom(p, INDEX, q).rows.map((r) => [r.station, Number(r.weight.toFixed(4))]);

describe("a level-only own gauge", () => {
  it("does not touch Flow: the answer, the rows and the map's value are the unled ones", () => {
    expect(answerFrom(PITT, INDEX, "discharge")).toEqual(answerFrom(UNLED, INDEX, "discharge"));
    expect(shares(PITT, "discharge")).toEqual([["08MH006", 1], ["08MH035", 0], ["08MH010", 0], ["08MH092", 0]]);
    expect(panelStanding(PITT, INDEX, "discharge", 0)).toBe(panelStanding(UNLED, INDEX, "discharge", 0));
  });

  it("leads Level with the main weight, North Alouette keeping the rest", () => {
    // unled, the own gauge already carried 67% on its own merits (1x, 8 years against 45x, 69 years, a tributary)
    expect(shares(UNLED, "level")).toEqual([["08MH035", 0.6662], ["08MH006", 0.3338], ["08MH010", 0], ["08MH092", 0]]);
    expect(shares(PITT, "level")).toEqual([["08MH035", 0.7], ["08MH006", 0.3], ["08MH010", 0], ["08MH092", 0]]);
    const led = answerFrom(PITT, INDEX, "level").answer;
    if (!led.ok) throw new Error("expected a Level estimate");
    expect(probit(led.value.percentile)).toBeCloseTo(0.7 * probit(0.05) + 0.3 * probit(0.271), 5);
    expect(Math.round(led.value.percentile * 100)).toBe(9);
  });

  it("hands the map the same value the sheet shows", () => {
    const sheet = answerFrom(PITT, INDEX, "level").answer;
    if (!sheet.ok) throw new Error("expected a Level estimate");
    expect(panelStanding(PITT, INDEX, "level", 0)).toBe(sheet.value.percentile);
  });

  it("carries its rank into the arithmetic and nothing for a member that has none", () => {
    expect(contributionsOf(PITT, INDEX, "level", 0).map((c) => c.lead ?? 0)).toEqual([1, 0]);
  });
});
