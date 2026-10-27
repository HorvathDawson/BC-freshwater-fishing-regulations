/**
 * T209 golden cases: one example water per kind of vessel restriction (the ticket's own examples), the two-limits reading,
 * and a water with no vessel rule (no line).
 */
import { describe, expect, it } from "vitest";
import { boatChips, boatKind } from "./boats";
import type { ExportRule, GearAnswer, RuleFacts, RuleIndex, RuleView } from "./inputs";
import { ruleView } from "./rules";
import { BOOK } from "./testing/speciesBook";

const vessel = (rix: number, fields: Record<string, unknown>, type = "vessel_rule"): RuleView =>
  ruleView(rix, { entry_id: `w:r${rix}`, rule_id: "r1", type, family: "vessel", label: "L", verbatim: "", fields, prov: { rank: 4 } } as ExportRule,
    { kind: "vessel", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
const index = (rs: RuleView[]): RuleIndex => ({ get: (ix) => rs.find((r) => r.rix === ix)!, has: (ix) => rs.some((r) => r.rix === ix), size: rs.length });
const chips = (rs: RuleView[], timed: number[] = []) =>
  boatChips({ vessel: { active: rs.map((r) => r.rix).filter((i) => !timed.includes(i)), timed } } as unknown as GearAnswer, { rules: index(rs) });
const texts = (rs: RuleView[]) => chips(rs).map((c) => c.text);

describe("boats line", () => {
  it("every one of the six kinds, one example water each", () => {
    expect(texts([vessel(0, { aspect: "propulsion", level: "electric_only" })])).toEqual(["Electric motors only"]);   // Chicadee Lake
    expect(texts([vessel(0, { aspect: "propulsion", level: "unpowered" })])).toEqual(["No powered boats"]);            // Antler Lake
    expect(texts([vessel(0, { aspect: "propulsion", level: "power_capped", max_power_kw: 7.5 })])).toEqual(["Engine limit 7.5 kW (10 hp)"]);   // Beaver Lake
    expect(texts([vessel(0, { aspect: "speed", max_kmh: 8 })])).toEqual(["Speed limit 8 km/h"]);
    expect(texts([vessel(0, { aspect: "towing", undrawn_part: "on parts" })])).toEqual(["No towing (in part)"]);       // Beaver Lake R1
    expect(texts([vessel(0, { aspect: "propulsion", level: "none" })])).toEqual(["No vessels"]);                       // Campbell River
  });
  it("two limits of one kind read as a range on parts, never the stricter alone (Hatzic)", () => {
    expect(texts([vessel(0, { aspect: "speed", max_kmh: 8, undrawn_part: "on parts" }), vessel(1, { aspect: "speed", max_kmh: 50, undrawn_part: "on parts" })]))
      .toEqual(["Speed limits 8–50 km/h on parts"]);
  });
  it("a speed rule without a number says only that; one chip per kind in the fixed order, linked to the first rule", () => {
    const c = chips([vessel(0, { aspect: "towing" }), vessel(1, { aspect: "speed" }), vessel(2, { aspect: "propulsion", level: "unpowered" })]);
    expect(c.map((x) => [x.kind, x.text, x.rule])).toEqual([["unpowered", "No powered boats", "w:r2::r1"], ["speed", "Speed restriction", "w:r1::r1"], ["towing", "No towing", "w:r0::r1"]]);
  });
  it("a timed rule carries its dates", () => {
    const r = ruleView(0, { entry_id: "w:r0", rule_id: "r1", type: "vessel_rule", family: "vessel", label: "L", verbatim: "",
      fields: { aspect: "propulsion", level: "unpowered", when: { dates: [{ from_month: 5, from_day: 1, to_month: 9, to_day: 30 }] } }, prov: { rank: 4 } } as ExportRule,
      { kind: "vessel", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
    expect(chips([r], [0]).map((x) => x.text)).toEqual(["No powered boats · May 1–Sep 30"]);
  });
  it("no vessel rule, or a rule that is not a vessel_rule: no line", () => {
    expect(chips([])).toEqual([]);
    expect(boatKind(vessel(0, { aspect: "speed" }, "method_rule"))).toBeNull();
    expect(chips([vessel(0, { aspect: "speed" }, "method_rule")])).toEqual([]);
  });
});
