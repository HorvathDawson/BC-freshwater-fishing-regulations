/**
 * T209 golden cases: one example water per kind of vessel restriction (the ticket's own examples), the two-limits reading,
 * and a water with no vessel rule (no line).
 */
import { describe, expect, it } from "vitest";
import { boatChips, boatKind, inBoatRules, inPartVessels } from "./boats";
import type { ExportRule, GearAnswer, RuleFacts, RuleIndex, RuleView } from "./inputs";
import { ruleView } from "./rules";
import { BOOK } from "./testing/speciesBook";

const vessel = (rix: number, fields: Record<string, unknown>, type = "vessel_rule"): RuleView =>
  ruleView(rix, { entry_id: `w:r${rix}`, rule_id: "r1", type, family: "vessel", label: "L", verbatim: "", fields, prov: { rank: 4 } } as ExportRule,
    { kind: "vessel", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
const index = (rs: RuleView[]): RuleIndex => ({ get: (ix) => rs.find((r) => r.rix === ix)!, has: (ix) => rs.some((r) => r.rix === ix), size: rs.length });
const chips = (rs: RuleView[], timed: number[] = [], inPart: number[] = []) =>
  boatChips({ circumstantial: [], in_part: inPart,
    vessel: { active: rs.map((r) => r.rix).filter((i) => !timed.includes(i) && !inPart.includes(i)), timed } } as unknown as GearAnswer, { rules: index(rs) });
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
  it("(T623 F15) a rule printed 'on parts' that names none keeps the words on its chip (Elk Lake R1 p.15, Chilko 'on part')", () => {
    const onParts = (rix: number, fields: Record<string, unknown>, words: string) =>
      ruleView(rix, { entry_id: `w:r${rix}`, rule_id: "r1", type: "vessel_rule", family: "vessel", label: "L", verbatim: "",
        fields, parts: { what: "W", on_parts: words }, prov: { rank: 0 } } as ExportRule,
        { kind: "vessel", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
    expect(texts([onParts(0, { aspect: "propulsion", level: "none" }, "on parts"),
                  onParts(1, { aspect: "propulsion", level: "unpowered" }, "on parts"),
                  onParts(2, { aspect: "propulsion", level: "power_capped", max_power_kw: 7.5 }, "on parts"),
                  onParts(3, { aspect: "towing" }, "on parts")]))
      .toEqual(["No vessels on parts", "No powered boats on parts", "Engine limit 7.5 kW (10 hp) on parts", "No towing on parts"]);
    expect(texts([onParts(0, { aspect: "speed", max_kmh: 5 }, "on part")])).toEqual(["Speed limit 5 km/h on part"]);
    // two speeds already say "on parts": never twice (Hatzic)
    expect(texts([onParts(0, { aspect: "speed", max_kmh: 8 }, "on parts"), onParts(1, { aspect: "speed", max_kmh: 50 }, "on parts")]))
      .toEqual(["Speed limits 8–50 km/h on parts"]);
    // one rule of a kind without the words: the chip cannot say all of them hold on parts
    expect(texts([onParts(0, { aspect: "towing" }, "on parts"), vessel(1, { aspect: "towing" })])).toEqual(["No towing"]);
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

  it("(T670 G1) a rule held for an angler in a boat is a chip in its stored label, before the vessel kinds, each rule once (Kispiox p.51)", () => {
    const rule = (rix: number, label: string): RuleView =>
      ruleView(rix, { entry_id: `w:r${rix}`, rule_id: "r1", type: "method_rule", family: "gear_and_method", label, verbatim: "No angling from boats",
        fields: { gear: [{ ban: ["angling"], slot: "method", when: { angler: "in_boat" } }] }, prov: { rank: 0 } } as ExportRule,
        { kind: "gear", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
    const rs = [rule(7, "No angling (from a boat), Aug 15-Dec 31"), rule(8, "No spear fishing"), vessel(9, { aspect: "propulsion", level: "unpowered" })];
    const ga = { circumstantial: [{ clause: [7, 0], while: ["in_boat"] }, { clause: [7, 1], while: ["in_boat"] }, { clause: [8, 0], targeting: ["BB"] },
      { clause: [8, 1], while: ["ice_fishing"] }], vessel: { active: [9], timed: [] }, in_part: [] } as unknown as GearAnswer;
    expect(boatChips(ga, { rules: index(rs) })).toEqual([
      { kind: "in_boat", text: "No angling (from a boat), Aug 15-Dec 31", rule: "w:r7::r1" },
      { kind: "unpowered", text: "No powered boats", rule: "w:r9::r1" }]);
    expect(inBoatRules(ga, { rules: index(rs) }).map((r) => r.rix)).toEqual([7]);
    // mutation: the circumstance dropped from the frame, the chip is gone (the presenter reads the frame, never the rule)
    expect(boatChips({ ...ga, circumstantial: [] } as GearAnswer, { rules: index(rs) }).map((c) => c.kind)).toEqual(["unpowered"]);
  });
  it("(T209, owner 2026-11-12) a vessel rule of a part no map draws is in the vessel section with the book's locator words", () => {
    // the ten 'close wording' rules (on-parts.md): the gear frame lists them in `in_part`, never in `vessel`
    const at = (rix: number, fields: Record<string, unknown>, part: string) => vessel(rix, { ...fields, undrawn_part: part });
    const various = "various locations (as buoyed and signed)";
    // Okanagan / Kalamalka / Skaha: "Speed restrictions or no vessels (as buoyed and signed), various locations"
    expect(chips([at(0, { aspect: "speed" }, various), at(1, { aspect: "propulsion", level: "none" }, various)], [], [0, 1]).map((c) => c.text))
      .toEqual(["No vessels — various locations (as buoyed and signed)", "Speed restriction — various locations (as buoyed and signed)"]);
    // Alouette, Osoyoos, the Columbia's wetlands, the Harrison's bays
    expect(chips([at(0, { aspect: "propulsion", level: "none" }, "in swimming areas, as buoyed and signed")], [], [0]).map((c) => c.text))
      .toEqual(["No vessels — in swimming areas, as buoyed and signed"]);
    expect(chips([at(0, { aspect: "propulsion", level: "none" }, "in 5 signed swimming areas")], [], [0]).map((c) => c.text))
      .toEqual(["No vessels — in 5 signed swimming areas"]);
    expect(chips([at(0, { aspect: "propulsion", level: "unpowered" }, "in wetlands")], [], [0]).map((c) => c.text))
      .toEqual(["No powered boats — in wetlands"]);
    expect(chips([at(0, { aspect: "speed" }, "in small bays along the river as signed")], [], [0]).map((c) => c.text))
      .toEqual(["Speed restriction — in small bays along the river as signed"]);
    // beside a whole-water rule of the same kind: both, the whole water first, never merged into one
    expect(chips([vessel(0, { aspect: "speed", max_kmh: 10 }), at(1, { aspect: "speed" }, various)], [], [1]).map((c) => [c.text, c.rule]))
      .toEqual([["Speed limit 10 km/h", "w:r0::r1"], ["Speed restriction — various locations (as buoyed and signed)", "w:r1::r1"]]);
    // only vessel rules with a part: an in-part gear rule stays the caveat's
    const gear = ruleView(2, { entry_id: "w:r2", rule_id: "r1", type: "gear_restriction", family: "gear_and_method", label: "Bait ban", verbatim: "",
      fields: { undrawn_part: "within signs" }, prov: { rank: 4 } } as ExportRule, { kind: "gear", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
    const rs = [at(0, { aspect: "towing" }, various), gear];
    expect(inPartVessels({ in_part: [0, 2] } as unknown as GearAnswer, { rules: index(rs) }).map((r) => r.rix)).toEqual([0]);
  });
  it("(T209 follow-up, owner 2026-11-12: \"Yes, book's full words\") one printed line of alternatives is one chip, in the book's order", () => {
    // one rule per kind, both from one row and one printed line, as the catalogue holds them (harrison_river.r3 / r4)
    const line = (rix: number, entry: string, verbatim: string, fields: Record<string, unknown>, part: string) =>
      ruleView(rix, { entry_id: entry, rule_id: `r${rix}`, type: "vessel_rule", family: "vessel", label: "L", verbatim,
        fields: { ...fields, undrawn_part: part }, prov: { rank: 4 } } as ExportRule,
        { kind: "vessel", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
    const harrison = "Speed restriction or no powered boats in small bays along the river as signed";
    const bays = "in small bays along the river as signed";
    expect(chips([line(0, "r2:harrison", harrison, { aspect: "speed" }, bays),
                  line(1, "r2:harrison", harrison, { aspect: "propulsion", level: "unpowered" }, bays)], [], [0, 1])
      .map((c) => [c.text, c.kind, c.rule]))
      .toEqual([["Speed restriction or no powered boats — in small bays along the river as signed", "speed", "r2:harrison::r0"]]);
    const okanagan = "Speed restrictions or no vessels (as buoyed and signed), various locations";
    const various = "various locations (as buoyed and signed)";
    expect(chips([line(4, "r8:okanagan", okanagan, { aspect: "speed" }, various),
                  line(5, "r8:okanagan", okanagan, { aspect: "propulsion", level: "none" }, various)], [], [4, 5]).map((c) => c.text))
      .toEqual(["Speed restriction or no vessels — various locations (as buoyed and signed)"]);
    // two lines of one row on one part (Premier Lake's "No towing, speed restriction (15 km/h), south half only"): two chips
    expect(chips([line(0, "r4:premier", "No towing", { aspect: "towing" }, "south half"),
                  line(1, "r4:premier", "speed restriction (15 km/h)", { aspect: "speed", max_kmh: 15 }, "south half")], [], [0, 1])
      .map((c) => c.text))
      .toEqual(["Speed limit 15 km/h — south half", "No towing — south half"]);
  });
});
