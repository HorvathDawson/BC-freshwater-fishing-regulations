/**
 * T30 unit cases over hand-built rules and gear answers (the golden-driven cases are app/conformance/src/regs/
 * gear.parity.test.ts): the level words, the chip words, the source line, the day line, closedUntil, changeVM, the
 * summary's part selection, the change routing and the rest-terms splice.
 */
import { describe, expect, it } from "vitest";
import type { ExportRule, GearAnswer, RuleFacts, RuleIndex, RuleView } from "./inputs";
import {
  changeVM, closedUntil, dayLine, downWater, gearLines, learnChipWords, levelTag, levelWord, sourceLine, whoWord, type GearCtx,
} from "./gear";
import { ruleView } from "./rules";
import { BOOK } from "./testing/speciesBook";

const exp = (entry_id: string, rank: number, entry_name?: string): ExportRule => ({ entry_id, rule_id: "r1", type: "", family: "", label: "L",
  verbatim: "", fields: {}, prov: { rank, ...(entry_name ? { entry_name } : {}) } });
const rule = (rix: number, entry_id: string, rank: number, entry_name?: string, book: [number, number][] = [], via: "reach" | "trib" = "reach"): RuleView =>
  ruleView(rix, exp(entry_id, rank, entry_name), { kind: "gear", title: "", book, applies: "" } as RuleFacts, BOOK, via);
const index = (rs: RuleView[]): RuleIndex => ({ get: (ix) => rs.find((r) => r.rix === ix)!, has: (ix) => rs.some((r) => r.rix === ix), size: rs.length });

describe("levelWord / levelTag / downWater (page v36 levelTag + openRule's sub, prototype levelOf)", () => {
  it("names each rank as the rule sheet does", () => {
    expect([
      levelWord(rule(0, "zp:gear", 4, "Everywhere in B.C.")), levelWord(rule(1, "z2:streams", 3, "Region 2")),
      levelWord(rule(2, "zx:area", 3, "Some area")), levelWord(rule(3, "na:x", 2, "Kootenay Lake area")),
      levelWord(rule(4, "w:duncan", 1, "Duncan River")), levelWord(rule(5, "fed:x", -1, "Federal")), levelWord(rule(6, "w:vedder", 0, "Vedder River")),
    ]).toEqual(["B.C.", "Region 2", "Region", "Named area", "Downstream", "Federal or park", "This water’s"]);
  });
  it("reads the part's rank: a tributary's view of a water rule is Downstream", () => {
    expect(levelTag(rule(0, "w:x", 0, "X River", [], "trib"))).toBe("Downstream rule");
  });
  it("with no entry name the sub is the tag alone (and levelWord keeps ' rule', as the prototype's split does)", () => {
    expect(levelWord(rule(0, "zp:gear", 4))).toBe("B.C. rule");
  });
  it("a downstream rule is named by its water; any other rank by nothing", () => {
    expect([downWater(rule(0, "w:d", 1, "Duncan River")), downWater(rule(1, "w:d", 0, "Duncan River")), downWater(rule(2, "w:d", 1))]).toEqual(["Duncan River", "", ""]);
  });
});

const ga = (o: Partial<GearAnswer> = {}): GearAnswer => ({
  summary: { says: "", parts: [], terms: [], cites: [], changes: [] }, counts: {}, specs: {}, elements: {}, main: [], circumstantial: [],
  hook: "any", fly: null, bait: [], bait_ban: false, ways: [], conduct: {}, vessel: { active: [], timed: [] }, timed: [], in_part: [], side: [],
  while_rules: [], caught: [], overruled: [], decides: [], repeats: [], ...o,
});
const count = (rix: number) => ({ by: [rix, 0] as [number, number], over: [], cites: [rix] });

describe("learnChipWords / whoWord (prototype learnChipWords, wordOf_)", () => {
  const prov = rule(0, "zp:gear", 4, "Everywhere in B.C."), reg = rule(1, "z2:s", 3, "Region 2"), own = rule(2, "w:v", 0, "Vedder River"),
    down = rule(3, "w:d", 1, "Duncan River"), named = rule(4, "na:k", 2, "Kootenay");
  const rules = index([prov, reg, own, down, named]);
  it("learns the chip of each counted slot and banned element; never Region", () => {
    const g = ga({ counts: { lines_per_angler: count(0), terminal_attachments_per_line: count(1), weight_per_line_kg: count(2) },
      elements: { "barb:barbed": { verdict: "ban", by: [4, 0], over: [] }, "lure:artificial_lure": { verdict: "allow", by: [3, 0], over: [] } } });
    expect([...learnChipWords(g, rules)]).toEqual([["B.C.", "Province"], ["Named area", "Named area"], ["This water’s", "This water"]]);
  });
  it("flies_per_line chips only without a hook-lure-or-fly count", () => {
    expect([...learnChipWords(ga({ counts: { flies_per_line: count(2) } }), rules)]).toEqual([["This water’s", "This water"]]);
    expect([...learnChipWords(ga({ counts: { flies_per_line: count(2), terminal_attachments_per_line: count(0) } }), rules)]).toEqual([["B.C.", "Province"]]);
  });
  it("who: the downstream water, else the learned chip, else the level", () => {
    const chips = new Map([["B.C.", "Province"]]);
    expect([whoWord(prov, chips), whoWord(down, chips), whoWord(reg, chips), whoWord(own, chips)]).toEqual(["Province", "Duncan River", "Region 2", "This water’s"]);
  });
});

describe("sourceLine (prototype itemSrc)", () => {
  const rules = index([rule(0, "zp:g", 4, "B.C.", [[8, 10]]), rule(1, "z2:s", 3, "Region 2", [[22, 24], [8, 11]]), rule(2, "w:v", 0, "V", [[21, 23]])]);
  const ctx = { rules, chips: new Map([["B.C.", "Province"]]) };
  it("who once each in order, the pages each printed page once (its first pdf), ascending; unknown rules dropped", () => {
    expect(sourceLine("srcs:gl|hook", [0, 1, 0, 9, 2], ctx)).toEqual({
      who: "Province · Region 2 · This water’s ›", aria: "Sources: Province, Region 2, This water’s", open: "srcs:gl|hook",
      keys: ["zp:g::r1", "z2:s::r1", "w:v::r1"], pages: [{ printed: 8, pdf: 10 }, { printed: 21, pdf: 23 }, { printed: 22, pdf: 24 }],
    });
  });
  it("no known rule: no line", () => {
    expect(sourceLine("srcs:bait", [7, 9], ctx)).toBeNull();
  });
});

describe("dayLine, closedUntil, changeVM", () => {
  it("Today, or the weekday", () => {
    expect([dayLine({ day: { month: 10, day: 10 }, weekday: "Saturday", today: true }), dayLine({ day: { month: 5, day: 15 }, weekday: "Friday", today: false })])
      .toEqual(["Today, Oct 10", "Fri, May 15"]);
  });
  it("the first open day after today, wrapping; closed every day: never", () => {
    const bits = Array.from({ length: 365 }, (_, i) => (i >= 151 && i < 181 ? 1 : 0));   // closed Jun 1–30
    expect(closedUntil(bits, 165)).toBe("No fishing here until Jul 1.");
    const wrap = Array.from({ length: 365 }, (_, i) => (i < 10 ? 0 : 1));
    expect(closedUntil(wrap, 200)).toBe("No fishing here until Jan 1.");
    expect(closedUntil(Array(365).fill(1), 0)).toBe("No fishing here on any day of the year.");
    // the walk starts the day AFTER today (prototype j = 1): an open today is never the answer
    const after = Array.from({ length: 365 }, (_, i) => (i === 100 || i >= 120 ? 0 : 1));
    expect(closedUntil(after, 100)).toBe("No fishing here until May 1.");
    expect(closedUntil(Array(365).fill(null), 0)).toBe("No fishing here on any day of the year.");
  });
  it("Next, or Now while in force", () => {
    expect([changeVM("May 1–31", "bait ban", false), changeVM("All year", "bait ban", true)]).toEqual([
      { chip: "Next", when: "May 1–31", says: "bait ban", inForce: false }, { chip: "Now", when: "All year", says: "bait ban", inForce: true }]);
  });
});

describe("gearLines (prototype buildGear summary half)", () => {
  const rules = index([rule(0, "zp:g", 4, "B.C.", [[8, 10]]), rule(1, "w:v", 0, "V", [[22, 24]])]);
  const ctx = (): GearCtx => ({ day: { month: 10, day: 10 }, weekday: "Saturday", today: true, rules, glossary: new Set(["single_barbless_hook", "fly_fishing"]),
    chips: new Map([["B.C.", "Province"]]), banner: null });
  const part = (p: "mode" | "lines" | "hook" | "attach" | "weight", says: string, cites: number[] = [0], terms: string[] = []) => ({ part: p, says, cites, terms });
  const sum = (says: string, parts: ReturnType<typeof part>[], o: Partial<GearAnswer["summary"]> = {}) => ga({ summary: { says, parts, terms: [], cites: [], changes: [], ...o } });

  it("draws only the parts the sentence names; drops the article and a repeated label; Hooks = attach then hook", () => {
    const g = sum("1 line, one hook, lure or fly, a single barbless hook and weight up to 1 kg", [
      part("lines", "1 line"), part("hook", "a single barbless hook", [1], ["single_barbless_hook", "not_in_glossary"]), part("attach", "one hook, lure or fly"),
      part("weight", "weight up to 1 kg"), part("weight", "a float"),
    ]);
    const v = gearLines(g, ctx());
    expect(v.mode).toBeNull();
    expect(v.lines.map((l) => [l.k, l.label, l.values, l.valueTerms])).toEqual([
      ["lines", "Lines", ["1 line"], [[]]],
      ["hook", "Hooks", ["One hook, lure or fly", "Single barbless hook"], [[], ["single_barbless_hook"]]],
      ["weight", "Weight on the line", ["Weight up to 1 kg"], [[]]],
    ]);
    expect(v.lines[1]!.src?.who).toBe("Province · This water’s ›");
  });

  it("routes each change to the line it talks about; with a mode, to the mode", () => {
    const parts = [part("lines", "1 line"), part("hook", "one hook"), part("weight", "1 kg")];
    const ch = (says: string) => ({ when: "May 1–31", says, cites: [1] });
    const routed = (changes: ReturnType<typeof ch>[], ps = parts, says = "1 line one hook 1 kg fly fishing only") =>
      gearLines(sum(says, ps, { changes }), ctx());
    const at = (v: ReturnType<typeof gearLines>) => v.lines.map((l) => l.changes.map((c) => c.says));
    expect(at(routed([ch("artificial fly only"), ch("no sinker"), ch("2 rods"), ch("something else")])))
      .toEqual([["2 rods"], ["artificial fly only", "something else"], ["no sinker"]]);
    expect(at(routed([ch("no sinker")], [part("lines", "1 line"), part("hook", "one hook")]))).toEqual([[], ["no sinker"]]);   // weight not drawn → last drawn
    const m = routed([ch("one hook")], [part("mode", "Fly fishing only", [1], ["fly_fishing"]), ...parts]);
    expect([m.mode?.value, m.mode?.terms, m.mode?.changes.map((c) => c.chip), m.mode?.src?.who, at(m)])
      .toEqual(["Fly fishing only", ["fly_fishing"], ["Next"], "This water’s ›", [[], [], []]]);
  });

  it("the summary's unused terms go on the last line only when it has a source line and no change", () => {
    const parts = [part("lines", "1 line"), part("weight", "1 kg")];
    const v = gearLines(sum("1 line 1 kg", parts, { terms: ["fly_fishing"] }), ctx());
    expect(v.lines.map((l) => l.valueTerms)).toEqual([[[]], [["fly_fishing"]]]);
    const withChange = gearLines(sum("1 line 1 kg", parts, { terms: ["fly_fishing"], changes: [{ when: "May 1–31", says: "no sinker", cites: [0] }] }), ctx());
    expect(withChange.lines.map((l) => l.terms)).toEqual([[], []]);
    const noSrc = gearLines(sum("1 line 1 kg", [part("lines", "1 line"), part("weight", "1 kg", [])], { terms: ["fly_fishing"] }), ctx());
    expect(noSrc.lines.map((l) => l.terms)).toEqual([[], []]);
  });
});
