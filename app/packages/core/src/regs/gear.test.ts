/**
 * T30 unit cases over hand-built rules and gear answers (the golden-driven cases are app/conformance/src/regs/
 * gear.parity.test.ts (retired, T439)): the level words, the chip words, the source line, the day line, closedUntil, changeVM, the
 * summary's part selection, the change routing and the rest-terms splice.
 */
import { describe, expect, it } from "vitest";
import type { ExportRule, GearAnswer, RuleFacts, RuleIndex, RuleView } from "./inputs";
import {
  changeVM, closedUntil, dayLine, downWater, gearLines, hooksAllowed, learnChipWords, levelWord, sourceLine, whoWord, type GearCtx,
} from "./gear";
import { levelTag } from "./level";
import { ruleView } from "./rules";
import { BOOK } from "./testing/speciesBook";
import { setFailureSink } from "../failure";

/** `prov.region` as the store ships it (T109: the pipeline's `entry_id.region_of`, stated here, never parsed by the app) */
const SHIPPED_REGION: Record<string, string> = { "z2:streams": "2", "z2:s": "2" };
const exp = (entry_id: string, rank: number, entry_name?: string): ExportRule => ({ entry_id, rule_id: "r1", type: "", family: "", label: "L",
  verbatim: "", fields: {}, prov: { rank, ...(entry_name ? { entry_name } : {}), region: SHIPPED_REGION[entry_id] ?? null } });
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
  // (T1051) a single barbless hook (every stream's): the Hooks row adds no "… hooks allowed" line
  hook: "single_barbless", fly: null, bait: [], bait_ban: false, ways: [], conduct: {}, vessel: { active: [], timed: [] }, timed: [], in_part: [], side: [],
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
  it("(T215) a page carries its rules' boxes on that PDF page, once each; a page with none carries no box", () => {
    // Cultus Lake, r-4df5bccc9360: "No vessels" / speed rules → the boats source line; its p.23 link opened 0 boxes
    const box = { pdf: 25, box: [35.75, 146.4, 539.75, 166.56] as [number, number, number, number], size: [576, 756] as [number, number] };
    const withBox = (rix: number, book: [number, number][], bb: unknown[]): RuleView =>
      ruleView(rix, exp("w:c", 0, "Cultus Lake"), { kind: "vessel", title: "", book, applies: "", book_box: bb } as RuleFacts, BOOK, "reach");
    const rs = index([withBox(0, [[23, 25]], [box]), withBox(1, [[23, 25]], [box]), rule(2, "z2:s", 3, "Region 2", [[21, 23]])]);
    expect(sourceLine("srcs:boats", [0, 1, 2], { rules: rs, chips: new Map() })!.pages)
      .toEqual([{ printed: 21, pdf: 23 }, { printed: 23, pdf: 25, box: [box] }]);
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

describe("(T1051) the Hooks row's second line: what the hook answer leaves allowed", () => {
  // Dick Lake wbk:328989582 (Region 2, no row of its own) and Dick Creek gnis:23637, r-0aec88ed3edb, Nov 24: the frames'
  // summaries as the release holds them (rule indexes made up)
  const rules = index([rule(0, "zp:g", 4, "B.C.", [[8, 10]]), rule(1, "z2:s", 3, "Region 2", [[21, 23]])]);
  const ctx = (): GearCtx => ({ day: { month: 11, day: 24 }, weekday: "Monday", today: true, rules,
    glossary: new Set(["single_barbless_hook", "def_single_hook", "treble_and_barbed_hooks"]), chips: new Map([["B.C.", "Province"]]), banner: null });
  const p = (part: "lines" | "hook" | "attach" | "weight", says: string, cites: number[] = [0], terms: string[] = []) => ({ part, says, cites, terms });
  const lake = ga({ hook: "trebles_and_barbs", summary: { says: "1 line (2 while alone in a boat) with one hook, lure or fly. Weight on the line up to 1 kg (2.2 lb).",
    parts: [p("lines", "1 line (2 while alone in a boat)"), p("hook", "any hook", []), p("attach", "one hook, lure or fly"), p("weight", "weight on the line up to 1 kg (2.2 lb)")],
    terms: [], cites: [0], changes: [] } });
  const creek = ga({ hook: "single_barbless", summary: { says: "1 line with a single barbless hook, no trebles (one hook, lure or fly). Weight on the line up to 1 kg (2.2 lb).",
    parts: [p("lines", "1 line"), p("hook", "a single barbless hook, no trebles", [1], ["single_barbless_hook"]), p("attach", "one hook, lure or fly"),
      p("weight", "weight on the line up to 1 kg (2.2 lb)")], terms: ["single_barbless_hook"], cites: [0, 1], changes: [] } });
  const hooks = (g: GearAnswer) => gearLines(g, ctx()).lines.filter((l) => l.k === "hook").map((l) => [l.values, l.valueTerms]);

  it("Dick Lake: a second Hooks line, Treble and barbed hooks allowed, its one ⓘ the Treble and barbed hooks term", () => {
    expect(hooks(lake)).toEqual([[["One hook, lure or fly", "Treble and barbed hooks allowed"], [[], ["treble_and_barbed_hooks"]]]]);
  });
  it("Dick Creek (a stream: single barbless hook) is unchanged", () => {
    expect(hooks(creek)).toEqual([[["One hook, lure or fly", "Single barbless hook, no trebles"], [[], ["single_barbless_hook"]]]]);
  });
  it("says only what is true, per hook answer and fly mode", () => {
    const says = (hook: GearAnswer["hook"], fly: GearAnswer["fly"] = null, counts: GearAnswer["counts"] = {}) => hooksAllowed(ga({ hook, fly, counts }), rules)?.says ?? null;
    expect([says("trebles_and_barbs"), says("any_barbless"), says("single"), says("single_barbless"), says("any")])
      .toEqual(["Treble and barbed hooks allowed", "Treble hooks allowed", "Barbed hooks allowed", null, "Barbed hooks allowed"]);
    // a fly is a single-pointed hook (p.80): a fly-only water never says treble hooks
    expect([says("trebles_and_barbs", "artificial_fly_only"), says("trebles_and_barbs", "fly_fishing_only"), says("any_barbless", "artificial_fly_only"),
      says("single", "fly_fishing_only")]).toEqual(["Barbed hooks allowed", "Barbed hooks allowed", null, "Barbed hooks allowed"]);
    // a hook-point count in force: no treble hooks, whatever the tag
    expect(says("any_barbless", null, { points_per_hook: count(0) })).toBeNull();
  });
  it("an ⓘ the glossary lacks is not drawn; with no Hooks row yet the line makes one, before Weight", () => {
    const bare = gearLines(lake, { ...ctx(), glossary: new Set() }).lines.find((l) => l.k === "hook")!;
    expect(bare.valueTerms).toEqual([[], []]);
    const noHooks = ga({ hook: "trebles_and_barbs", summary: { says: "1 line. Weight on the line up to 1 kg.", parts: [p("lines", "1 line"), p("weight", "weight on the line up to 1 kg")],
      terms: [], cites: [0], changes: [] } });
    expect(gearLines(noHooks, ctx()).lines.map((l) => [l.k, l.values])).toEqual([["lines", ["1 line"]], ["hook", ["Treble and barbed hooks allowed"]], ["weight", ["Up to 1 kg"]]]);
  });
});

describe("(T1051 review) no unqualified line where a part, a side, some hours or a circumstance holds a hook rule", () => {
  // r-0aec88ed3edb: François Lake (map 1 outlet area), Mahood Lake (Mahood River outlet; western tip; Jul 1-Dec 31), Upper
  // Arrow Lake (north-end drawdown area) and Pitt Lake (north of the head-of-lake signs) each hold "Single barbless hook — in
  // part: …" in `in_part`; their whole-water hook answer is trebles_and_barbs (rule indexes made up)
  const sbh = (rix: number, entry: string, name: string): RuleView => ruleView(rix,
    { ...exp(entry, 0, name), fields: { gear: [{ slot: "barb", only: ["barbless"] }, { slot: "points_per_hook", max: 1 }] } },
    { kind: "gear", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
  const other = (rix: number): RuleView => ruleView(rix, { ...exp("w:x", 0, "X"), fields: { gear: [{ slot: "weight_per_line_kg", max: 1 }] } },
    { kind: "gear", title: "", book: [], applies: "" } as RuleFacts, BOOK, "reach");
  const lakes = [["w:francois_lake", "François Lake"], ["w:mahood_lake", "Mahood Lake"], ["w:upper_arrow_lake", "Upper Arrow Lake"], ["w:pitt_lake", "Pitt Lake"]] as const;
  const rules = index([...lakes.map(([e, n], i) => sbh(i, e, n)), other(9)]);
  const says = (o: Partial<GearAnswer>) => hooksAllowed(ga({ hook: "trebles_and_barbs", ...o }), rules)?.says ?? null;

  it("François, Mahood, Upper Arrow and Pitt lakes: a single barbless hook in part, so no allowed line", () => {
    expect(lakes.map((_, i) => says({ in_part: [i] }))).toEqual([null, null, null, null]);
    // a part rule that says nothing of hooks leaves the line
    expect(says({ in_part: [9] })).toBe("Treble and barbed hooks allowed");
  });
  it("a hook rule on one side, some hours, or in a circumstance: no line either", () => {
    expect([says({ side: [0] }), says({ timed: [1] }), says({ circumstantial: [{ clause: [2, 0], targeting: ["SALMON"] }] })]).toEqual([null, null, null]);
  });
  it("an unknown hook answer draws no line and is reported (W46)", () => {
    const got: unknown[] = [];
    const undo = setFailureSink((e) => got.push(e));
    try {
      expect(hooksAllowed(ga({ hook: "double" as GearAnswer["hook"] }), rules)).toBeNull();
    } finally { undo(); }
    expect(got.map((e) => (e as Error).name)).toEqual(["UnknownHookAnswer"]);
  });
});
