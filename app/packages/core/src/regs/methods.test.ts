/**
 * T32 unit cases over hand-built rules and gear answers (the golden-driven cases are app/conformance/src/regs/
 * methods.parity.test.ts): countTxt, tgtTxt, the way chips (limited way, clauses, conduct, crayfish rule, devices), the ways
 * list and its source order, boats, the reference fold.
 */
import { describe, expect, it } from "vitest";
import type { GearCtx } from "./gear";
import type { ExportRule, GearAnswer, RegsInputs, RuleFacts, RuleIndex, RuleView } from "./inputs";
import { boatsVM, countTxt, referenceVM, tgtTxt, wayChips, waysVM } from "./methods";
import { ruleView } from "./rules";
import { BOOK } from "./testing/speciesBook";

const rule = (rix: number, o: { label?: string; gear?: unknown[]; rank?: number; book?: [number, number][]; plain?: string } = {}): RuleView =>
  ruleView(rix, { entry_id: `w:r${rix}`, rule_id: "r1", type: "", family: "", label: o.label ?? "L", verbatim: "", fields: o.gear ? { gear: o.gear } : {},
    prov: { rank: o.rank ?? 4, entry_name: "Somewhere" } } as ExportRule,
  { kind: "gear", title: "", book: o.book ?? [[8, 10]], applies: "", ...(o.plain ? { plain: o.plain } : {}) } as RuleFacts, BOOK, "reach");
const index = (rs: RuleView[]): RuleIndex => ({ get: (ix) => rs.find((r) => r.rix === ix)!, has: (ix) => rs.some((r) => r.rix === ix), size: rs.length });
const ga = (o: Partial<GearAnswer> = {}): GearAnswer => ({
  summary: { says: "", parts: [], terms: [], cites: [], changes: [] }, counts: {}, specs: {}, elements: {}, main: [], circumstantial: [],
  hook: "any", fly: null, bait: [], bait_ban: false, ways: [], conduct: {}, vessel: { active: [], timed: [] }, timed: [], in_part: [], side: [],
  while_rules: [], caught: [], overruled: [], decides: [], repeats: [], ...o,
});
const ctxOf = (rules: RuleIndex): GearCtx => ({ day: { month: 10, day: 10 }, weekday: "Saturday", today: true, rules, glossary: new Set(),
  chips: new Map([["B.C.", "Province"]]), banner: null });

describe("countTxt (page v36 countTxt)", () => {
  it("says each slot's count as page v36 does", () => {
    expect([
      countTxt("points_per_hook", { max: 1 }), countTxt("points_per_hook", { max: 3 }), countTxt("weight_per_line_kg", { max: 1 }),
      countTxt("bait_possession_kg", { min: 2 }), countTxt("light_to_hook_mm", { max: 1000 }), countTxt("lines_per_angler", { unlimited: true }),
      countTxt("hook_gap_mm", { min: 19 }), countTxt("lines_per_angler", { max: 2 }), countTxt("lines_per_angler", null),
    ]).toEqual(["Single point only (no trebles)", "3 points", "up to 1 kg", "at least 2 kg", "within 1 m of the hook", "no limit",
      "at least 1.9 cm", "2", "(clause missing)"]);
  });
});

describe("tgtTxt (page v36 tgtTxt)", () => {
  it("a group word, else the fish names; two or more read 'a, b or c'", () => {
    expect([tgtTxt(["ALL_GAME_FISH"], BOOK), tgtTxt(["BASS"], BOOK), tgtTxt(["ALL_GAME_FISH", "SALMON", "PROTECTED_SPECIES"], BOOK), tgtTxt(["KO", "BASS"], BOOK)])
      .toEqual(["game fish", "bass", "game fish, salmon or protected species", "kokanee or bass"]);
  });
});

describe("wayChips + waysVM (page v36 gearParts ways, prototype WAYS)", () => {
  const rules = index([
    rule(0, { book: [[8, 10]] }), rule(1, { gear: [{ slot: "lines_per_angler", max: 1 }, { must_be: ["quick_release_to_line", "tied_on"] }], rank: 0, book: [[22, 24]] }),
    rule(2, { label: "Release any fish other than crayfish **at once**" }), rule(3, { label: "Set  lines *only* in winter" }),
  ]);
  const ctx = { ...ctxOf(rules), species: BOOK, exportConduct: { remove_hut: "Remove your ice hut before breakup" } };
  const g = ga({ ways: [
    { method: "spear_fishing", allowed: true, by: [0, 0], not_for: ["ALL_GAME_FISH", "SALMON", "PROTECTED_SPECIES"], for: ["KO"] },
    { method: "ice_fishing", allowed: true, by: [1, 0], while: [{ clause: [1, 0] }, { clause: [1, 1] }, { clause: [1, 9] }], conduct: ["remove_hut", "remove_hut", "other"] },
    { method: "crayfish_trapping", allowed: true, by: [0, 0], while_rules: [2, 3] },
    { method: "angling", allowed: true, by: [0, 0], devices: { light: { must_be: ["submerged"], by: [], within_mm: 1000 }, downrigger: { must_be: ["quick_release_to_line"], by: [] } } },
    { method: "netting", allowed: false, by: [1, 0], why: "not_lawful" },
    { method: "angling", allowed: true, by: null, why: "fly_fishing_only" },
    { method: "snagging", allowed: false, by: null },
  ] });
  it("the chips: the limited way, clauses, conduct (each once), the crayfish rule, devices in the frame's order", () => {
    expect(wayChips(g, ctx)).toEqual([
      { method: "spear_fishing", allow: true, rix: 0, info: "not for game fish, salmon or protected species · kokanee allowed" },
      { method: "ice_fishing", allow: true, rix: 1, info: "lines: 1 · must be attached to your line by a quick-release, tied on · Remove your ice hut before breakup · other" },
      { method: "crayfish_trapping", allow: true, rix: 0, info: "release anything in the trap that isn’t a crayfish · Set lines only in winter" },
      { method: "angling", allow: true, rix: 0, info: "with a light: submerged, within 1 m of the hook · with a downrigger: attached to your line by a quick-release" },
      { method: "netting", allow: false, rix: 1, info: "not a lawful way to sport fish" },
      { method: "angling", allow: true, rix: null, info: "fly fishing only: nothing but the fly on the line, no float, sinker or attractor" },
      { method: "snagging", allow: false, rix: null, info: "" },
    ]);
  });
  it("the list: ✓ ways (a limited way's name says so) then ✕ ways without their why; sources ✓ first, then ✕", () => {
    const v = waysVM(wayChips(g, ctx), ctx)!;
    expect(v.label).toBe("Ways to fish");
    expect(v.list.map((c) => [c.col, c.items.map((i) => `${i.mark} ${i.name}${i.note ? ` | ${i.note}` : ""}`)])).toEqual([
      ["ok", ["✓ Spear or bow — not for trout or other game fish | not for game fish, salmon or protected species · kokanee allowed",
        "✓ Ice fishing | lines: 1 · must be attached to your line by a quick-release, tied on · Remove your ice hut before breakup · other",
        "✓ Crayfish traps | release anything in the trap that isn’t a crayfish · Set lines only in winter",
        "✓ Angling (rod and line) | with a light: submerged, within 1 m of the hook · with a downrigger: attached to your line by a quick-release",
        "✓ Angling (rod and line) | fly fishing only: nothing but the fly on the line, no float, sinker or attractor"]],
      ["no", ["✕ Nets", "✕ Snagging"]],
    ]);
    expect([v.src?.who, v.src?.open, v.src?.keys, v.src?.pages]).toEqual(["Province · This water’s ›", "srcs:ways", ["w:r0::r1", "w:r1::r1"],
      [{ printed: 8, pdf: 10 }, { printed: 22, pdf: 24 }]]);
  });
});

describe("(T35, owner wave A) the set_line ⓘ on an allowed Set lines way", () => {
  const rules = index([rule(0)]);
  const g = ga({ ways: [{ method: "set_lining", allowed: true, by: [0, 0] }, { method: "netting", allowed: true, by: [0, 0] },
    { method: "set_lining", allowed: false, by: [0, 0] }] });
  const terms = (glossary: string[]) => {
    const ctx = { ...ctxOf(rules), glossary: new Set(glossary), species: BOOK, exportConduct: {} };
    return waysVM(wayChips(g, ctx), ctx)!.list.map((c) => c.items.map((i) => [i.mark, i.name, i.terms]));
  };
  it("the allowed Set lines item carries the glossary's set_line id; a not-allowed one and other ways carry none", () => {
    expect(terms(["set_line"])).toEqual([[["✓", "Set lines", ["set_line"]], ["✓", "Nets", []]], [["✕", "Set lines", []]]]);
  });
  it("a glossary without the term draws no ⓘ", () => {
    expect(terms([])).toEqual([[["✓", "Set lines", []], ["✓", "Nets", []]], [["✕", "Set lines", []]]]);
  });
});

describe("boatsVM, referenceVM", () => {
  const rules = index([rule(0), rule(4, { label: "No powered *boats*", rank: 0, book: [[43, 49]] }), rule(5, { label: "Speed  8 km/h", rank: 0, book: [[43, 49]] }),
    rule(6, { plain: "Any fish snagged — even by accident — must be released.", book: [[10, 12]] }), rule(7)]);
  const ctx = ctxOf(rules);
  it("boats: none without boat rules; else each rule's clean label, in force then timed", () => {
    expect(boatsVM(ga(), ctx)).toBeNull();
    expect(boatsVM(ga({ vessel: { active: [4], timed: [5] } }), ctx)).toEqual({ label: "Boats", items: ["No powered boats", "Speed 8 km/h"],
      src: { who: "This water’s ›", aria: "Sources: This water’s", open: "srcs:boats", keys: ["w:r4::r1", "w:r5::r1"], pages: [{ printed: 43, pdf: 49 }] } });
  });
  it("reference: the snagged-fish duty first, then each moment in the frame's order, the phrase from the static moments", () => {
    const statics = { gearMoments: [["release", [["gently", "Gently, back in the water", "long"]]]], conductMeans: { waste: "Don’t waste it" },
      exportConduct: { sell: "Sell your catch" } } as unknown as RegsInputs["statics"];
    expect(referenceVM(ga(), { ...ctx, statics })).toBeNull();
    const r = referenceVM(ga({ caught: [6], conduct: { release: [["gently", [0]]], keep: [["waste", [7]], ["sell", [0]], ["odd", [0]]] } }), { ...ctx, statics })!;
    expect([r.label, r.title, r.sub, r.groups]).toEqual(["Reference", "Handling fish", "5 rules that apply everywhere", [
      { head: "When you catch a fish", items: ["Any fish snagged — even by accident — must be released."] },
      { head: "When you release a fish", items: ["Gently, back in the water"] },
      { head: "When you keep a fish", items: ["Don’t waste it", "Sell your catch", "odd"] },
    ]]);
    expect([r.src?.open, r.src?.keys, r.src?.pages]).toEqual(["srcs:always", ["w:r6::r1", "w:r0::r1", "w:r7::r1"], [{ printed: 8, pdf: 10 }, { printed: 10, pdf: 12 }]]);
  });
});
