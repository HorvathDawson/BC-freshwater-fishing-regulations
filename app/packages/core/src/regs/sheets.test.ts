/**
 * T36 (A) — the sheets over hand-built inputs. The rules are the store's rows (testing/ruleRows.ts), the glossary terms the
 * store's `glossary` static; every expected string is the golden's (app/design/regs-v11/port/golden/golden.json, regs v38,
 * path in each test). The 134-state parity is app/conformance/src/regs/sheets.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { GroupPossession } from "../answers.generated";
import type { GlossaryTerm, RegsInputs, RuleFacts } from "./inputs";
import type { PossessionVM, SheetCtx, SheetVM } from "./model";
import { ruleIndex } from "./rules";
import { levelTag, sheetLines, sheetOf } from "./sheets";
import { RULE_ROWS } from "./testing/ruleRows";
import { BOOK } from "./testing/speciesBook";

// the store's glossary terms (glossary 5), copied whole
const BAIT_BAN: GlossaryTerm = { id: "bait_ban", term: "Bait ban", pages: [4, 8], source: "definitions",
  says: "No natural bait of any kind — worms, roe, insects, fish parts — for any fish, while the ban runs. Lures and flies are fine.",
  quote: "Bait Ban: the use of natural bait is prohibited in waters with a bait ban. Bait may be banned year round or seasonally. During the period when bait is banned it is banned for all angling and for all species." };
const SBH: GlossaryTerm = { id: "single_barbless_hook", term: "Single barbless hook", pages: [8, 80], source: "definitions", says: "One hook, one point, no barb.", quote: "" };
const POSQ: GlossaryTerm = { id: "possession_quota", term: "Possession quota", pages: [13, 80], source: "definitions",
  says: "The fish you have with you at any time — in camp, a cooler or a vehicle.",
  quote: "possession quota: the number of fish of any species that an angler may have in their possession at any given time, EXCEPT at place of ordinary residence. In most instances, the possession quota is two times the daily quota. See Tables for exceptions.",
  example: "With a daily quota of 5 trout you may have 10: 2 days' fish." };
const HW: GlossaryTerm = { id: "hatchery_wild", term: "Hatchery or wild (adipose fin)", pages: [13, 80], source: "definitions p.80; adipose diagram p.13",
  says: "Hatchery trout and steelhead are marked before release by removing the adipose fin — the small fleshy fin on the back between the dorsal fin and the tail — so a hatchery fish has a healed scar there. A fish with its adipose fin (or a fresh, unhealed wound) is wild. Where only hatchery fish may be kept, every wild one goes back.",
  quote: "hatchery trout: in some waters, hatchery trout may be harvested but wild trout must be released." };

// the store's rows: Vedder's Jun closure (with its applies_trib, golden rule sheet line 2) and Region 2's possession rule
const CONDUCT = [{ entry_id: "zp:conduct", rule_id: "conduct.r1", type: "conduct", family: "conduct", label: "Do not waste the fish you catch",
  verbatim: "Waste the fish you catch.", fields: { conduct: ["do_not_waste_catch"] }, prov: { rank: 4, entry_name: "Everywhere in B.C." } },
  { kind: "conduct", title: "Do not waste the fish you catch", book: [[8, 10], [10, 12]], applies: "A B.C.-wide rule — everywhere in B.C." }] as const;
const stored = new Map([
  [529, { export: RULE_ROWS[529]![0], facts: { ...RULE_ROWS[529]![1], applies_trib: "Inherited from Chilliwack River: its row covers its tributaries." } as RuleFacts }],
  [3079, { export: RULE_ROWS[3079]![0], facts: RULE_ROWS[3079]![1] }],
  [7, { export: CONDUCT[0], facts: CONDUCT[1] as unknown as RuleFacts }],
]);

// golden vedder_1010 rows' group.possession (rows 31)
const OWN: GroupPossession[] = [
  { per_daily: 2, of: 4, n: 8, by: 3079, example: "With 4 trout and char a day you may have 8: 2 days’ fish.", doubles: "Possession doubles the 4 a day, not the 2 from streams." },
  { per_daily: 2, of: 15, n: 30, by: 3079, example: "With 15 whitefish a day you may have 30: 2 days’ fish.", doubles: null },
];
const POS: PossessionVM = { label: "Possession", value: "2 days’ quota", says: "Possession: 2 days’ quota. Fish at home don’t count.",
  rule: "z2:possession_quota::possession_quota.r1", term: "possession_quota", open: "pos:pos" };

function ctx(o: { terms?: GlossaryTerm[]; pdf?: Record<number, number>; own?: GroupPossession[]; pos?: PossessionVM | null } = {}): SheetCtx {
  const inp = {
    rules: ruleIndex(stored, new Map(), BOOK),
    statics: { glossary: { version: 5, terms: o.terms ?? [BAIT_BAN, SBH, POSQ, HW] }, pdfPages: o.pdf ?? { 4: 6, 8: 10, 13: 15, 80: 86 } },
    frames: { rows: { rows: (o.own ?? OWN).map((p) => ({ group: { possession: p } })).concat([{ group: null } as never]) } },
  } as unknown as RegsInputs;
  return { inp, today: { month: 10, day: 10 }, page: { keep: { possession: o.pos === undefined ? POS : o.pos } } as unknown as SheetCtx["page"] };
}
const lines = (vm: SheetVM | null) => sheetLines(vm!);

describe("gl: (prototype glossBody)", () => {
  it("1. one term: its words, the book's words folded and closed, its pages linked through the page map (golden sheets['gl:bait_ban'])", () => {
    expect(lines(sheetOf(ctx(), "gl:bait_ban"))).toEqual({
      title: "Bait ban", sub: null, folds: [{ summary: "In the book’s words", open: false }],
      pages: [{ text: "p.4 ↗", pdf: 6 }, { text: "p.8 ↗", pdf: 10 }],
      lines: ["No natural bait of any kind — worms, roe, insects, fish parts — for any fish, while the ban runs. Lures and flies are fine.",
        "In the book’s words",
        "Bait Ban: the use of natural bait is prohibited in waters with a bait ban. Bait may be banned year round or seasonally. During the period when bait is banned it is banned for all angling and for all species.",
        "Rule book p.4 ↗ p.8 ↗"],
      nested: [] });
  });
  it("a page the map lacks is drawn unlinked (never computed): 'p.4', not a link", () => {
    const l = lines(sheetOf(ctx({ pdf: {} }), "gl:bait_ban"));
    expect([l.pages, l.lines[3]]).toEqual([[], "Rule book p.4 p.8"]);
  });
  it("3. several terms: 'What these words mean', one section per term, in the key's order; unknown ids dropped", () => {
    const vm = sheetOf(ctx(), "gl:bait_ban,nope,single_barbless_hook")!;
    expect([vm.title, vm.blocks.map((b) => (b.k === "term" ? b.title : b.k))]).toEqual(["What these words mean", ["Bait ban", "Single barbless hook"]]);
    expect(lines(vm).lines.slice(-3)).toEqual(["Single barbless hook", "One hook, one point, no barb.", "Rule book p.8 ↗ p.80 ↗"]);
    expect(sheetOf(ctx(), "gl:nope")).toBeNull();
  });
});

describe("rule: (page v36 openRule in the prototype's sheet)", () => {
  it("4. B.C.-wide (golden sheets['rule:zp:conduct::conduct.r1'])", () => {
    const l = lines(sheetOf(ctx(), "rule:zp:conduct::conduct.r1"));
    expect([l.title, l.sub, l.folds, l.lines.slice(0, 4), l.lines.at(-1), l.pages]).toEqual(["Do not waste the fish you catch", "B.C. rule · Everywhere in B.C.",
      [{ summary: "Technical detail", open: false }],
      ["A B.C.-wide rule — everywhere in B.C.", "The exact words", "Waste the fish you catch.", "Technical detail"],
      "Rule book p.8 ↗ Rule book p.10 ↗", [{ text: "Rule book p.8 ↗", pdf: 10 }, { text: "Rule book p.10 ↗", pdf: 12 }]]);
    expect(l.lines[4]).toBe('zp:conduct::conduct.r1 { "conduct": [ "do_not_waste_catch" ] }');
  });
  it("5. this water + applies_trib (golden rule:r2:chilliwack…::chilliwack_vedder_rivers.r8)", () => {
    const l = lines(sheetOf(ctx(), "rule:r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8"));
    expect([l.title, l.sub, l.lines.slice(0, 5), l.lines.at(-1)]).toEqual(["No fishing, Jun 1-Jun 30 — downstream of Vedder Crossing Bridge",
      "This water’s rule · Chilliwack River",
      ["This water’s own row in the 2025–2027 BC Freshwater Fishing Regulations Synopsis: Chilliwack River.",
        "Inherited from Chilliwack River: its row covers its tributaries.", "The exact words", "No Fishing June 1-30", "No fishing, Jun 1–30."],
      "Rule book p.22 ↗"]);
  });
  it("the level tag reads the part's rank: 'Region 2 rule · Region 2' says 'Region 2'; a rule the store lacks opens nothing", () => {
    expect(lines(sheetOf(ctx(), "rule:z2:possession_quota::possession_quota.r1")).sub).toBe("Region 2");
    expect(sheetOf(ctx(), "rule:z9:nope::nope.r1")).toBeNull();
    const r = ruleIndex(stored, new Map([[529, "trib" as const]]), BOOK).get(529);
    expect(levelTag(r)).toBe("Downstream rule");
  });
});

describe("pos: (prototype sheetHtml pos branch, rows 25 own examples)", () => {
  it("6. own examples (golden vedder_1010 pos): para, para, example head, list, fold, source", () => {
    const vm = sheetOf(ctx(), "pos:pos")!;
    expect(vm.blocks.map((b) => b.k)).toEqual(["para", "para", "example", "list", "fold", "source"]);
    expect(lines(vm)).toEqual({ title: "Possession", sub: null, folds: [{ summary: "In the rule book’s words", open: false }],
      pages: [{ text: "Rule book p.21 ↗", pdf: 23 }],
      lines: ["Possession: 2 days’ quota. Fish at home don’t count.", "The fish you have with you at any time — in camp, a cooler or a vehicle.",
        "For example:", "With 4 trout and char a day you may have 8: 2 days’ fish. Possession doubles the 4 a day, not the 2 from streams.",
        "With 15 whitefish a day you may have 30: 2 days’ fish.", "In the rule book’s words", POSQ.quote, "Source › Rule book p.21 ↗"],
      nested: ["rule:z2:possession_quota::possession_quota.r1"] });
    expect(lines(vm).lines.some((l) => l.includes("5 trout"))).toBe(false);
  });
  it("7. no own example: the term's generic one", () => {
    expect(lines(sheetOf(ctx({ own: [] }), "pos:pos")).lines.filter((l) => l.startsWith("For example")))
      .toEqual(["For example: With a daily quota of 5 trout you may have 10: 2 days' fish."]);
  });
  it("8. the term's words are dropped when they restate the sentence", () => {
    const t = { ...POSQ, says: "Possession is 2 days’ quota. Fish at home don’t count, ever." };
    expect(lines(sheetOf(ctx({ terms: [t] }), "pos:pos")).lines.slice(0, 2)).toEqual(["Possession: 2 days’ quota. Fish at home don’t count.", "For example:"]);
  });
  it("no possession row on the page: no sheet", () => { expect(sheetOf(ctx({ pos: null }), "pos:pos")).toBeNull(); });
});

describe("hatch (page v36 adiposeHtml + the hatchery_wild term)", () => {
  it("16. golden sheets.hatch", () => {
    const l = lines(sheetOf(ctx(), "hatch"));
    expect([l.title, l.sub, l.lines.slice(0, 4), l.lines.at(-1), l.folds]).toEqual(["Hatchery or wild?", "The adipose fin",
      ["Wild · adipose fin there", "Hatchery · clipped, a healed scar instead",
        "Look on the back, just in front of the tail. A small fin there means wild. Only a scar means hatchery. If you can’t tell, treat the fish as wild.",
        "Hatchery or wild (adipose fin)"], "Rule book p.13 ↗ p.80 ↗", [{ summary: "In the book’s words", open: false }]]);
  });
});

describe("every key", () => {
  it("19. an unknown or not-yet-built key opens nothing (never throws)", () => {
    for (const k of ["date", "other:o99", "src:r0", "srcs:bait", "licsrc:adult", "sp:RB", "parts", "kind:o0"])
      expect(sheetOf(ctx(), k as never), k).toBeNull();
  });
  it("17. every fold is closed", () => {
    for (const k of ["gl:bait_ban", "rule:zp:conduct::conduct.r1", "pos:pos", "hatch"] as const)
      expect(lines(sheetOf(ctx(), k)).folds.every((f) => f.open === false), k).toBe(true);
  });
});
