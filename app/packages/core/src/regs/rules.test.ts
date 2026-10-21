/**
 * T06: a store rule as one part sees it (page_v36 RULES/RIX decode, NORULE, makePlace.R), over the real rule rows in
 * testing/ruleRows.ts. The store-backed parity over all 726 oracle rules is conformance/src/regs/describe.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import { noRule, ruleIndex, ruleView, viaOf } from "./rules";
import { BOOK } from "./testing/speciesBook";
import { RULE_ROWS } from "./testing/ruleRows";
import { WORDS } from "./words";
import type { ExportRule, RuleFacts } from "./inputs";

const row = (rix: number) => RULE_ROWS[rix]!;
const view = (rix: number, via: "reach" | "trib" = "reach") => ruleView(rix, row(rix)[0], row(rix)[1], BOOK, via);

describe("ruleView: the page's decode of one rule", () => {
  it("reads key, kind, ranks, expanded species, windows and time condition (rix 529)", () => {
    const r = view(529);
    expect(r.rix).toBe(529);
    expect(r.key).toBe("r2:chilliwack_vedder_rivers_does_not_include_sumas_river_see_ma@2-4::chilliwack_vedder_rivers.r8");
    expect(r.kind).toBe("gate");
    expect(r.baseRank).toBe(0);
    expect(r.via).toBe("reach");
    expect(r.rank).toBe(0);
    expect(r.species).toEqual(BOOK.groups.ALL_GAME_FISH!.members);
    expect(r.speciesExcept).toEqual([]);
    expect(r.wins).toEqual([[601, 630]]);
    expect(r.tcond).toBe("");
    expect(r.part).toBeNull();
    expect(r.notes).toEqual([]);
    expect(r.missing).toBe(false);
    expect(r.f).toBe(row(529)[0].fields);
    expect(r.export).toBe(row(529)[0]);
    expect(r.facts).toBe(row(529)[1]);
  });

  it("expands species_except (rix 3092) and reads the undrawn part (rix 78)", () => {
    expect(view(3092).speciesExcept).toEqual(["ST"]);
    expect(view(3092).species).toEqual(["RB", "ST", "CT", "GB", "DV", "LT", "EB"]);
    expect(view(78).part).toBe("on parts");
    expect(view(1367).tcond).toBe("Mon, Tue, Wed, Thu, Fri");
  });

  it("has no kind without display facts", () => {
    expect(ruleView(529, row(529)[0], null, BOOK, "reach").kind).toBeNull();
  });

  it("freezes its arrays", () => {
    const r = view(1644);
    expect(Object.isFrozen(r.wins)).toBe(true);
    expect(Object.isFrozen(r.notes)).toBe(true);
  });

  it("notes an uncertain placement (with or without a reason) and a curator's open question", () => {
    const base = row(529)[0];
    const odd: ExportRule = { ...base, fields: { ...base.fields, review_reason: "two boundary signs" },
      prov: { ...base.prov, uncertain: true, why: "the bridge is not on the map" } };
    expect(ruleView(529, odd, row(529)[1], BOOK, "reach").notes)
      .toEqual(["Uncertain placement: the bridge is not on the map", "Curator still to settle: two boundary signs"]);
    const noWhy: ExportRule = { ...base, prov: { ...base.prov, uncertain: true, why: null } };
    expect(ruleView(529, noWhy, row(529)[1], BOOK, "reach").notes).toEqual(["Uncertain placement: no reason given"]);
  });
});

describe("via and rank (T06 test 6)", () => {
  it("a this-water rule reached by the tributary walk ranks 1 (ape_501, rix 1644)", () => {
    const r = view(1644, "trib");
    expect(r.key).toBe("r5:atnarko_bella_coola_rivers_includes_tributaries_except_burnt@5-11+5-6+5-8::atnarko_bella_coola_rivers.r10");
    expect(r.baseRank).toBe(0);
    expect(r.via).toBe("trib");
    expect(r.rank).toBe(1);
    expect(view(1644, "reach").rank).toBe(0);
  });

  it("a region rule reached by the tributary walk ranks 1 too; a negative base rank keeps its rank", () => {
    expect(view(3092, "trib").rank).toBe(1);
    const fed: ExportRule = { ...row(529)[0], prov: { ...row(529)[0].prov, rank: -1 } };
    const r = ruleView(529, fed, row(529)[1], BOOK, "trib");
    expect(r.baseRank).toBe(-1);
    expect(r.rank).toBe(-1);
  });
});

describe("noRule: the visible stand-in for a rule the data lacks", () => {
  it("is key #rix, rank 4, missing, and says so in its words", () => {
    const r = noRule(7);
    expect(r.rix).toBe(7);
    expect(r.key).toBe("#7");
    expect(r.baseRank).toBe(4);
    expect(r.rank).toBe(4);
    expect(r.via).toBe("reach");
    expect(r.missing).toBe(true);
    expect(r.kind).toBeNull();
    expect(r.facts).toBeNull();
    expect(r.export.label).toBe("(rule 7: missing from the page’s data)");
    expect(r.export.verbatim).toBe("(rule 7: missing from the page’s data)");
    expect(r.notes).toEqual(["Missing from the page’s data: rule 7"]);
    expect(r.species).toEqual([]);
    expect(r.wins).toEqual([]);
    expect(WORDS.MISSING_RULE(7)).toBe("(rule 7: missing from the page’s data)");
  });

  it("ranks as the page's R would when a tributary list names it", () => {
    expect(noRule(7, "trib").rank).toBe(1);
  });
});

describe("viaOf, ruleIndex", () => {
  const stored = new Map<number, { export: ExportRule; facts: RuleFacts }>(
    [529, 1644, 3092].map((ix) => [ix, { export: row(ix)[0], facts: row(ix)[1] }]));

  it("viaOf names every reach rix 'reach' and every trib rix 'trib'; no ruleset, no entries", () => {
    expect([...viaOf({ reach: [529, 3092], trib: [1644] })]).toEqual([[529, "reach"], [3092, "reach"], [1644, "trib"]]);
    expect(viaOf(null).size).toBe(0);
  });

  it("views each stored rule through the part's via; a rix the part does not list is 'reach'", () => {
    const ix = ruleIndex(stored, viaOf({ reach: [529], trib: [1644] }), BOOK);
    expect(ix.size).toBe(3);
    expect(ix.get(1644).via).toBe("trib");
    expect(ix.get(1644).rank).toBe(1);
    expect(ix.get(529).via).toBe("reach");
    expect(ix.get(3092).via).toBe("reach");
    expect(ix.get(3092).rank).toBe(3);
    expect(ix.get(529)).toBe(ix.get(529));   // one view per rix per part
  });

  it("never misses: an unknown rix is the noRule stand-in, and has() says it is not stored", () => {
    const ix = ruleIndex(stored, new Map(), BOOK);
    expect(ix.has(529)).toBe(true);
    expect(ix.has(9999)).toBe(false);
    expect(ix.get(9999).missing).toBe(true);
    expect(ix.get(9999).key).toBe(noRule(9999).key);   // "#" + rix (written so, not as a literal the colour gate reads)
  });
});
