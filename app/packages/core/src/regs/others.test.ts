/**
 * others.ts, store-free, over hand-built rows (testing/keepFixtures.ts). Golden parity on all 134 states is
 * app/conformance/src/regs/others.parity.test.ts (retired, T439).
 */
import { describe, expect, it } from "vitest";
import type { HydRow } from "./card";
import type { FishManifest } from "./fish.generated";
import type { Settled } from "./settle";
import type { RegsInputs, RuleView } from "./inputs";
import { drawnRows } from "./grouphead";
import { plain } from "./line";
import { backOf, groupRows, listRows, looseRows, otherFishOf, shortValue, splitRows } from "./others";
import { cardOf, GROUP, INP, kind, rowOf, TROUT_ROW } from "./testing/keepFixtures";

/** a day with nothing lifted (the rows' items come from the stored roles alone) */
const NONE = { lifted: new Map() } as unknown as Settled;
const FISH: FishManifest = {
  KO: [{ file: "k.webp", caption: "Kokanee", credit: "", type: "", source: "", phase: null, standInFor: null, width: 2, height: 1 }],
};
const KO = rowOf({ kind: "keep", pool: 3, daily: 5, members: ["KO"], all_members: ["KO"],
  week: { species: ["KO"], says: "Keep 5 on weekends", status: "Weekends: keep 5 · Weekdays: release" } });
const BASS = rowOf({ kind: "no_limit", pool: 3, members: ["LMB", "SMB"], all_members: ["LMB", "SMB"] });
const GR = rowOf({ kind: "keep", pool: 3, daily: 4, members: ["GR"], all_members: ["GR"],
  group: GROUP({ tree: [{ node: "tier", id: "t1", tier: "own", n: 4, says: "", by: 3,
    children: [kind({ species: ["GR"], size_rule: "must be 30 cm (12 in) or longer; release any shorter" })] }] }) });
const PROT = rowOf({ kind: "closed", prot: ["GREEN_STURGEON"] });

describe("splitRows (page v36 rowsHtml; T810 S: by the trout table's `covers`, never a hand list)", () => {
  it("beside a table: the covered rows are the table's; two or more others are the list, fewer are drawn in full", () => {
    expect(splitRows(cardOf([KO, TROUT_ROW, BASS]), [1])).toEqual({ main: [], rest: [0, 2] });
    expect(splitRows(cardOf([TROUT_ROW, KO]), [0])).toEqual({ main: [1], rest: null });   // one other: drawn in full (owner "Stays as today")
    expect(splitRows(cardOf([KO, BASS]), [])).toEqual({ main: [0, 1], rest: null });      // no table: every row in order
  });
  it("a row the table covers is never drawn by the old card, whatever its fish (no TROUTISH)", () => {
    expect(splitRows(cardOf([KO, TROUT_ROW, BASS]), [0])).toEqual({ main: [], rest: [1, 2] });
  });
  it("a closed day draws no rows: no groups, no list", () => {
    const shut = cardOf([TROUT_ROW, KO, BASS], { status: "closed" });
    expect([splitRows(shut, [0]), groupRows(shut, drawnRows(shut), [0]), otherFishOf(shut, INP(), FISH, [0], NONE)]).toEqual([{ main: [], rest: null }, [], null]);
  });
  it("groups are the drawn full rows whose stored group has a headline", () => {
    const card = cardOf([GR, KO, BASS, { ...GR, group: GROUP({ headline: null as never }) }]);
    expect(groupRows(card, drawnRows(card), [])).toEqual([0]);
    expect(groupRows(card, new Set(), [])).toEqual([]);
    expect(groupRows(card, drawnRows(card), [0])).toEqual([]);                            // covered: the table's
  });
});

describe("otherFishOf", () => {
  it("each row's title and one-word answer; the weekday split under its name; the first pictured fish; the size hint", () => {
    expect(otherFishOf(cardOf([TROUT_ROW, KO, BASS, GR, PROT]), INP(), FISH, [0], NONE)).toEqual([
      { thumb: "KO", name: "Kokanee", value: "5 a day", tone: "keep", week: "Weekends: keep 5 · Weekdays: release", sizeHint: null, open: "other:o0", stamps: [], back: [] },
      { thumb: null, name: "Bass", value: "no limit", tone: "nolimit", week: null, sizeHint: null, open: "other:o1", stamps: [], back: [] },
      { thumb: null, name: "GR", value: "4 a day", tone: "keep", week: null, sizeHint: "· size limit", open: "other:o2", stamps: [], back: [] },
      { thumb: null, name: "Protected species", value: "closed", tone: "closed", week: null, sizeHint: null, open: "other:o3", stamps: [], back: [] }]);
  });
  it("(rows 32) a row's stored stamps, each worded as a stamp line under it — the store's words, never chosen here", () => {
    const WSG = rowOf({ kind: "release", win: 3, members: ["WSG"], all_members: ["WSG"], stamps: [{ doc: "white_sturgeon_licence",
      act: "targeting", name: "White Sturgeon Conservation Licence", when: "Only if you fish for white sturgeon, even to release them" }] });
    const o = otherFishOf(cardOf([TROUT_ROW, KO, WSG]), INP(), FISH, [0], NONE)!;
    expect(o.map((x) => x.stamps.map(plain))).toEqual([[],
      ["You need White Sturgeon Conservation Licence › only if you fish for white sturgeon, even to release them"]]);
    // mutation: other stored words come out as they are (the app adds no document and drops none)
    const mut = rowOf({ ...WSG, stamps: [{ doc: "x", act: "targeting", name: "MUT-DOC", when: "MUT-WHEN" }] });
    expect(otherFishOf(cardOf([TROUT_ROW, KO, mut]), INP(), FISH, [0], NONE)![1]!.stamps.map(plain)).toEqual(["You need MUT-DOC › mUT-WHEN"]);
  });
  it("the size hint rides only a keep count (prototype: it wraps the .k-keep value)", () => {
    const o = otherFishOf(cardOf([TROUT_ROW, { ...GR, kind: "no_limit", daily: null }, { ...GR, kind: "release", daily: null }]), INP(), FISH, [0], NONE)!;
    expect(o.map((x) => [x.value, x.sizeHint])).toEqual([["no limit", null], ["release", null]]);
  });
  it("shortValue: the stored kind in one word", () => {
    expect([shortValue("keep", 15), shortValue("nolimit", Infinity), shortValue("closed", null), shortValue("release", null)])
      .toEqual(["15 a day", "no limit", "closed", "release"]);
  });
});

describe("(T670 K4/K5) a full row nothing draws is a line of the list", () => {
  // (T810 S) the trout ones (Region 8's "20 brook trout from streams", p.68; the Duncan River's bull trout) are the
  // table's rows now (`covers`); a loose row is another fish's: a pooled row with no stored group headline, no parent
  it("with no table: the loose rows are the list (`other:oN` counts them); a row with a headline is a card, not listed", () => {
    const card = cardOf([KO, BASS, GR]);
    expect([splitRows(card, []).main, looseRows(card, []), listRows(card, [])]).toEqual([[0, 1, 2], [0, 1], [0, 1]]);
    expect(otherFishOf(card, INP(), FISH, [], NONE)!.map((o) => [o.name, o.value, o.open])).toEqual([
      ["Kokanee", "5 a day", "other:o0"], ["Bass", "no limit", "other:o1"]]);
  });
  it("beside a table, a lone other fish is drawn in full and, loose, still listed; the table's rows never are", () => {
    expect(listRows(cardOf([TROUT_ROW, KO]), [0])).toEqual([1]);
    expect(listRows(cardOf([TROUT_ROW]), [0])).toBeNull();
  });
  it("a child row (drawn inside its parent's card) is not loose; a closed day lists nothing", () => {
    expect(looseRows(cardOf([GR, { ...KO, parent: 0 }]), [])).toEqual([]);
    expect(looseRows(cardOf([KO, BASS], { status: "closed" }), [])).toEqual([]);
  });
  it("a loose row's size limit is hinted from its stored bands (never read as any size)", () => {
    const BB = rowOf({ kind: "keep", pool: 3, daily: 1, members: ["BB"], all_members: ["BB"], items: [{ members: ["BB"], bands: [[60, null, 1]], back: false, xref: false, sub: null, conds: [] }] });
    expect(otherFishOf(cardOf([TROUT_ROW, BB]), INP(), FISH, [0], NONE)![0]!.sizeHint).toBe("· size limit");
  });
});

describe("(T670 K2) the fish of a row that must go back", () => {
  it("each stored `back` condition is said in its rule's stored sentence (else its label), with its status; other conditions are not", () => {
    const row = { conds: [{ c: "size", a: 0, b: 30, r: { key: "e::s" } },
      { c: "back", who: ["MW"], status: "release", r: { key: "e::r", facts: { plain: "Release every mountain whitefish." }, export: { label: "L" } } },
      { c: "back", who: ["X"], status: "closed", r: { key: "e::c", facts: null, export: { label: "**No fishing** for X" } } },
      { c: "back", who: ["Y"], status: "release", r: null }] } as unknown as HydRow;
    expect(backOf(row)).toEqual([{ status: "release", says: "Release every mountain whitefish.", rule: "e::r" },
      { status: "closed", says: "No fishing for X", rule: "e::c" }]);
    expect(backOf({ conds: null } as unknown as HydRow)).toEqual([]);
  });
});

describe("(T892) otherFishOf: a row from another water's row carried down to its tributaries says so under its name", () => {
  // the rows frame's stored roles (rowSources), the rules seen through this part: Kootenay River's row reaches Anchor
  // Creek by its tributaries (`via` trib, rank 1); Region 4's limit it replaced; the creek's own whitefish rule
  const view = (rix: number, rank: number, entry: string): RuleView => ({ rix, key: `e${rix}::r${rix}`, rank, baseRank: rank === 1 ? 0 : rank,
    via: rank === 1 ? "trib" : "reach", part: null, missing: false, f: {}, species: [], speciesExcept: [], facts: null,
    export: { prov: { rank: rank === 1 ? 0 : rank, entry_name: entry } } }) as unknown as RuleView;
  const RS: Record<number, RuleView> = { 10: view(10, 1, "Kootenay River"), 11: view(11, 3, "Region 4"), 12: view(12, 0, "Anchor Creek") };
  const BB = rowOf({ kind: "release", members: ["BB"], all_members: ["BB"] });
  const MW = rowOf({ kind: "keep", daily: 15, members: ["MW"], all_members: ["MW"] });
  const base = cardOf([TROUT_ROW, BB, MW], { R: { BB: {}, MW: {} } as never });
  const card = { ...base, ctx: { ...base.ctx, s: 0 } } as typeof base;
  const inp = { ...INP({ name: "Anchor Creek" }), rules: { get: (i: number) => RS[i]!, has: (i: number) => !!RS[i], size: 3 },
    year: { rowsBySeg: [{ fish: {
      BB: { lead: null, hatchery: { roles: [[11, "replaced", 10], [10, "governs", null]] }, wild: null },
      MW: { lead: null, hatchery: { roles: [[11, "replaced", 12], [12, "governs", null]] }, wild: null },
    } }] } } as unknown as RegsInputs;
  it("the inherited row: whole line; the water's own row: none", () => {
    expect(otherFishOf(card, inp, FISH, [0], NONE)!.map((o) => [o.name, o.value, o.from ?? null])).toEqual([
      ["BB", "release", "Kootenay River’s rule for its tributaries"],
      ["MW", "15 a day", null],
    ]);
  });
});
