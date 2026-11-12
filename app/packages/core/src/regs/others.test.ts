/**
 * others.ts, store-free, over hand-built rows (testing/keepFixtures.ts). Golden parity on all 134 states is
 * app/conformance/src/regs/others.parity.test.ts (retired, T439).
 */
import { describe, expect, it } from "vitest";
import type { HydRow } from "./card";
import type { FishManifest } from "./fish.generated";
import { drawnRows } from "./grouphead";
import { plain } from "./line";
import { backOf, groupRows, listRows, looseRows, otherFishOf, shortValue, splitRows, TROUTISH } from "./others";
import { cardOf, GROUP, INP, kind, rowOf, TROUT_ROW } from "./testing/keepFixtures";

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

describe("splitRows (page v36 rowsHtml)", () => {
  it("trout and char rows first in full, the rest a list — when there are two or more others", () => {
    expect(TROUTISH).toEqual(["RB", "CT", "EB", "GB", "LT", "DV", "ST", "BT", "CT_ws", "CT_c"]);
    expect(splitRows(cardOf([KO, TROUT_ROW, BASS]))).toEqual({ main: [1], rest: [0, 2] });
    expect(splitRows(cardOf([TROUT_ROW, KO]))).toEqual({ main: [0, 1], rest: null });   // one other: drawn in order
    expect(splitRows(cardOf([KO, BASS]))).toEqual({ main: [0, 1], rest: null });        // no trout row
  });
  it("a closed day draws no rows: no groups, no list", () => {
    const shut = cardOf([TROUT_ROW, KO, BASS], { status: "closed" });
    expect([splitRows(shut), groupRows(shut, drawnRows(shut)), otherFishOf(shut, INP(), FISH)]).toEqual([{ main: [], rest: null }, [], null]);
  });
  it("groups are the drawn full rows whose stored group has a headline", () => {
    const card = cardOf([TROUT_ROW, KO, BASS, { ...TROUT_ROW, group: GROUP({ headline: null as never }) }]);
    expect(groupRows(card, drawnRows(card))).toEqual([0]);
    expect(groupRows(card, new Set())).toEqual([]);
  });
});

describe("otherFishOf", () => {
  it("each row's title and one-word answer; the weekday split under its name; the first pictured fish; the size hint", () => {
    expect(otherFishOf(cardOf([TROUT_ROW, KO, BASS, GR, PROT]), INP(), FISH)).toEqual([
      { thumb: "KO", name: "Kokanee", value: "5 a day", tone: "keep", week: "Weekends: keep 5 · Weekdays: release", sizeHint: null, open: "other:o0", stamps: [], back: [] },
      { thumb: null, name: "Bass", value: "no limit", tone: "nolimit", week: null, sizeHint: null, open: "other:o1", stamps: [], back: [] },
      { thumb: null, name: "GR", value: "4 a day", tone: "keep", week: null, sizeHint: "· size limit", open: "other:o2", stamps: [], back: [] },
      { thumb: null, name: "Protected species", value: "closed", tone: "closed", week: null, sizeHint: null, open: "other:o3", stamps: [], back: [] }]);
  });
  it("(rows 32) a row's stored stamps, each worded as a stamp line under it — the store's words, never chosen here", () => {
    const WSG = rowOf({ kind: "release", win: 3, members: ["WSG"], all_members: ["WSG"], stamps: [{ doc: "white_sturgeon_licence",
      act: "targeting", name: "White Sturgeon Conservation Licence", when: "Only if you fish for white sturgeon, even to release them" }] });
    const o = otherFishOf(cardOf([TROUT_ROW, KO, WSG]), INP(), FISH)!;
    expect(o.map((x) => x.stamps.map(plain))).toEqual([[],
      ["You need White Sturgeon Conservation Licence › only if you fish for white sturgeon, even to release them"]]);
    // mutation: other stored words come out as they are (the app adds no document and drops none)
    const mut = rowOf({ ...WSG, stamps: [{ doc: "x", act: "targeting", name: "MUT-DOC", when: "MUT-WHEN" }] });
    expect(otherFishOf(cardOf([TROUT_ROW, KO, mut]), INP(), FISH)![1]!.stamps.map(plain)).toEqual(["You need MUT-DOC › mUT-WHEN"]);
  });
  it("the size hint rides only a keep count (prototype: it wraps the .k-keep value)", () => {
    const o = otherFishOf(cardOf([TROUT_ROW, { ...GR, kind: "no_limit", daily: null }, { ...GR, kind: "release", daily: null }]), INP(), FISH)!;
    expect(o.map((x) => [x.value, x.sizeHint])).toEqual([["no limit", null], ["release", null]]);
  });
  it("shortValue: the stored kind in one word", () => {
    expect([shortValue("keep", 15), shortValue("nolimit", Infinity), shortValue("closed", null), shortValue("release", null)])
      .toEqual(["15 a day", "no limit", "closed", "release"]);
  });
});

describe("(T670 K4/K5) a full row nothing draws is a line of the list", () => {
  // Region 8's "20 brook trout from streams" (p.68): a pooled trout row with no stored group headline, no parent
  const EB = rowOf({ kind: "keep", pool: 3, daily: 20, members: ["EB"], all_members: ["EB"], items: [{ members: ["EB"], bands: [[0, null, 20]], back: false, xref: false, sub: null, conds: [] }] });
  it("loose rows lead the list; `other:oN` counts them first; with no other row they are the list", () => {
    const card = cardOf([TROUT_ROW, EB, KO, BASS]);
    expect([looseRows(card), listRows(card)]).toEqual([[1], [1, 0 + 2, 3]]);
    expect(otherFishOf(card, INP(), FISH)!.map((o) => [o.name, o.value, o.tone, o.sizeHint, o.open])).toEqual([
      ["Brook trout", "20 a day", "keep", null, "other:o0"], ["Kokanee", "5 a day", "keep", null, "other:o1"], ["Bass", "no limit", "nolimit", null, "other:o2"]]);
    expect(listRows(cardOf([TROUT_ROW, EB]))).toEqual([1]);                      // rest null: still listed
    expect(listRows(cardOf([TROUT_ROW, KO]))).toEqual([1]);                      // any pooled row with no headline, not only trout
    expect(listRows(cardOf([TROUT_ROW]))).toBeNull();
  });
  it("a child row (drawn inside its parent's card) and a row with a headline are not loose; a closed day lists nothing", () => {
    expect(looseRows(cardOf([TROUT_ROW, { ...EB, parent: 0 }]))).toEqual([]);
    expect(looseRows(cardOf([TROUT_ROW, EB], { status: "closed" }))).toEqual([]);
  });
  it("a loose row's size limit is hinted from its stored bands (never read as any size)", () => {
    const DV = rowOf({ kind: "keep", pool: 3, daily: 1, members: ["DV"], all_members: ["DV"], items: [{ members: ["DV"], bands: [[60, null, 1]], back: false, xref: false, sub: null, conds: [] }] });
    expect(otherFishOf(cardOf([TROUT_ROW, DV]), INP(), FISH)![0]!.sizeHint).toBe("· size limit");
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
