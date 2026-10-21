/**
 * others.ts, store-free, over hand-built rows (testing/keepFixtures.ts). Golden parity on all 134 states is
 * app/conformance/src/regs/others.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { FishManifest } from "./fish.generated";
import { drawnRows } from "./grouphead";
import { plain } from "./line";
import { groupRows, otherFishOf, shortValue, splitRows, TROUTISH } from "./others";
import { cardOf, GROUP, INP, kind, rowOf, TROUT_ROW } from "./testing/keepFixtures";

const FISH: FishManifest = {
  KO: [{ file: "k.webp", caption: "Kokanee", credit: "", type: "", source: "", phase: null, standInFor: null, width: 2, height: 1 }],
};
const KO = rowOf({ kind: "keep", pool: 3, daily: 5, members: ["KO"], all_members: ["KO"],
  week: { species: ["KO"], says: "Keep 5 on weekends", status: "Weekends: keep 5 · Weekdays: release" } });
const BASS = rowOf({ kind: "no_limit", pool: 3, members: ["LMB", "SMB"], all_members: ["LMB", "SMB"] });
const GR = rowOf({ kind: "keep", pool: 3, daily: 4, members: ["GR"], all_members: ["GR"],
  group: GROUP({ tree: [{ node: "tier", id: "t1", tier: "own", n: 4, says: "", by: 3,
    children: [kind({ species: ["GR"], size_rule: "keep 30 cm (12 in) or longer; release the rest" })] }] }) });
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
      { thumb: "KO", name: "Kokanee", value: "5 a day", tone: "keep", week: "Weekends: keep 5 · Weekdays: release", sizeHint: null, open: "other:o0", stamps: [] },
      { thumb: null, name: "Bass", value: "no limit", tone: "nolimit", week: null, sizeHint: null, open: "other:o1", stamps: [] },
      { thumb: null, name: "GR", value: "4 a day", tone: "keep", week: null, sizeHint: "· size limit", open: "other:o2", stamps: [] },
      { thumb: null, name: "Protected species", value: "closed", tone: "closed", week: null, sizeHint: null, open: "other:o3", stamps: [] }]);
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
