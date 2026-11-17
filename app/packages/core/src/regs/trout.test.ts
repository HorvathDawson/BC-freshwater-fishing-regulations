/**
 * (T810 N) trout.ts and the dev switch's split, store-free. The vectors, the hand cases, the a11y golden, the "drawn
 * exactly once" proof over the trout fixture release and trout.ts's imports test (it reads the source: core imports no
 * platform) are app/conformance/src/regs/trout.release.test.ts.
 */
import { describe, expect, it } from "vitest";
import { troutGlance } from "../glance";
import { keepOrder } from "./keep";
import { listRows, looseRows, splitRows } from "./others";
import { dayPhrase } from "./strip";
import { a11yOf, DAY_SLOT } from "./trout";
import type { TroutVM } from "./model";
import { TROUT_NAV } from "./words";
import { cardOf, rowOf, TROUT_ROW } from "./testing/keepFixtures";

describe("dayPhrase: the strip's day words, shared with the trout table's slot", () => {
  it("today, else on <day>", () => {
    expect(dayPhrase({ month: 7, day: 15 }, { month: 7, day: 15 })).toBe("today");
    expect(dayPhrase({ month: 7, day: 15 }, { month: 7, day: 16 })).toBe("on Jul 15");
    expect(DAY_SLOT).toBe("{day}");
  });
});

describe("a11yOf: payload texts in payload order, joined by \". \", nothing else", () => {
  it("drops the empty parts only", () => {
    expect(a11yOf(["Steelhead", null, "Hatchery only", undefined, "only 3", "until Oct 31, then release"]))
      .toBe("Steelhead. Hatchery only. only 3. until Oct 31, then release");
  });
});

describe("the dev switch's split by covers (others.ts)", () => {
  const KO = rowOf({ kind: "keep", pool: 3, daily: 5, members: ["KO"], all_members: ["KO"] });
  const BASS = rowOf({ kind: "no_limit", pool: 3, members: ["LMB", "SMB"], all_members: ["LMB", "SMB"] });
  // (Nahwitti) Region 1's "Release all char": a row with no pool and no group, beside the trout keep pool
  const CHAR = rowOf({ kind: "release", members: ["DV", "LT", "EB"], all_members: ["DV", "LT", "EB"] });
  // an other fish with no pool (a plain row in full; a pooled one with no card is a list line, as today: T670 K4)
  const WSG = rowOf({ kind: "release", win: 3, members: ["WSG"], all_members: ["WSG"] });

  it("covered rows are the table's; two or more others are the list", () => {
    expect(splitRows(cardOf([KO, TROUT_ROW, BASS]), [1])).toEqual({ main: [], rest: [0, 2] });
    expect(listRows(cardOf([KO, TROUT_ROW, BASS]), [1])).toEqual([0, 2]);
  });
  it("a lone other fish stays as today: drawn in full, no list", () => {
    expect(splitRows(cardOf([TROUT_ROW, WSG]), [0])).toEqual({ main: [1], rest: null });
    expect(listRows(cardOf([TROUT_ROW, WSG]), [0])).toBeNull();
    expect(listRows(cardOf([TROUT_ROW, BASS]), [0])).toEqual([1]);
  });
  it("Nahwitti: a pool-less release row the table covers is drawn by nothing else (no plain row, no list line)", () => {
    const card = cardOf([TROUT_ROW, CHAR, WSG]);
    expect([splitRows(card, [0, 1]), listRows(card, [0, 1]), looseRows(card, [0, 1])]).toEqual([{ main: [2], rest: null }, null, []]);
    // mutation: the old split (no covers) draws the char row in full beside the trout card
    expect(splitRows(card).main).toContain(1);
  });
  it("no covered row (the cell has no table): every row drawn as today; the switch off: TROUTISH, unchanged", () => {
    expect(splitRows(cardOf([KO, BASS]), [])).toEqual({ main: [0, 1], rest: null });
    expect(splitRows(cardOf([KO, TROUT_ROW, BASS]))).toEqual({ main: [1], rest: [0, 2] });
  });
  it("a closed day: no rows", () => {
    expect(splitRows(cardOf([TROUT_ROW, KO, BASS], { status: "closed" }), [0])).toEqual({ main: [], rest: null });
  });
});

describe("keepOrder and the fold row", () => {
  const none = { strip: false, inherited: false, banner: false, caveats: [], rows: [], otherFish: false, possession: false, whereNot: false };
  it("one trout block, where the trout rows stood: before the full rows", () => {
    expect(keepOrder({ ...none, strip: true, trout: true, rows: [{ plain: 0 }], otherFish: true }))
      .toEqual(["strip", "trout", { plain: 0 }, "otherFish"]);
    expect(keepOrder({ ...none, strip: true })).not.toContain("trout");
  });
  it("the trout fold: value, tone and date as shipped (the no-count rule lifted for trout only)", () => {
    const t = { boxes: [{ rows: [{ fish: "DV" }, { fish: "RB" }] }],
                fold: { name: TROUT_NAV.ENTRY, value: "Up to 2 or 5", tone: "none", date: "until Mar 31" } } as unknown as TroutVM;
    expect(troutGlance(t)).toEqual({ source: "bc", thumb: "RB", name: "Trout and char", value: "Up to 2 or 5", tone: "none", date: "until Mar 31" });
    expect(troutGlance({ ...t, fold: { ...t.fold, date: null } })).not.toHaveProperty("date");
  });
});
