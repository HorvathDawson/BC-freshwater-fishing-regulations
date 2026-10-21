/**
 * settle.ts and place.ts, store-free, over hand-built inputs. The 134-state parity against page v36's own settle() is
 * app/conformance/src/regs/settle.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { DisplayFrame, LadderFrameWire, RegsInputs, RuleIndex, RuleView, Statics, VerdictWire } from "./inputs";
import { makePlace } from "./place";
import { broadOf, isClosedOn, settle, settleAt, displayStatusOn } from "./settle";

/** A RuleIndex whose views carry only a rix and a rank (all broadOf reads). */
const index = (ranks: Readonly<Record<number, number>>): RuleIndex => ({
  get: (rix) => ({ rix, rank: ranks[rix] ?? 4 }) as unknown as RuleView,
  has: (rix) => rix in ranks,
  size: Object.keys(ranks).length,
});
const frame = (closing: [number, string[]][]): DisplayFrame => ({ status: "closed", closing, strip: 0 }) as unknown as DisplayFrame;

describe("broadOf: the frame's decided closures, most fish first, then rank", () => {
  it("orders by fish count, then by the part's rank; equal keys keep the frame's order", () => {
    const R = index({ 10: 0, 11: 3, 12: -1, 13: 0 });
    const f = frame([[11, ["RB"]], [10, ["RB", "CT"]], [13, ["RB"]], [12, ["RB"]]]);
    expect(broadOf(f, R).map((r) => r.rix)).toEqual([10, 12, 13, 11]);
  });
  it("is stable: two closures of the same count and rank stay in the frame's order", () => {
    const R = index({ 5: 0, 6: 0 });
    expect(broadOf(frame([[6, ["RB"]], [5, ["CT"]]]), R).map((r) => r.rix)).toEqual([6, 5]);
  });
  it("names only what the frame closes: none for an open frame, a tidal frame or no frame", () => {
    const R = index({ 1: 0 });
    expect(broadOf(frame([]), R)).toEqual([]);
    expect(broadOf({ status: "tidal", tidal: true, note: "", see: [], licence: "" } as DisplayFrame, R)).toEqual([]);
    expect(broadOf(null, R)).toEqual([]);
  });
});

/* ---------- settle over hand-built inputs ---------- */
const V = (speaks: number[], beside: number[] = [], shown: number[] = [], nym: number[] = [],
           lost: [number, number, number][] = []): VerdictWire => [speaks, beside, shown, nym, [], lost];
const NONE_V = V([]);
/** Rules: 1 standing (shown), 2 an angler closure, 3 a beside rule with a time condition, 4 a beside rule on one side,
 *  5 plain, 6 and 7 undrawn-part rules. */
const VIEWS: Record<number, Partial<RuleView>> = {
  1: { kind: "standing" }, 2: { kind: "anglerclosure" }, 3: { tcond: "Sat, Sun" }, 4: { f: { side: "left" } }, 5: {}, 6: {}, 7: {},
};
const R: RuleIndex = {
  get: (rix) => ({ rix, rank: 0, tcond: "", f: {}, kind: "gate", ...VIEWS[rix] }) as unknown as RuleView,
  has: (rix) => rix in VIEWS, size: 7,
};
const SEG0: LadderFrameWire = { common: V([5]), rows: [
  ["RB", V([2], [3, 4], [1], [6]), NONE_V, NONE_V],
  ["CT", V([], [], [5], [], [[1, 0, 9]]), NONE_V, NONE_V],   // 5 speaks in common and is shown on CT: speaks holds; 1 lifted by 9
  ["DV", V([], [], [], [], [[1, 0, 8]]), NONE_V, NONE_V]] }; // and by 8 on DV: the LAST by is kept (page Map.set)
const SEG1: LadderFrameWire = { common: V([5], [], [], [7, 6]), rows: [["RB", NONE_V, NONE_V, NONE_V]] };
/** A frame whose common verdict no fish row reads: page v36 reads verdicts per fish, so it says nothing. */
const NO_FISH: LadderFrameWire = { common: V([5], [1], [2], [6]), rows: [] };
const OPEN_F = { status: "own", closing: [], strip: 1 } as unknown as DisplayFrame;
const SHUT_F = { status: "closed", closing: [[5, ["RB"]], [2, ["RB", "CT"]]], strip: 2 } as unknown as DisplayFrame;
const STATICS = { ladderReasons: ["lifted", "ladder"], ladderReasonState: ["lifted", "displaced"] } as unknown as Statics;

function inputs(o: { key?: boolean } = {}): RegsInputs {
  const key = o.key === false ? null : { akey: 1, tidal: false } as unknown as RegsInputs["key"];
  return {
    day: { month: 6, day: 15 }, s: key ? 0 : null, key, statics: STATICS, rules: R,
    ruleset: key ? { reach: [5, 2], trib: [3] } : null,
    item: null, partIx: 0,
    year: key ? { segStarts: [1, 153], moments: null, cells: [], ladderBySeg: [SEG0, SEG1], rowsBySeg: [null, null],
      displayBySeg: [OPEN_F, SHUT_F], displayByDay: Array.from({ length: 366 }, (_, i) => (i < 152 ? OPEN_F : SHUT_F)) } : null,
  } as unknown as RegsInputs;
}
const ix = (l: readonly RuleView[]) => l.map((r) => r.rix);

describe("settle: the stored verdicts of origin none, grouped for the page", () => {
  it("speaks / beside / shown in first-seen order (a rule speaking for any fish speaks); lifted keeps the last by", () => {
    const inp = inputs(), c = settle(inp, makePlace(inp));
    expect([c.s, ix(c.speaks), ix(c.beside), ix(c.shown)]).toEqual([0, [5, 2], [3, 4], [1]]);
    expect([...c.lifted]).toEqual([[1, 8]]);
    expect(c.status).toBe(OPEN_F);
    expect(c.broad).toEqual([]);
  });

  it("the subsets: timed (beside with a time condition), side, standing (shown), anglerclosed (speaks)", () => {
    const inp = inputs(), c = settle(inp, makePlace(inp));
    expect([ix(c.timed), ix(c.side), ix(c.standing), ix(c.anglerclosed)]).toEqual([[3], [4], [1], [2]]);
  });

  it("inpart: every not_yet_mapped rule over the year, first seen; now = undrawn today", () => {
    const inp = inputs(), p = makePlace(inp);
    expect(settle(inp, p).inpart.map((x) => [x.rule.rix, x.now])).toEqual([[6, true], [7, false]]);
    expect(settleAt(inp, p, 1, { month: 7, day: 1 }).inpart.map((x) => [x.rule.rix, x.now])).toEqual([[6, true], [7, true]]);
  });

  it("settleAt another segment reads that segment's ladder and display frame; broad is its decided closing, ordered", () => {
    const inp = inputs(), c = settleAt(inp, makePlace(inp), 1, { month: 7, day: 1 });
    expect([c.s, c.day, ix(c.speaks), c.status, ix(c.broad)]).toEqual([1, { month: 7, day: 1 }, [5], SHUT_F, [2, 5]]);
  });

  it("a ladder frame with no fish rows: nothing speaks, nothing undrawn (verdicts are read per fish)", () => {
    const inp = { ...inputs() } as RegsInputs;
    (inp as { year: unknown }).year = { ...inp.year!, ladderBySeg: [NO_FISH, NO_FISH] };
    const c = settle(inp, makePlace(inp));
    expect([c.speaks, c.beside, c.shown, c.inpart]).toEqual([[], [], [], []]);
  });

  it("is memoised per inputs, segment and day", () => {
    const inp = inputs(), p = makePlace(inp);
    expect(settle(inp, p)).toBe(settle(inp, p));
    expect(settleAt(inp, p, 1, { month: 7, day: 1 })).not.toBe(settleAt(inp, p, 1, { month: 7, day: 2 }));
  });

  it("a place with no key (outside B.C.): empty lists, no status, s -1 — never throws", () => {
    const inp = inputs({ key: false }), c = settle(inp, makePlace(inp));
    expect([c.s, c.L, c.status, c.speaks, c.beside, c.shown, c.inpart, [...c.lifted], c.broad])
      .toEqual([-1, null, null, [], [], [], [], [], []]);
  });

  it("displayStatusOn / isClosedOn read the year's display frame of the day", () => {
    const inp = inputs();
    expect([displayStatusOn(inp, 152), displayStatusOn(inp, 153), isClosedOn(inp, 152), isClosedOn(inp, 153)]).toEqual(["own", "closed", false, true]);
    expect([displayStatusOn(inputs({ key: false }), 153), isClosedOn(inputs({ key: false }), 153)]).toEqual([null, false]);
  });
});

describe("makePlace", () => {
  it("the key, the rules, the ruleset's candidates (reach then trib), the year's starts and moments, tidal", () => {
    const inp = inputs(), p = makePlace(inp);
    expect([p.key, p.R, ix(p.cands), p.starts, p.moments, p.part, p.tidal]).toEqual([inp.key, R, [5, 2, 3], [1, 153], null, null, false]);
    const out = makePlace(inputs({ key: false }));
    expect([out.key, out.cands, out.starts, out.moments, out.tidal]).toEqual([null, [], [], null, false]);
  });
});
