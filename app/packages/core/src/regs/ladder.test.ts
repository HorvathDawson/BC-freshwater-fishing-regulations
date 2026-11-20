/**
 * ladder.ts, store-free: the wire verdicts decoded as page v36's verdictList / ladderAt decode them. The reasons and their
 * states are the store's `static ladder.reasons` / `ladder.reason_state`, and verdict 0 the store's `ladder_verdict` 0
 * (regs store 8af9a94bc53dd773, copied from the data). The 134-state parity is conformance/src/regs/settle.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { LadderFrameWire, VerdictWire } from "./inputs";
import { ladderAt, verdictList } from "./ladder";

const REASONS = ["lifted", "ladder", "own_row", "water_dates", "zone_release", "closure", "water_closure", "size_release",
  "water_release", "same_row_release", "moot_size_clause", "stricter_region", "same_as_peer", "nearer_row"];
const REASON_STATE = ["lifted", "displaced", "displaced", "displaced", "displaced", "displaced", "displaced", "displaced",
  "displaced", "displaced", "moot", "displaced", "displaced", "displaced"];
const V0: VerdictWire = [[3079, 3333, 3339, 3355, 3356, 3359], [], [3351, 3352], [], [], [[3364, 1, 3079]]];

describe("verdictList", () => {
  it("1. ladder_verdict 0: six speak, two shown, 3364 lost to 3079 (reason 1: displaced by the ladder)", () => {
    const m = verdictList(V0, REASONS, REASON_STATE);
    expect([...m]).toEqual([
      [3079, { state: "speaks", partly: null }], [3333, { state: "speaks", partly: null }], [3339, { state: "speaks", partly: null }],
      [3355, { state: "speaks", partly: null }], [3356, { state: "speaks", partly: null }], [3359, { state: "speaks", partly: null }],
      [3351, { state: "shown", partly: null }], [3352, { state: "shown", partly: null }],
      [3364, { state: "displaced", partly: null, reason: "ladder", by: 3079 }]]);
  });

  it("every list keeps its state: beside, not_yet_mapped; a lost rule takes its reason's state (lifted, moot)", () => {
    const m = verdictList([[], [5], [], [6], [], [[7, 0, 9], [8, 10, 9]]], REASONS, REASON_STATE);
    expect([...m]).toEqual([[5, { state: "beside", partly: null }], [6, { state: "not_yet_mapped", partly: null }],
      [7, { state: "lifted", partly: null, reason: "lifted", by: 9 }], [8, { state: "moot", partly: null, reason: "moot_size_clause", by: 9 }]]);
  });

  it("2. partly marks a listed rule with its lifting rules, and is ignored for a rule not listed", () => {
    const m = verdictList([[3376, 1], [], [], [], [[3376, [3377, 3378]], [42, [1]]], []], REASONS, REASON_STATE);
    expect([...m]).toEqual([[3376, { state: "speaks", partly: [3377, 3378] }], [1, { state: "speaks", partly: null }]]);
  });
});

describe("ladderAt", () => {
  it("3. each fish × origin is common overlaid by its own verdict; the same verdict gives the same map for all three", () => {
    const common: VerdictWire = [[1, 2], [], [3], [], [], []];
    const own: VerdictWire = [[], [], [], [], [], [[2, 1, 9]]];
    const frame: LadderFrameWire = { common, rows: [["RB", own, own, own], ["CT", [[4], [], [], [], [], []], common, [[], [], [], [], [], [[1, 0, 5]]]]] };
    const L = ladderAt(frame, REASONS, REASON_STATE)!;
    expect([...L.keys()]).toEqual(["RB", "CT"]);
    const rb = L.get("RB")!;
    expect([...rb.none]).toEqual([[1, { state: "speaks", partly: null }], [2, { state: "displaced", partly: null, reason: "ladder", by: 9 }],
      [3, { state: "shown", partly: null }]]);
    expect(rb.hatchery).toEqual(rb.none);
    expect(rb.wild).toEqual(rb.none);
    const ct = L.get("CT")!;
    expect([...ct.none.keys()]).toEqual([1, 2, 3, 4]);
    expect([...ct.hatchery]).toEqual([...verdictList(common, REASONS, REASON_STATE)]);
    expect(ct.wild.get(1)).toEqual({ state: "lifted", partly: null, reason: "lifted", by: 5 });
  });

  it("(T917) origins with the same wire verdict share one map; each origin is the spread overlay, in its order", () => {
    const common: VerdictWire = [[1, 2], [], [3], [], [], []];
    const own: VerdictWire = [[4], [], [], [], [], [[2, 1, 9]]], other: VerdictWire = [[], [], [], [5], [], []];
    const frame: LadderFrameWire = { common, rows: [["RB", own, own, own], ["CT", own, other, own], ["DV", own, other, other]] };
    const L = ladderAt(frame, REASONS, REASON_STATE)!;
    // the decode before T917, written out: `new Map([...base, ...own])` per origin
    const spread = (v: VerdictWire) => [...new Map([...verdictList(common, REASONS, REASON_STATE), ...verdictList(v, REASONS, REASON_STATE)])];
    for (const [fish, none, hatchery, wild] of frame.rows) {
      const got = L.get(fish)!;
      expect([[...got.none], [...got.hatchery], [...got.wild]]).toEqual([spread(none), spread(hatchery), spread(wild)]);
    }
    const rb = L.get("RB")!, ct = L.get("CT")!, dv = L.get("DV")!;
    expect(rb.hatchery).toBe(rb.none);
    expect(rb.wild).toBe(rb.none);
    expect(ct.wild).toBe(ct.none);
    expect(ct.hatchery).not.toBe(ct.none);
    expect(dv.wild).toBe(dv.hatchery);
    // a fish's maps are its own: no other fish's row reaches them
    expect(rb.none).not.toBe(ct.none);
    expect([...ct.none.keys()]).toEqual([1, 2, 3, 4]);
  });

  it("no frame, no ladder", () => {
    expect(ladderAt(null, REASONS, REASON_STATE)).toBeNull();
  });
});
