/**
 * settle.ts, store-free. The 134-state parity against page v36's own settle() is
 * app/conformance/src/regs/settle.parity.test.ts.
 */
import { describe, expect, it } from "vitest";
import type { DisplayFrame, RuleIndex, RuleView } from "./inputs";
import { broadOf } from "./settle";

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
