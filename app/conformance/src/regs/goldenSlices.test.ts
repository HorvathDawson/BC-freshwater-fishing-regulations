/**
 * T23 / T29: the golden slices the RN render tests read are fresh copies of golden.json / pins.json (goldenSlices.ts).
 * No store needed: this never skips. Rewrite with UPDATE_REGS_FIXTURES=1 after a prototype sync.
 */
import { describe, expect, it } from "vitest";
import { pinChain, PIN_LH, PIN_ROW, type PinLine } from "@app/core";
import { headersSlice, pinsSlice, syncSlices } from "./goldenSlices";
import { golden } from "./oracles";

describe("golden slices for the RN render tests", () => {
  it("golden-headers.json and golden-pins.json are fresh (UPDATE_REGS_FIXTURES=1 pnpm -C app exec vitest run conformance/src/regs/goldenSlices)", () => {
    expect(syncSlices()).toEqual([]);
  });

  it("every golden state is in both slices", () => {
    const ids = Object.keys(golden().states).sort();
    expect([Object.keys(headersSlice()), Object.keys(pinsSlice().states)]).toEqual([ids, ids]);
  });

  it("the app coordinates replay every golden bar (header 0 = the prototype's header minus its own height)", () => {
    const P = pinsSlice(), k = (ls: readonly PinLine[]) => JSON.stringify(ls.map((l) => [l.cls, l.text, l.sub]));
    const heightOf = (l: PinLine) => P.heights[`${l.cls}|${l.text}|${l.sub ?? ""}`] ?? (l.sub ? 2 * PIN_LH : PIN_LH);
    let n = 0;
    for (const [id, s] of Object.entries(P.states)) for (const x of s.samples) {
      expect([id, x.y, k(pinChain(s.anchors, x.y, { header: 0, LH: PIN_LH, ROW: PIN_ROW }, heightOf))]).toEqual([id, x.y, k(x.lines)]);
      n++;
    }
    expect(n).toBeGreaterThan(400);
  });
});
