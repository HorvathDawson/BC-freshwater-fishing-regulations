/**
 * (T946) The T892 sweep's rail reader (`conformance/src/regs/tributaryEvidence.ts` `downWater`) reads back core's
 * `DECIDE.K_DOWN` — both its forms. T919 a named a row whose own name says "tributaries" once ("Lake Revelstoke's
 * Tributaries", not "… Tributaries · its tributaries"); the reader knew only "<water> · its tributaries", so the sweep
 * failed every such row's line (18 Lake Revelstoke tributaries' DV, Iverson Creek's whitefish and grayling, …).
 */
import { describe, expect, it } from "vitest";
import { DECIDE as D } from "../packages/core/src/regs/decidedWords";
import { downWater } from "../conformance/src/regs/tributaryEvidence";

describe("downWater reads K_DOWN back (T946)", () => {
  it("names the water of either form", () => {
    for (const name of ["Kootenay River", "Lake Revelstoke's Tributaries", "Chilko Lake's tributary streams",
      "Rancheria River's Tributaries"])
      expect(downWater(D.K_DOWN(name))).toBe(name);
  });
  it("names none for every other kicker", () => {
    for (const k of [D.K_BC, D.K_FEDERAL, D.K_PART, D.K_BASE("Region 4"), D.K_AREA("Zone B"),
      D.K_WATER("Lake Revelstoke's Tributaries"), D.K_LISTED("Lake Revelstoke's Tributaries")])
      expect(downWater(k)).toBeNull();
  });
});
