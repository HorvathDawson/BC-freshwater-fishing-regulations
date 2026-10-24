/**
 * T176 — THE CONDITIONS MAP LEGEND SAYS WHAT IT COUNTS. Its histogram is of the reaches on screen with a standing, and
 * on Nicomen Slough it said "13 gauges in view" over 13 coloured reaches (T168 review). Whole strings, every shape.
 */
import { describe, expect, it } from "vitest";
import { legendCount, legendTickLabel } from "./conditions";

describe("legendCount: the reaches the legend's histogram is of, in words", () => {
  it("names reaches, never gauges, singular and plural", () => {
    expect([legendCount(13), legendCount(1), legendCount(2)])
      .toEqual(["13 reaches coloured", "1 reach coloured", "2 reaches coloured"]);
  });
  it("says nothing for none, or for a count not known (rule 32: not asked is not none)", () => {
    expect([legendCount(0), legendCount(null), legendCount(undefined), legendCount(Number.NaN), legendCount(-3)])
      .toEqual([null, null, null, null, null]);
  });
});

describe("legendTickLabel: one histogram tick, for a screen reader", () => {
  it("is the count of reaches and where they stand, whole", () => {
    expect([legendTickLabel(2, "near the 6th percentile"), legendTickLabel(1, "near 4× today's flow")])
      .toEqual(["2 reaches near the 6th percentile", "1 reach near 4× today's flow"]);
  });
});
