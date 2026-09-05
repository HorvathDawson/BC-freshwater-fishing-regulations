/**
 * A watershed group's colour, from every gauge standing in it.
 *
 * The field used to name ONE station per group — the largest catchment — which made the
 * group's answer hostage to it. Two ways that showed as a blank group on a river anyone can
 * see is gauged: the elected station had no climatology (13 groups, KISP choosing SKEENA
 * RIVER AT HAZELTON among them), or it simply was not transmitting (Chilliwack, six gauges,
 * grey because the one named was quiet).
 */
import { describe, expect, it } from "vitest";
import { basinStanding, probit, type BasinVote } from "./trust";

const v = (percentile: number, areaKm2: number | null, years = 40): BasinVote =>
  ({ percentile, areaKm2, years });

describe("a watershed group's standing", () => {
  it("answers from one gauge when that is all there is", () => {
    expect(basinStanding([v(0.42, 500)])!).toBeCloseTo(0.42, 6);
  });

  it("stays coloured when the biggest gauge is the one that is quiet", () => {
    /*
     * CHILLIWACK. Six gauges in the group; the one the old code named was not reporting, so
     * the whole group went grey. A quiet station simply does not vote now.
     */
    expect(basinStanding([v(0.8, 1200), v(0.78, 300)])).not.toBeNull();
  });

  it("says nothing when nothing in the group is reporting", () => {
    // Null, not 0.5. "No reading" and "exactly normal" are different claims, and the field
    // draws the first as the same grey a group with no gauge gets.
    expect(basinStanding([])).toBeNull();
  });

  it("lets the gauge that drains more of the group count for more", () => {
    const big = basinStanding([v(0.9, 4000), v(0.1, 40)])!;
    const small = basinStanding([v(0.9, 40), v(0.1, 4000)])!;
    expect(big).toBeGreaterThan(0.5);
    expect(small).toBeLessThan(0.5);
  });

  it("discounts a station whose baseline is barely established", () => {
    /*
     * A percentile is a claim about history, so a three-year record is a weaker claim than
     * a ninety-year one — same shape and ceiling as the reach panel's record factor.
     */
    const shaky = basinStanding([v(0.9, 100, 3), v(0.1, 100, 40)])!;
    expect(shaky).toBeLessThan(0.5);
  });

  it("combines in probit space, not by averaging the percentiles", () => {
    /*
     * THE METRIC. Percentiles are bounded and non-linear — the step from the 50th to the
     * 60th is not the step from the 88th to the 98th — so a plain mean flattens exactly the
     * extremes the map exists to show. Two equal gauges at the 2nd and 98th are symmetric
     * about the median either way, so the discriminating case is an asymmetric pair.
     */
    const got = basinStanding([v(0.5, 100), v(0.99, 100)])!;
    const plainMean = (0.5 + 0.99) / 2;
    expect(got).not.toBeCloseTo(plainMean, 3);
    // It is the transform's own answer, and it agrees with core's probit/normCdf pair.
    expect(probit(got)).toBeCloseTo((probit(0.5) + probit(0.99)) / 2, 6);
  });

  it("still answers when no station in the group records a catchment", () => {
    // Share falls back to an equal split rather than zeroing every weight — otherwise a
    // group of gauges with unknown areas would report nothing at all.
    expect(basinStanding([v(0.3, null), v(0.7, null)])!).toBeCloseTo(0.5, 6);
  });
});
