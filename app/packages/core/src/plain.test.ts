/**
 * The words a reader who has never met a percentile is given.
 *
 * Tested because this is the layer most likely to drift into saying something the
 * arithmetic did not: a phrasing that rounds the wrong way, or sounds sure where the
 * interval is 40 points wide.
 */
import { describe, expect, it } from "vitest";
import { confidenceWord, inTen, plainStanding, seasonPhrase, standing } from "./flow";
import { probit } from "./trust";

describe("saying a percentile in plain words", () => {
  it("compares rather than ranks", () => {
    // The comparison is the part that means something, and the part a percentile hides.
    expect(inTen(0.2)).toBe("lower than 8 days in 10 at this time of year");
    expect(inTen(0.8)).toBe("higher than 8 days in 10 at this time of year");
  });

  it("does not claim a precision the transfer error rules out", () => {
    // ±11.7 points at best, so tenths are the finest honest grain: 0.34 and 0.31 are the
    // same claim and must read as the same claim.
    expect(inTen(0.34)).toBe(inTen(0.31));
  });

  it("says the extremes as extremes, not as a count", () => {
    // "lower than 10 days in 10" is a sentence nobody says.
    expect(inTen(0.01)).toMatch(/almost every day/);
    expect(inTen(0.99)).toMatch(/almost every day/);
  });

  it("hands the middle to whichever side is shorter to say", () => {
    expect(inTen(0.5)).toBe("lower than 5 days in 10 at this time of year");
  });

  it("gives a headline in words a person would use", () => {
    expect(plainStanding(standing(0.03))).toBe("Very low for the time of year");
    expect(plainStanding(standing(0.5))).toBe("About normal for the time of year");
    expect(plainStanding(standing(null))).toBe("No record to compare against");
  });

  it("sounds less sure as the interval widens", () => {
    // Calibrated to the measurement: the best donor is out by 11.7 points, so ±12 is as
    // good as this method gets.
    expect(confidenceWord(11.7)).toBe("fairly confident");
    expect(confidenceWord(16)).toBe("roughly");
    expect(confidenceWord(22)).toBe("very roughly");
  });

  it("names the season the way a reader would", () => {
    expect(seasonPhrase(new Date(2026, 8, 2))).toBe("early September");
    expect(seasonPhrase(new Date(2026, 5, 15))).toBe("mid June");
    expect(seasonPhrase(new Date(2026, 0, 31))).toBe("late January");
  });
});

describe("the normal quantile", () => {
  /** The bisection this replaced — kept here as the reference it is checked against. */
  function bisect(p: number): number {
    const cdf = (z: number) => {
      const t = 1 / (1 + (0.3275911 * Math.abs(z)) / Math.SQRT2);
      const y = 1 - (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t
                       - 0.284496736) * t + 0.254829592) * t) * Math.exp(-(z * z) / 2);
      return z >= 0 ? 0.5 * (1 + y) : 0.5 * (1 - y);
    };
    const q = Math.min(0.999, Math.max(0.001, p));
    let lo = -6, hi = 6;
    for (let i = 0; i < 60; i++) {
      const mid = (lo + hi) / 2;
      if (cdf(mid) < q) lo = mid; else hi = mid;
    }
    return (lo + hi) / 2;
  }

  it("agrees with sixty rounds of bisection across the whole range", () => {
    // The closed form exists because the map runs this over every reach in the viewport on
    // every pan — 5.15 ms against 0.23 ms for 5,000 reaches of three donors. It is only
    // allowed to be faster if it is also the same answer.
    let worst = 0;
    for (let i = 1; i < 999; i++) worst = Math.max(worst, Math.abs(bisect(i / 1000) - probit(i / 1000)));
    expect(worst).toBeLessThan(1e-4);
  });

  it("is symmetric about the median", () => {
    for (const p of [0.01, 0.1, 0.25, 0.4]) expect(probit(p)).toBeCloseTo(-probit(1 - p), 6);
    expect(probit(0.5)).toBeCloseTo(0, 9);
  });

  it("gives the textbook quantiles", () => {
    expect(probit(0.975)).toBeCloseTo(1.959964, 4);
    expect(probit(0.95)).toBeCloseTo(1.644854, 4);
    expect(probit(0.16)).toBeCloseTo(-0.994458, 4);
  });

  it("does not blow up at the ends", () => {
    for (const p of [0, 1, -1, 2, NaN]) expect(Number.isFinite(probit(p)) || Number.isNaN(p))
      .toBe(true);
  });
});
