/**
 * S1: the arithmetic under a spot's one-week lead-up — which seven days, and where a day's mean sat in its envelope.
 *
 * Timestamps are built from LOCAL parts, so the days mean the same thing in any time zone the suite runs in.
 */
import { describe, expect, it } from "vitest";
import { leadUpDays } from "./day";
import { placeInBand, type Band } from "./flow";

describe("leadUpDays", () => {
  it("is the seven local days BEFORE the visit, oldest first — never the visit day itself", () => {
    expect(leadUpDays(new Date(2026, 7, 30, 9).getTime())).toEqual([
      "2026-08-23", "2026-08-24", "2026-08-25", "2026-08-26", "2026-08-27", "2026-08-28", "2026-08-29"]);
  });

  it("crosses a month and a year as calendar days", () => {
    expect(leadUpDays(new Date(2026, 0, 3, 23, 30).getTime())).toEqual([
      "2025-12-27", "2025-12-28", "2025-12-29", "2025-12-30", "2025-12-31", "2026-01-01", "2026-01-02"]);
  });
});

describe("placeInBand", () => {
  const band: Band = [10, 20, 40, 60, 100];

  it("reads a value on a knot as that knot's percentile", () => {
    expect([10, 20, 40, 60, 100].map((v) => placeInBand(v, band))).toEqual([0.1, 0.25, 0.5, 0.75, 0.9]);
  });

  it("interpolates between knots", () => {
    expect(placeInBand(30, band)).toBe(0.375);
    expect(placeInBand(80, band)).toBe(0.825);
  });

  it("says only WHICH SIDE outside the envelope: it has no knot below p10 or above p90 to measure by", () => {
    expect(placeInBand(9.9, band)).toBe("below");
    expect(placeInBand(101, band)).toBe("above");
  });

  it("is null with no value or no envelope — never a guess", () => {
    expect(placeInBand(null, band)).toBeNull();
    expect(placeInBand(30, null)).toBeNull();
    expect(placeInBand(Number.NaN, band)).toBeNull();
  });

  it("does not divide by zero on a flat stretch of envelope", () => {
    expect(placeInBand(20, [10, 20, 20, 60, 100])).toBe(0.25);
  });
});
