/**
 * (T583) The standing's words, whole: one percentile said in each named form, by one module. A reader comparing the card,
 * a donor's row, the map dot and the public page's headline meets the same band and the same rank.
 */
import { describe, expect, it } from "vitest";
import { standing } from "./flow";
import {
  bandWord, figure, inTen, percentileSaid, placeChip, placeSaid, standingHeadline, standingLong, standingReads, standingShort,
} from "./standing";

describe("one standing, every form", () => {
  const p = 0.2;
  it("says the 20th percentile in the card's family", () => {
    expect(bandWord(standing(p))).toBe("Below normal");
    expect(standingHeadline(standing(p))).toBe("Below normal for the time of year");
    expect(standingShort(p)).toBe("Below normal · 20th percentile");
    expect(standingLong(p)).toBe("Below normal · 20th percentile for the date");
    expect(standingReads(p)).toBe("Reads below normal · 20th percentile.");
    expect(percentileSaid(p)).toBe("20th percentile");
  });
  it("keeps one decimal in the tail, in every form", () => {
    expect(percentileSaid(0.004)).toBe("0.4th percentile");
    expect(standingShort(0.004)).toBe("Much below normal · 0.4th percentile");
  });
  it("says every band", () => {
    expect([0.05, 0.2, 0.5, 0.8, 0.95].map((x) => standingShort(x))).toEqual([
      "Much below normal · 5th percentile", "Below normal · 20th percentile", "Normal · 50th percentile",
      "Above normal · 80th percentile", "Much above normal · 95th percentile"]);
    expect(standingHeadline("no-record")).toBe("No record to compare against");
    expect(standingShort(null)).toBeNull();
    expect(standingLong(null)).toBe("There is a reading here, but no record to compare it against.");
  });
  it("says a day's place, edges included", () => {
    expect(placeChip("below")).toBe("≤10th percentile");
    expect(placeChip(0.63)).toBe("63rd percentile");
    expect(placeSaid("above", "day")).toBe("at or above the 90th percentile for the day");
    expect(placeSaid(0.63, "date")).toBe("the 63rd percentile for the date");
  });
  it("keeps the count out of ten for the estimate's sentence", () => {
    expect(inTen(0.2)).toBe("lower than 8 days in 10 at this time of year");
  });
  it("rounds a figure to three figures, once", () => {
    expect([1234.5, 123.4, 12.34, 1.548, 0.3349].map(figure)).toEqual(["1235", "123", "12.3", "1.55", "0.33"]);
  });
});
