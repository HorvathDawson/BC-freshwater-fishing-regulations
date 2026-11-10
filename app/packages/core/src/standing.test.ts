/**
 * (T583, T598) The standing's words, whole: one percentile said in each named form, by one module. A reader comparing the
 * card, a donor's row, the map dot and the public page's headline meets the same band and the same rank, in days in 10.
 */
import { describe, expect, it } from "vitest";
import { standing } from "./flow";
import {
  bandWord, daysInTen, daysNear, daysRange, daysShort, figure, inTen, levelChip, levelName, placeChip, placeSaid,
  standingHeadline, standingLong, standingReads, standingShort,
} from "./standing";

describe("one standing, every form", () => {
  const p = 0.2;
  it("says a low rank in the card's family", () => {
    expect(bandWord(standing(p))).toBe("Below normal");
    expect(standingHeadline(standing(p))).toBe("Below normal for the time of year");
    expect(standingShort(p)).toBe("Below normal · lower than 8 days in 10");
    expect(standingLong(p)).toBe("Below normal · lower than 8 days in 10 for the date");
    expect(standingReads(p)).toBe("Reads below normal · lower than 8 days in 10.");
    expect(daysInTen(p)).toBe("lower than 8 days in 10");
    expect(daysShort(p)).toBe("8 in 10 days higher");
    expect(daysNear(p)).toBe("about 8 in 10 days higher");
  });
  it("says every band", () => {
    expect([0.1, 0.2, 0.5, 0.8, 0.9].map((x) => standingShort(x))).toEqual([
      "Below normal · lower than 9 days in 10", "Below normal · lower than 8 days in 10", "Normal · lower than 5 days in 10",
      "Above normal · higher than 8 days in 10", "Much above normal · higher than 9 days in 10"]);
    expect(standingHeadline("no-record")).toBe("No record to compare against");
    expect(standingShort(null)).toBeNull();
    expect(standingLong(null)).toBe("There is a reading here, but no record to compare it against.");
  });
  it("rounds to the nearest tenth, half up, and the short form is the long one turned round", () => {
    const rows: [number, string, string][] = [
      [0.06, "lower than 9 days in 10", "9 in 10 days higher"],
      [0.15, "lower than 9 days in 10", "9 in 10 days higher"],
      [0.16, "lower than 8 days in 10", "8 in 10 days higher"],
      [0.25, "lower than 8 days in 10", "8 in 10 days higher"],
      [0.75, "higher than 8 days in 10", "8 in 10 days lower"],
      [0.5, "lower than 5 days in 10", "5 in 10 days higher"],
      [0.45, "lower than 6 days in 10", "6 in 10 days higher"],
      [0.55, "higher than 6 days in 10", "6 in 10 days lower"],
      [0.8, "higher than 8 days in 10", "8 in 10 days lower"],
      [0.94, "higher than 9 days in 10", "9 in 10 days lower"],
    ];
    for (const [x, long, short] of rows) expect([daysInTen(x), daysShort(x)], String(x)).toEqual([long, short]);
  });
  it("never says 10 days in 10 or 0 days in 10: the ends say almost every day", () => {
    for (const x of [0, 0.004, 0.049, 0.05]) {
      expect(daysInTen(x)).toBe("lower than almost every day on record");
      expect(daysShort(x)).toBe("almost all days higher");
    }
    for (const x of [0.95, 0.951, 0.99, 1]) {
      expect(daysInTen(x)).toBe("higher than almost every day on record");
      expect(daysShort(x)).toBe("almost all days lower");
    }
    for (let i = 0; i <= 100; i++) {
      for (const s of [daysInTen(i / 100), daysShort(i / 100)]) expect(s).not.toMatch(/\b(?:10|0) (?:days )?in 10|\bin 10 days.*\b10 in 10/);
    }
    expect(standingShort(0.004)).toBe("Much below normal · lower than almost every day on record");
  });
  it("says a day's place, edges included", () => {
    expect(placeChip("below")).toBe("9+ in 10 days higher");
    expect(placeChip("above")).toBe("9+ in 10 days lower");
    expect(placeChip(0.63)).toBe("6 in 10 days lower");
    expect(placeSaid("below", "day")).toBe("lower than at least 9 days in 10 for the day");
    expect(placeSaid("above", "day")).toBe("higher than at least 9 days in 10 for the day");
    expect(placeSaid(0.63, "date")).toBe("higher than 6 days in 10 for the date");
  });
  it("keeps the count out of ten for the estimate's sentence", () => {
    expect(inTen(0.2)).toBe("lower than 8 days in 10 at this time of year");
    expect(inTen(0.01)).toBe("lower than almost every day on record for this time of year");
    expect(inTen(0.99)).toBe("higher than almost every day on record for this time of year");
  });
  it("says an interval and an alert level without a rank", () => {
    expect(daysRange(3, 27)).toBe("Lower than 7 to 9 days in 10");
    expect(daysRange(41, 44)).toBe("Lower than 6 days in 10");
    expect(daysRange(60, 80)).toBe("Higher than 6 to 8 days in 10");
    expect(daysRange(1, 99)).toBe("Lower than 9 days in 10 to higher than 9 days in 10");
    expect([levelName(50), levelName(75), levelName(10)]).toEqual([
      "the median", "the level higher than 8 days in 10", "the level lower than 9 days in 10"]);
    expect([levelChip(50), levelChip(25), levelChip(90)]).toEqual(["Median", "8 in 10 days higher", "9 in 10 days lower"]);
  });
  it("rounds a figure to three figures, once", () => {
    expect([1234.5, 123.4, 12.34, 1.548, 0.3349].map(figure)).toEqual(["1235", "123", "12.3", "1.55", "0.33"]);
  });
});
