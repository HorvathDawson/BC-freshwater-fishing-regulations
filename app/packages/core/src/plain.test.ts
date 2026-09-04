/**
 * The words a reader who has never met a percentile is given.
 *
 * Tested because this is the layer most likely to drift into saying something the
 * arithmetic did not: a phrasing that rounds the wrong way, or sounds sure where the
 * interval is 40 points wide.
 */
import { describe, expect, it } from "vitest";
import { confidenceWord, inTen, plainStanding, seasonPhrase, standing } from "./flow";

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
