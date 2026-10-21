/**
 * T06: time words (page_v36 fmtMd, whenDates, rangeTxt, timeCond, winTxt). Month names are the generated calendar's.
 */
import { describe, expect, it } from "vitest";
import { fmtMd, rangeTxt, timeCond, whenDates, winTxt } from "./time";

describe("fmtMd, rangeTxt (T06 test 4)", () => {
  it("says a day as 'Mon D'", () => {
    expect(fmtMd(704)).toBe("Jul 4");
    expect(fmtMd(101)).toBe("Jan 1");
    expect(fmtMd(1231)).toBe("Dec 31");
  });

  it("says one day, a range in one month, and a range across months", () => {
    expect(rangeTxt(601, 630)).toBe("Jun 1–30");
    expect(rangeTxt(704, 715)).toBe("Jul 4–15");
    expect(rangeTxt(704, 802)).toBe("Jul 4–Aug 2");
    expect(rangeTxt(704, 704)).toBe("Jul 4");
    expect(rangeTxt(901, 515)).toBe("Sep 1–May 15");
  });
});

describe("whenDates, winTxt", () => {
  it("reads a rule's dates as [from, to] MMDD pairs, and nothing as none", () => {
    expect(whenDates({ dates: [{ from_month: 9, from_day: 1, to_month: 5, to_day: 15 }, { from_month: 7, from_day: 15, to_month: 8, to_day: 31 }] }))
      .toEqual([[901, 515], [715, 831]]);
    expect(whenDates({ weekdays: ["Saturday"] })).toEqual([]);
    expect(whenDates(null)).toEqual([]);
    expect(whenDates(undefined)).toEqual([]);
  });

  it("says the windows after a comma, or nothing", () => {
    expect(winTxt([[601, 630]])).toBe(", Jun 1–30");
    expect(winTxt([[715, 831], [901, 515]])).toBe(", Jul 15–Aug 31, Sep 1–May 15");
    expect(winTxt([])).toBe("");
  });
});

describe("timeCond (T06 test 5)", () => {
  it("says solar hours with their offsets (rix 576)", () => {
    expect(timeCond({ hours: { start: { offset_min: 60, solar: "sunset" }, end: { offset_min: -60, solar: "sunrise" } } }))
      .toBe("60 min after sunset–60 min before sunrise");
  });

  it("says clock hours as the rule writes them (rix 1539)", () => {
    expect(timeCond({ hours: { start: { at: "12:00", offset_min: 0 }, end: { at: "00:00", offset_min: 0 } } })).toBe("12:00–00:00");
  });

  it("says a solar time with no offset as the bare event", () => {
    expect(timeCond({ hours: { start: { offset_min: 0, solar: "sunrise" }, end: { offset_min: 0, solar: "sunset" } } })).toBe("sunrise–sunset");
  });

  it("says weekdays short (rix 1366), weekdays before hours, and nothing for no condition", () => {
    expect(timeCond({ weekdays: ["Saturday", "Sunday"] })).toBe("Sat, Sun");
    expect(timeCond({ weekdays: ["Friday"], hours: { start: { at: "12:00", offset_min: 0 }, end: { at: "00:00", offset_min: 0 } } }))
      .toBe("Fri, 12:00–00:00");
    expect(timeCond({ dates: [{ from_month: 6, from_day: 1, to_month: 6, to_day: 30 }] })).toBe("");
    expect(timeCond(null)).toBe("");
  });
});
