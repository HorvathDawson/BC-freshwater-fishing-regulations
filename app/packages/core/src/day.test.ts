/**
 * Days, in the reader's own time zone (P12b).
 *
 * Every timestamp here is built from LOCAL parts (`new Date(y, m, d, h)`), so the tests mean the same thing in any
 * time zone the suite runs in — which is the point: a spot fished on the evening of the 30th in Vancouver is the 30th,
 * not the 31st in UTC.
 */
import { describe, expect, it } from "vitest";
import { dayLabel, localDate, localDateTime, setVisitHour, shiftDay, shiftVisitDay, visitSteps,
         VISIT_QUICK_DAYS } from "./day";

const at = (d: number, h = 9, m = 0) => new Date(2026, 7, d, h, m).getTime();   // August 2026, local

describe("the map's day stepper", () => {
  it("steps a day and says it in words", () => {
    expect(dayLabel(shiftDay(new Date(2026, 9, 10), 1))).toBe("Oct 11");
    expect(dayLabel(shiftDay(new Date(2026, 0, 1), -1))).toBe("Dec 31");
  });
  it("steps a month keeping the day, clamped to the month's end", () => {
    expect(dayLabel(shiftDay(new Date(2026, 9, 10), 1, true))).toBe("Nov 10");
    expect(dayLabel(shiftDay(new Date(2026, 0, 31), 1, true))).toBe("Feb 28");
  });
});

describe("local dates", () => {
  it("names the local day, even late in the evening", () => {
    expect(localDate(at(30, 23, 30))).toBe("2026-08-30");
    expect(localDate(at(1, 0, 5))).toBe("2026-08-01");
  });
  it("names the local day and minute", () => {
    expect(localDateTime(at(30, 7, 5))).toBe("2026-08-30 at 07:05");
  });
});

describe("the visit's day and hour, never ahead of now", () => {
  const now = at(30, 14, 20);

  it("refuses a later day that would be in the future, and keeps the visit", () => {
    expect(shiftVisitDay(at(30, 9), 1, now)).toBe(at(30, 9));
    expect(shiftVisitDay(at(28, 9), 1, now)).toBe(at(29, 9));
    expect(shiftVisitDay(at(30, 9), -2, now)).toBe(at(28, 9));
  });

  it("clamps an hour past now to now", () => {
    expect(setVisitHour(at(30, 9), 16, now)).toBe(now);
    expect(setVisitHour(at(30, 9), 6, now)).toBe(at(30, 6));
    expect(setVisitHour(at(29, 9), 16, now)).toBe(at(29, 16));
  });

  it("says what may be stepped, today at 14:20", () => {
    expect(visitSteps(at(30, 14), now)).toEqual({
      dayOffset: 0, laterDay: false, earlierHour: true, laterHour: false, hour: 14,
      hourLabel: "14:00", dayText: new Date(at(30, 14)).toDateString(),
    });
  });

  it("says what may be stepped, two days ago at midnight", () => {
    expect(visitSteps(at(28, 0), now)).toEqual({
      dayOffset: -2, laterDay: true, earlierHour: false, laterHour: true, hour: 0,
      hourLabel: "00:00", dayText: new Date(at(28, 0)).toDateString(),
    });
  });

  it("offers today, yesterday and the day before as one tap each", () => {
    expect(VISIT_QUICK_DAYS).toEqual([["Today", 0], ["Yesterday", -1], ["2 days ago", -2]]);
  });
});
