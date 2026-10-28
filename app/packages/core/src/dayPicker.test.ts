/**
 * T49 — the day picker's words and grid (the map's calendar button, and the date sheet both the map and the Regulations
 * page open). Pure: every function takes `now`, and every date is built from LOCAL parts, so the expectations hold in
 * any time zone the suite runs in.
 */
import { describe, expect, it } from "vitest";
import { chooseDay, DAY_PICKER, dayButtonVM, dayRange, inRange, mapDayChips, monthVM, showingLine, stepDay } from "./dayPicker";

const NOW = new Date(2026, 9, 21, 9, 30);            // Wed Oct 21 2026, local
const YEAR = dayRange(NOW, null);
const iso = (d: Date | null | undefined) => (d ? `${d.getFullYear()}-${d.getMonth() + 1}-${d.getDate()} ${d.getHours()}h` : String(d));
/** A month as the reader sees it: one line a week, `[n]` the chosen day, `(n)` today, `-n` a day that cannot be chosen. */
const draw = (m: ReturnType<typeof monthVM>) => m.weeks.map((w) => w.map((c) =>
  !c ? "  ." : c.chosen ? `[${c.n}]` : c.today ? `(${c.n})` : c.disabled ? `-${c.n}` : String(c.n).padStart(3)).join(" "));

describe("the range the data covers: one calendar year, cut at the release's last day when it states one", () => {
  it("no last day stated: Jan 1 to Dec 31 of today's year", () => {
    expect(YEAR).toEqual({ year: 2026, first: "2026-01-01", last: "2026-12-31", until: null });
  });
  it("a stated last day inside the year cuts it; one after the year or before today does not", () => {
    expect(dayRange(NOW, "2026-11-30")).toEqual({ year: 2026, first: "2026-01-01", last: "2026-11-30", until: "2026-11-30" });
    expect(dayRange(NOW, "2027-03-31")).toEqual({ year: 2026, first: "2026-01-01", last: "2026-12-31", until: null });
    // a release whose last day has passed still reads today (the menu's Data sources page says how old it is)
    expect(dayRange(NOW, "2026-04-01")).toEqual({ year: 2026, first: "2026-01-01", last: "2026-12-31", until: null });
    expect(dayRange(NOW, "not a day")).toEqual(YEAR);
  });
  it("inRange: the first and last days are in, a day either side is out", () => {
    expect([new Date(2025, 11, 31), new Date(2026, 0, 1), new Date(2026, 11, 31, 23, 59), new Date(2027, 0, 1)]
      .map((d) => inRange(YEAR, d))).toEqual([false, true, true, false]);
  });
});

describe("(T213) the map's day strip: Today · Tomorrow · a calendar segment, in Conditions' corner", () => {
  const say = (day: Date | null, r = YEAR) => mapDayChips(day, NOW, r)
    .map((c) => `${c.on ? "[" : ""}${c.chip}${c.on ? "]" : ""}${c.enabled ? "" : " (off)"}`).join(" | ");
  it("today: Today is on and the calendar says Date", () => {
    expect(say(null)).toBe("[Today] | Tomorrow | Date");
  });
  it("tomorrow: Tomorrow is on; its press sets tomorrow at local noon", () => {
    expect(say(new Date(2026, 9, 22, 12))).toBe("Today | [Tomorrow] | Date");
    expect(iso(mapDayChips(null, NOW, YEAR)[1]!.day)).toBe("2026-10-22 12h");
  });
  it("another day: the calendar segment names it and is on", () => {
    expect(say(new Date(2026, 5, 15, 12))).toBe("Today | Tomorrow | [Mon, Jun 15]");
    expect(mapDayChips(new Date(2026, 5, 15, 12), NOW, YEAR)[2]!.label).toBe("Map coloured for Monday, June 15. Choose a day");
  });
  it("tomorrow past the release's last day is refused, not clamped", () => {
    expect(say(null, { ...YEAR, last: "2026-10-21", until: "2026-10-21" })).toBe("[Today] | Tomorrow (off) | Date");
  });
});

describe("the map's calendar button: 'Today' or the day it shows", () => {
  it("today (null, or a date that is today) says Today; another day its short weekday and date", () => {
    expect(dayButtonVM(null, NOW)).toEqual({ text: "Today", label: "Map coloured for today. Choose a day" });
    expect(dayButtonVM(new Date(2026, 9, 21, 23), NOW)).toEqual({ text: "Today", label: "Map coloured for today. Choose a day" });
    expect(dayButtonVM(new Date(2026, 5, 15, 12), NOW))
      .toEqual({ text: "Mon, Jun 15", label: "Map coloured for Monday, June 15. Choose a day" });
  });
  it("the sheet's line says the day in full", () => {
    expect([showingLine(null, NOW), showingLine(new Date(2026, 5, 13, 12), NOW)])
      .toEqual(["Showing today, Wednesday, October 21", "Showing Saturday, June 13"]);
  });
});

describe("the month grid", () => {
  it("October 2026 on today (null): today is the chosen day; Sunday first, blanks before the 1st, every day labelled in words", () => {
    const m = monthVM(2026, 9, null, NOW, YEAR);
    expect([m.title, m.prev, m.next, m.reason]).toEqual(["October 2026", true, true, null]);
    expect(m.weekdays.map((w) => `${w.short}/${w.long}`).join(" "))
      .toBe("Sun/Sunday Mon/Monday Tue/Tuesday Wed/Wednesday Thu/Thursday Fri/Friday Sat/Saturday");
    expect(draw(m)).toEqual([
      "  .   .   .   .   1   2   3",
      "  4   5   6   7   8   9  10",
      " 11  12  13  14  15  16  17",
      " 18  19  20 [21]  22  23  24",
      " 25  26  27  28  29  30  31",
    ]);
    const cells = m.weeks.flat().filter((c) => c !== null);
    expect([cells[0]!.label, cells[20]!.label, cells[30]!.label, cells[20]!.iso])
      .toEqual(["Thursday, October 1", "Wednesday, October 21, today, chosen", "Saturday, October 31", "2026-10-21"]);
  });
  it("another chosen day is selected, and today is still marked", () => {
    const m = monthVM(2026, 9, new Date(2026, 9, 10, 12), NOW, YEAR);
    expect(draw(m)[1]).toBe("  4   5   6   7   8   9 [10]");
    expect(draw(m)[3]).toBe(" 18  19  20 (21)  22  23  24");
    expect(m.weeks.flat().filter((c) => c?.chosen).map((c) => c!.label)).toEqual(["Saturday, October 10, chosen"]);
  });
  it("January: no month before it, and the reason is said", () => {
    const m = monthVM(2026, 0, null, NOW, YEAR);
    expect([m.title, m.prev, m.next, m.reason])
      .toEqual(["January 2026", false, true, "Only days in 2026 can be chosen: the regulations here are one calendar year."]);
  });
  it("a release cut mid-year: the days after it are disabled, labelled so, and the reason names the last day", () => {
    const r = dayRange(NOW, "2026-11-20");
    const m = monthVM(2026, 10, null, NOW, r);
    expect([m.prev, m.next, m.reason]).toEqual([true, false, "Days after November 20 can't be chosen: these regulations end then."]);
    expect(draw(m)).toEqual([
      "  1   2   3   4   5   6   7",
      "  8   9  10  11  12  13  14",
      " 15  16  17  18  19  20 -21",
      "-22 -23 -24 -25 -26 -27 -28",
      "-29 -30   .   .   .   .   .",
    ]);
    expect(m.weeks[2]![6]!.label).toBe("Saturday, November 21, not available");
  });
});

describe("choosing and stepping", () => {
  it("chooseDay: today is null (so 'Today' follows midnight); any other day is its local noon", () => {
    expect(chooseDay(new Date(2026, 9, 21, 3), NOW)).toBeNull();
    expect(iso(chooseDay(new Date(2026, 5, 15), NOW))).toBe("2026-6-15 12h");
  });
  it("stepDay: a day or a month either way; onto today is null; off the range is refused (undefined)", () => {
    expect(iso(stepDay(null, 1, NOW, YEAR))).toBe("2026-10-22 12h");
    expect(iso(stepDay(new Date(2026, 9, 22, 12), -1, NOW, YEAR))).toBe("null");
    expect(iso(stepDay(new Date(2026, 0, 31, 12), 1, NOW, YEAR, true))).toBe("2026-2-28 12h");
    expect(iso(stepDay(new Date(2026, 11, 31, 12), 1, NOW, YEAR))).toBe("undefined");
    expect(iso(stepDay(new Date(2026, 0, 1, 12), -1, NOW, YEAR))).toBe("undefined");
  });
  it("the words are one table", () => {
    expect([DAY_PICKER.today, DAY_PICKER.earlierLabel, DAY_PICKER.laterLabel, DAY_PICKER.prevMonth, DAY_PICKER.nextMonth,
            DAY_PICKER.todayLabel])
      .toEqual(["Today", "One day earlier", "One day later", "Previous month", "Next month", "Back to today"]);
  });
});
