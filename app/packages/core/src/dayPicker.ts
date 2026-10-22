/**
 * THE DAY PICKER (T49, owner 2026-10-21 "could just be a calendar icon"): the words and the month grid of the ONE day the
 * map is coloured for and the Regulations page reads (the Shell's `mapDay`, null = today; decisions D7). Two entry
 * points draw this one picker — the map's calendar button and the regs header's date pill — so they say the same words
 * and refuse the same days.
 *
 * THE RANGE. The regulations ship as ONE calendar year (366 reading days, `calendar.generated`, no year in the data), so
 * a date in another year would be read as the same month and day of this one — silently wrong. The picker offers today's
 * calendar year only, cut earlier where the release states the last day its regulations are in force
 * (`regulations.valid_until`), and says why the other days are greyed out. Today itself is always offered.
 *
 * Pure: every function takes `now`, and a day is the reader's LOCAL calendar day (`day.ts`).
 */
import { localDate, shiftDay } from "./day";

/** The picker's words: one table, so the map's button, the sheet and its cells say the same thing. */
export const DAY_PICKER = {
  today: "Today",
  /** the map's calendar button, said aloud */
  buttonLabel: (said: string) => `Map coloured for ${said}. Choose a day`,
  /** the sheet's first line: the day the map and the page read */
  showing: (said: string) => `Showing ${said}`,
  todaySaid: "today",
  todayFull: (full: string) => `today, ${full}`,
  prevMonth: "Previous month",
  nextMonth: "Next month",
  /** the quick steps: tomorrow / yesterday without the grid */
  earlier: "‹ Day before",
  later: "Day after ›",
  earlierLabel: "One day earlier",
  laterLabel: "One day later",
  todayLabel: "Back to today",
  /** a cell's label suffixes */
  cellToday: ", today",
  cellChosen: ", chosen",
  cellOff: ", not available",
  /** the grid, said aloud: what it is and how a keyboard moves in it */
  gridLabel: (month: string) => `${month}. Arrow keys move the day; Page Up and Page Down move the month`,
  /** why a day is greyed out */
  outsideYear: (year: number) => `Only days in ${year} can be chosen: the regulations here are one calendar year.`,
  afterUntil: (day: string) => `Days after ${day} can't be chosen: these regulations end then.`,
  months: ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October",
           "November", "December"],
  monthsShort: ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"],
  /** Sunday first, as Canadian calendars print them */
  weekdays: [["Sun", "Sunday"], ["Mon", "Monday"], ["Tue", "Tuesday"], ["Wed", "Wednesday"], ["Thu", "Thursday"],
             ["Fri", "Friday"], ["Sat", "Saturday"]],
} as const;

/** The days the picker offers, as LOCAL ISO days ("2026-01-01"); `until` is the release's cut when it made one. */
export interface DayRange { readonly year: number; readonly first: string; readonly last: string; readonly until: string | null }

const ISO_DAY = /^\d{4}-\d{2}-\d{2}$/;

export function dayRange(now: Date, validUntil: string | null): DayRange {
  const year = now.getFullYear();
  const first = `${year}-01-01`, dec31 = `${year}-12-31`, today = localDate(now.getTime());
  const cut = validUntil && ISO_DAY.test(validUntil) && validUntil >= today && validUntil < dec31 ? validUntil : null;
  return { year, first, last: cut ?? dec31, until: cut };
}

const isoOf = (d: Date) => localDate(d.getTime());
export const inRange = (r: DayRange, d: Date): boolean => { const i = isoOf(d); return i >= r.first && i <= r.last; };
const sameDay = (a: Date, b: Date) => isoOf(a) === isoOf(b);

/** "Saturday, June 15" */
export function fullDay(d: Date): string {
  return `${DAY_PICKER.weekdays[d.getDay()]![1]}, ${DAY_PICKER.months[d.getMonth()]} ${d.getDate()}`;
}

/** The map's calendar button: "Today", or the day it shows ("Sat, Jun 15"). */
export function dayButtonVM(day: Date | null, now: Date): { text: string; label: string } {
  if (!day || sameDay(day, now)) return { text: DAY_PICKER.today, label: DAY_PICKER.buttonLabel(DAY_PICKER.todaySaid) };
  return { text: `${DAY_PICKER.weekdays[day.getDay()]![0]}, ${DAY_PICKER.monthsShort[day.getMonth()]} ${day.getDate()}`,
           label: DAY_PICKER.buttonLabel(fullDay(day)) };
}

/** "Showing today, Wednesday, October 21" / "Showing Saturday, June 13" */
export function showingLine(day: Date | null, now: Date): string {
  const d = day ?? now;
  return DAY_PICKER.showing(sameDay(d, now) ? DAY_PICKER.todayFull(fullDay(d)) : fullDay(d));
}

/** The day a pick lands on: today is null (so "Today" follows midnight), any other day its local noon (`mapDayOf`). */
export function chooseDay(d: Date, now: Date): Date | null {
  return sameDay(d, now) ? null : new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12);
}

/** `n` days (or months) from the day shown; undefined when that leaves the range (the step is refused, not clamped). */
export function stepDay(day: Date | null, n: number, now: Date, range: DayRange, months = false): Date | null | undefined {
  const next = shiftDay(day ?? now, n, months);
  return inRange(range, next) ? chooseDay(next, now) : undefined;
}

export interface DayCell {
  readonly iso: string;
  readonly n: number;
  /** in words: "Saturday, June 15", then ", today" / ", chosen" / ", not available" */
  readonly label: string;
  readonly today: boolean;
  readonly chosen: boolean;
  readonly disabled: boolean;
}

export interface MonthVM {
  readonly year: number;
  /** 0..11 */
  readonly month: number;
  /** "October 2026" */
  readonly title: string;
  /** whether the month before / after has any day in the range */
  readonly prev: boolean;
  readonly next: boolean;
  readonly weekdays: readonly { short: string; long: string }[];
  /** Sunday-first weeks; null is a blank before the 1st or after the last */
  readonly weeks: readonly (readonly (DayCell | null)[])[];
  /** why days near here are greyed out, or null when the whole month and both neighbours are open */
  readonly reason: string | null;
}

export function monthVM(year: number, month: number, chosen: Date | null, now: Date, range: DayRange): MonthVM {
  const firstOf = new Date(year, month, 1, 12);
  const days = new Date(year, month + 1, 0).getDate();
  const cells: (DayCell | null)[] = Array.from({ length: firstOf.getDay() }, () => null);
  const shown = chosen ?? now;
  let offBefore = false, offAfter = false;
  for (let n = 1; n <= days; n++) {
    const d = new Date(year, month, n, 12);
    const iso = isoOf(d);
    const today = sameDay(d, now), isChosen = sameDay(d, shown);
    const disabled = iso < range.first || iso > range.last;
    if (iso < range.first) offBefore = true;
    if (iso > range.last) offAfter = true;
    cells.push({ iso, n, today, chosen: isChosen, disabled,
                 label: fullDay(d) + (today ? DAY_PICKER.cellToday : "") + (isChosen ? DAY_PICKER.cellChosen : "")
                        + (disabled ? DAY_PICKER.cellOff : "") });
  }
  while (cells.length % 7) cells.push(null);
  const weeks = Array.from({ length: cells.length / 7 }, (_, w) => cells.slice(w * 7, w * 7 + 7));
  const prev = isoOf(new Date(year, month, 0, 12)) >= range.first;
  const next = isoOf(new Date(year, month + 1, 1, 12)) <= range.last;
  const reasons: string[] = [];
  if (offBefore || !prev) reasons.push(DAY_PICKER.outsideYear(range.year));
  if (offAfter || !next) {
    const [, m, d] = (range.until ?? "").split("-").map(Number);
    const r = range.until ? DAY_PICKER.afterUntil(`${DAY_PICKER.months[m! - 1]} ${d}`) : DAY_PICKER.outsideYear(range.year);
    if (!reasons.includes(r)) reasons.push(r);
  }
  return {
    year, month, title: `${DAY_PICKER.months[month]} ${year}`, prev, next,
    weekdays: DAY_PICKER.weekdays.map(([short, long]) => ({ short, long })),
    weeks, reason: reasons.length ? reasons.join(" ") : null,
  };
}
