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
import { isIsoDay, shiftDay, wallIso } from "./day";
import { MONTHS, WEEKDAY_NAMES, monthAbbr, weekdayAbbr } from "./flow";

/** The picker's words: one table, so the map's button, the sheet and its cells say the same thing. */
export const DAY_PICKER = {
  today: "Today",
  /** the map's calendar button, said aloud */
  buttonLabel: (said: string) => `Map coloured for ${said}. Choose a day`,
  /** the sheet's first line: the day the map and the page read */
  showing: (said: string) => `Showing ${said}`,
  todaySaid: "today",
  /** (T213) the map's day strip (Conditions' horizon pattern): Today · Tomorrow · a calendar segment for any other day */
  strip: "Map date",
  tomorrow: "Tomorrow",
  tomorrowSaid: "tomorrow",
  pick: "Date",
  chipLabel: (said: string) => `Colour the map for ${said}`,
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
  /** the one month table (`MONTHS`, flow.ts) */
  months: MONTHS,
  monthsShort: MONTHS.map((_, i) => monthAbbr(i)),
  /** Sunday first, as Canadian calendars print them: the one weekday table (`WEEKDAY_NAMES`, flow.ts) */
  weekdays: WEEKDAY_NAMES.map((w, i) => [weekdayAbbr(i), w] as const),
} as const;

/** The days the picker offers, as LOCAL ISO days ("2026-01-01"); `until` is the release's cut when it made one. */
export interface DayRange { readonly year: number; readonly first: string; readonly last: string; readonly until: string | null }

export function dayRange(now: Date, validUntil: string | null): DayRange {
  const year = now.getFullYear();
  const first = `${year}-01-01`, dec31 = `${year}-12-31`, today = wallIso(now);
  const cut = validUntil && isIsoDay(validUntil) && validUntil >= today && validUntil < dec31 ? validUntil : null;
  return { year, first, last: cut ?? dec31, until: cut };
}

const isoOf = (d: Date) => wallIso(d);
export const inRange = (r: DayRange, d: Date): boolean => { const i = isoOf(d); return i >= r.first && i <= r.last; };
/** The same local date, year included. */
const sameDate = (a: Date, b: Date) => isoOf(a) === isoOf(b);

/** "Saturday, June 15" */
export function fullDay(d: Date): string {
  return `${DAY_PICKER.weekdays[d.getDay()]![1]}, ${DAY_PICKER.months[d.getMonth()]} ${d.getDate()}`;
}

/** The map's calendar button: "Today", or the day it shows ("Sat, Jun 15"). */
export function dayButtonVM(day: Date | null, now: Date): { text: string; label: string } {
  if (!day || sameDate(day, now)) return { text: DAY_PICKER.today, label: DAY_PICKER.buttonLabel(DAY_PICKER.todaySaid) };
  return { text: `${DAY_PICKER.weekdays[day.getDay()]![0]}, ${DAY_PICKER.monthsShort[day.getMonth()]} ${day.getDate()}`,
           label: DAY_PICKER.buttonLabel(fullDay(day)) };
}

/** "Showing today, Wednesday, October 21" / "Showing Saturday, June 13" */
export function showingLine(day: Date | null, now: Date): string {
  const d = day ?? now;
  return DAY_PICKER.showing(sameDate(d, now) ? DAY_PICKER.todayFull(fullDay(d)) : fullDay(d));
}

/** The day a pick lands on: today is null (so "Today" follows midnight), any other day its local noon (`mapDayOf`). */
export function chooseDay(d: Date, now: Date): Date | null {
  return sameDate(d, now) ? null : new Date(d.getFullYear(), d.getMonth(), d.getDate(), 12);
}

/** `n` days (or months) from the day shown; undefined when that leaves the range (the step is refused, not clamped). */
export function stepDay(day: Date | null, n: number, now: Date, range: DayRange, months = false): Date | null | undefined {
  const next = shiftDay(day ?? now, n, months);
  return inRange(range, next) ? chooseDay(next, now) : undefined;
}

/**
 * (T874) The day the walkthrough's Next picks on "Pick another day": the day after the one shown, or, on the range's last
 * day, the day before (so Next always picks one); undefined only when neither is in the range.
 */
export function guideDay(day: Date | null, now: Date, range: DayRange): Date | null | undefined {
  const after = stepDay(day, 1, now, range);
  return after !== undefined ? after : stepDay(day, -1, now, range);
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
  /** Sunday-first weeks, always `WEEKS_SHOWN` of seven (a cell's index is its weekday); null is a blank before the 1st or after the last */
  readonly weeks: readonly (readonly (DayCell | null)[])[];
  /** why days near here are greyed out, or null when the whole month and both neighbours are open */
  readonly reason: string | null;
  /**
   * (T915, owner 2026-11-19: "calendar still jumpy in size from january to other dates") THE NOTE'S ROOM: the longest
   * `reason` any month of the range says. The calendar always holds the room this takes (drawn unseen, unread) and draws
   * this month's reason in it, so the note coming and going never changes the calendar's height.
   */
  readonly reasonRoom: string;
}

/** (T877) the grid's weeks in every month: the most any month spans, so the grid's height never changes between months */
const WEEKS_SHOWN = 6;

export function monthVM(year: number, month: number, chosen: Date | null, now: Date, range: DayRange): MonthVM {
  const firstOf = new Date(year, month, 1, 12);
  const days = new Date(year, month + 1, 0).getDate();
  const cells: (DayCell | null)[] = Array.from({ length: firstOf.getDay() }, () => null);
  const shown = chosen ?? now;
  for (let n = 1; n <= days; n++) {
    const d = new Date(year, month, n, 12);
    const iso = isoOf(d);
    const today = sameDate(d, now), isChosen = sameDate(d, shown);
    const disabled = iso < range.first || iso > range.last;
    cells.push({ iso, n, today, chosen: isChosen, disabled,
                 label: fullDay(d) + (today ? DAY_PICKER.cellToday : "") + (isChosen ? DAY_PICKER.cellChosen : "")
                        + (disabled ? DAY_PICKER.cellOff : "") });
  }
  // (T877, owner 2026-11-18: "The calendar shifts around") ALWAYS six weeks: a month spans 4 to 6, and a grid that
  // followed it changed the sheet's height (and moved the page behind it) on every step between months
  while (cells.length < WEEKS_SHOWN * 7) cells.push(null);
  const weeks = Array.from({ length: WEEKS_SHOWN }, (_, w) => cells.slice(w * 7, w * 7 + 7));
  const { prev, next, reason } = monthEdges(year, month, range);
  return {
    year, month, title: `${DAY_PICKER.months[month]} ${year}`, prev, next,
    weekdays: DAY_PICKER.weekdays.map(([short, long]) => ({ short, long })),
    weeks, reason, reasonRoom: reasonRoom(range),
  };
}

/** Whether a month has a neighbour in the range each way, and why days near it are greyed out (null: none are). */
function monthEdges(year: number, month: number, range: DayRange): { prev: boolean; next: boolean; reason: string | null } {
  const prev = isoOf(new Date(year, month, 0, 12)) >= range.first;
  const next = isoOf(new Date(year, month + 1, 1, 12)) <= range.last;
  // a day of the month before the range / after it (the 1st and the last day are the month's earliest and latest)
  const offBefore = isoOf(new Date(year, month, 1, 12)) < range.first;
  const offAfter = isoOf(new Date(year, month + 1, 0, 12)) > range.last;
  const reasons: string[] = [];
  if (offBefore || !prev) reasons.push(DAY_PICKER.outsideYear(range.year));
  if (offAfter || !next) {
    const [, m, d] = (range.until ?? "").split("-").map(Number);
    const r = range.until ? DAY_PICKER.afterUntil(`${DAY_PICKER.months[m! - 1]} ${d}`) : DAY_PICKER.outsideYear(range.year);
    if (!reasons.includes(r)) reasons.push(r);
  }
  return { prev, next, reason: reasons.length ? reasons.join(" ") : null };
}

/** (T915) the longest reason any month the calendar can show says: the range's year's twelve and the one each side */
function reasonRoom(range: DayRange): string {
  let room = "";
  for (let mo = -1; mo <= 12; mo++) {
    const at = new Date(range.year, mo, 1, 12);
    const r = monthEdges(at.getFullYear(), at.getMonth(), range).reason ?? "";
    if (r.length > room.length) room = r;
  }
  return room;
}
