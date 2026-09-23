/**
 * Season windows. Half the regulations in the synopsis are seasonal, so a map with no
 * date is wrong for most of the year, and a window that wraps the new year is the case
 * that a naive comparison gets silently backwards.
 *
 * A window is inclusive at both ends and carries no year: "Apr 1 - Jun 30" is the same
 * window every year, and "Oct 15 - Apr 15" wraps.
 */

/** Month is 1-12. Not a Date: a window has no year, and pretending it does invents one. */
export interface MonthDay {
  readonly month: number;
  readonly day: number;
}

export interface Window {
  readonly from: MonthDay;
  readonly to: MonthDay;
}

/** A calendar day, with no time and no zone. Regulations change at midnight local. */
export interface PlainDate {
  readonly year: number;
  readonly month: number;
  readonly day: number;
}

const ord = (m: number, d: number): number => m * 100 + d;

export function isValid({ month, day }: MonthDay): boolean {
  if (!Number.isInteger(month) || !Number.isInteger(day)) return false;
  if (month < 1 || month > 12 || day < 1) return false;
  // 29 Feb is a legitimate window edge; a leap year is not this type's business.
  return day <= [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1]!;
}

/** Does `on` fall inside `w`? Handles windows that wrap the year end. */
export function inWindow(w: Window, on: PlainDate): boolean {
  const from = ord(w.from.month, w.from.day);
  const to = ord(w.to.month, w.to.day);
  const at = ord(on.month, on.day);
  return from <= to ? at >= from && at <= to : at >= from || at <= to;
}

/**
 * Is this rule in force on this date?
 *
 * No windows means all year — NOT "never". Getting that backwards would render every
 * year-round closure as open, which is the one failure with real consequences.
 */
export function inForce(windows: readonly Window[], on: PlainDate): boolean {
  if (windows.length === 0) return true;
  return windows.some((w) => inWindow(w, on));
}

/** A time of day, off the clock ("21:00") or off the sun ("one hour after sunset"). A solar
 *  time needs a date and a latitude to become a clock time, which is a screen's to do. */
export interface Clock {
  readonly at?: string;
  readonly solar?: "sunrise" | "sunset";
  /** Minutes from the solar event. Negative is BEFORE. */
  readonly offsetMin?: number;
}

/** A range within the day. It wraps midnight the way a window wraps the year end. */
export interface Hours {
  readonly start: Clock;
  readonly end: Clock;
}

export type Weekday =
  | "Monday" | "Tuesday" | "Wednesday" | "Thursday" | "Friday" | "Saturday" | "Sunday";
export const WEEKDAYS: readonly Weekday[] =
  ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"] as const;

/**
 * WHEN A RULE BINDS — the catalogue's `When`, said once: the days of the year, the weekdays, the
 * hours of the day, and any season the parser could NOT read.
 *
 * `dates` EMPTY MEANS ALL YEAR, per the synopsis. `unparsed` is the opposite of empty: a season
 * that exists and could not be read, so a rule carrying one is NOT all year — `evaluate` treats
 * it as uncertain, and it can only ever raise "unknown".
 *
 * This replaced `windows`, which the bundle filled from a field the catalogue no longer has and
 * so shipped empty on every rule — every seasonal closure in the province read as all year.
 */
export interface When {
  readonly dates: readonly Window[];
  readonly weekdays: readonly Weekday[];
  readonly hours?: Hours;
  readonly unparsed: readonly string[];
}

/** No season at all: the rule binds every day. */
export const ALL_YEAR: When = { dates: [], weekdays: [], unparsed: [] };

/** The weekday of a calendar day. Computed in UTC so no zone can move it off its date. */
export function weekdayOf({ year, month, day }: PlainDate): Weekday {
  // getUTCDay: 0 = Sunday.
  const d = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
  return WEEKDAYS[(d + 6) % 7]!;
}

/**
 * Does this rule bind on this DAY?
 *
 * Dates and weekdays decide the day. HOURS DO NOT: "No fishing 21:00 to 05:00, Aug 1-Dec 31"
 * binds on 10 August, for part of it — `closesTheWater` is what stops such a rule painting the
 * whole day closed. An UNPARSED season decides nothing here; the caller must treat the rule as
 * uncertain (see `evaluate`), because answering yes or no would both be a guess.
 */
export function holdsOn(when: When, on: PlainDate): boolean {
  if (!inForce(when.dates, on)) return false;
  return when.weekdays.length === 0 || when.weekdays.includes(weekdayOf(on));
}

/**
 * Today, as a calendar day in the reader's own zone.
 *
 * LOCAL, NOT UTC. `new Date().toISOString()` would put anyone in BC onto tomorrow's date
 * for the last 7-8 hours of every day — and a date is what decides whether a seasonal
 * closure is in force, so an off-by-one here opens a river that is shut.
 *
 * The app used to carry `const ON = { year: 2026, month: 8, day: 30 }`, a fixed day
 * transcribed from the design mock, so every answer it gave was about 30 August whatever
 * the actual date. Injectable for tests, which is the only reason it takes an argument.
 */
export function today(now: Date = new Date()): PlainDate {
  return { year: now.getFullYear(), month: now.getMonth() + 1, day: now.getDate() };
}
