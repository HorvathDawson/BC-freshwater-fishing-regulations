/**
 * Days, in the reader's own time zone (P12b).
 *
 * Two screens chose days with their own arithmetic: the map's day picker (`DayCalendar`; its grid and words are
 * `dayPicker.ts`) and the visit step of adding a spot (`SpotCapture`). Both are here now, beside the words for them, so a component draws what this returns.
 *
 * LOCAL, NOT UTC, throughout. A spot fished at 6 p.m. in Vancouver on the 30th is in UTC on the 31st, and the list
 * used to say so; the reader's day is the only day a reader means.
 */
import { BC_TIME_LABEL, bcOffsetHours } from "./clock";
import { reportOnce } from "./failure";
import { monthAbbr, weekdayAbbr } from "./flow";

const DAY_MS = 86_400_000;

/**
 * IS THIS A DAY, "YYYY-MM-DD" — the one test (T125 item 8): the day picker's release cut, a gauge's last report and its
 * short stamp each had their own `/^\d{4}-\d{2}-\d{2}$/`, one of them also asking `Date.parse`. Both, here: the shape and
 * a date the platform can read.
 */
export function isIsoDay(s: unknown): s is string {
  return typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s) && Number.isFinite(Date.parse(s));
}
/** "07": a clock or calendar field, two digits. */
export const pad2 = (n: number): string => String(n).padStart(2, "0");

/** The same month and day (a regulations `Day`, or any {month, day}): the year is not compared. */
export const sameDay = (a: { readonly month: number; readonly day: number },
                        b: { readonly month: number; readonly day: number }): boolean =>
  a.month === b.month && a.day === b.day;

/**
 * 1..366: the CALENDAR day of the year of an ISO date or timestamp ("2026-03-01" is 60, and 61 in a leap year) — the
 * axis of a station's year (`gaugeSeries`) and every seasonal question about flow. NOT `readingDayOf`
 * (statusIndex.ts), the day whose regulations a date reads, on which Mar 1 is 61 in every year: the two differ by a
 * day from March in a common year, so they must never be merged. Not a date: 1.
 */
export function calendarDayOfYear(iso: string): number {
  const y = Number(iso.slice(0, 4));
  const m = Number(iso.slice(5, 7));
  const d = Number(iso.slice(8, 10));
  if (!y || !m || !d) return 1;
  return Math.round((Date.UTC(y, m - 1, d) - Date.UTC(y, 0, 1)) / 86_400_000) + 1;
}

/** "Oct 10" */
export function dayLabel(d: Date): string {
  return `${monthAbbr(d.getMonth())} ${d.getDate()}`;
}

/** The day `n` days (or, with `months`, months) after `d`; a month step keeps the day, clamped. */
export function shiftDay(d: Date, n: number, months = false): Date {
  const x = new Date(d.getFullYear(), d.getMonth(), d.getDate());
  if (!months) {
    x.setDate(x.getDate() + n);
    return x;
  }
  const day = x.getDate();
  x.setDate(1);
  x.setMonth(x.getMonth() + n);
  const last = new Date(x.getFullYear(), x.getMonth() + 1, 0).getDate();
  x.setDate(Math.min(day, last));
  return x;
}

/**
 * (owner 2026-11-04: "spots should use the same date/time as everything else") EVERY DAY AND HOUR AN INSTANT IS SAID IN
 * IS B.C.'S (`clock.ts`): a spot's visit day and hour, its weather and flow days, a reading's stamp — never the phone's
 * own calendar, so a visit, the regulations' day and the Conditions times always agree.
 * `bcWall(ms)`: the instant as B.C.'s wall clock, read with the UTC getters.
 */
const HOUR_MS = 3_600_000;
const bcWall = (ms: number): Date => new Date(ms + bcOffsetHours(ms) * HOUR_MS);
/** The instant at B.C. wall time `wall` (ms, as if UTC). */
function fromBcWall(wall: number): number {
  const guess = wall + 8 * HOUR_MS;
  return wall - bcOffsetHours(guess) * HOUR_MS;
}

/** "2026-08-30", the B.C. day of an instant. */
export function localDate(ms: number): string {
  const d = bcWall(ms);
  return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
}

/**
 * "2026-08-30" of a WALL-CLOCK date: a Date whose own fields already hold the B.C. day (`bcNow`, `shiftDay`, a picked
 * day) — read as they are. Never `localDate(d.getTime())` on one: that would move it through the B.C. clock twice.
 */
export function wallIso(d: Date): string {
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

/** The instant a B.C. day ("YYYY-MM-DD") begins. */
export function bcMidnight(isoDay: string): number {
  const [y, m, d] = isoDay.split("-").map(Number) as [number, number, number];
  return fromBcWall(Date.UTC(y, m - 1, d));
}

/** (T551) The instant the NEXT B.C. day begins after `ms`: when "today" rolls over. */
export function nextBcMidnight(ms: number): number {
  const w = bcWall(ms);
  return fromBcWall(Date.UTC(w.getUTCFullYear(), w.getUTCMonth(), w.getUTCDate() + 1));
}

/**
 * (T551) An instant as B.C.'s calendar and clock fields (month 1–12, hour 0–23, 24-hour): the one reading of a stamp's
 * parts, so a line that words a date or time never reaches for the device's `getHours()`.
 */
export function bcFields(ms: number): { year: number; month: number; day: number; hour: number; minute: number } {
  const w = bcWall(ms);
  return { year: w.getUTCFullYear(), month: w.getUTCMonth() + 1, day: w.getUTCDate(), hour: w.getUTCHours(),
           minute: w.getUTCMinutes() };
}

/**
 * (T551) THE B.C. WORDS FOR AN INSTANT, one set for the app, the gauge card (worker) and the gauge page (browser), so a
 * reading is never stamped on two different wall clocks (the card read `America/Vancouver` from tz data, which turns back
 * to UTC−8 on 1 Nov 2026; the app's clock is UTC−7 all year, `clock.ts`).
 *   bcDayWords    "Nov 9", or "Mon, Nov 9" with the weekday
 *   bcWeekdayAbbr "Mon"
 *   bcStampWords  "Nov 9, 11:04 a.m. PT"
 */
export function bcDayWords(ms: number, weekday = false): string {
  const w = bcWall(ms);
  return `${weekday ? `${weekdayAbbr(w.getUTCDay())}, ` : ""}${monthAbbr(w.getUTCMonth())} ${w.getUTCDate()}`;
}
export const bcWeekdayAbbr = (ms: number): string => weekdayAbbr(bcWall(ms).getUTCDay());
export function bcStampWords(ms: number): string {
  const f = bcFields(ms);
  return `${bcDayWords(ms)}, ${f.hour % 12 || 12}:${pad2(f.minute)} ${f.hour < 12 ? "a.m." : "p.m."} ${BC_TIME_LABEL}`;
}
/** "Mon, Nov 9": a CALENDAR day ("YYYY-MM-DD"), said as that day (parsed at B.C. noon, so no zone moves it). */
export function bcIsoDayWords(isoDay: string): string {
  return bcDayWords(bcMidnight(isoDay) + 12 * HOUR_MS, true);
}

/** The B.C. hour of an instant, 0–23. */
export function bcHour(ms: number): number {
  return bcWall(ms).getUTCHours();
}

/** "2026-08-30 at 07:05", the B.C. day and minute of an instant. */
export function localDateTime(ms: number): string {
  const d = bcWall(ms);
  return `${localDate(ms)} at ${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}`;
}

/**
 * "2026-09-03 at 23:00 PDT" — an instant in the reader's own clock, NAMED (P16).
 *
 * Every time on the Conditions screen is said this one way: the same day-and-minute as `localDateTime`, plus the
 * zone it is in, so a reading's "observed" and a forecast's "issued" can be read against each other and against the
 * reader's watch. They printed UTC, which is right and which nobody standing on a river is keeping.
 *
 * `zone` is for tests (an IANA name); left out, it is B.C.'s clock, labelled "PT" (owner 2026-11-04: one date/time
 * everywhere). With a zone and no `Intl` zone names the label falls back to the offset ("UTC−7").
 */
export function localStamp(ms: number, zone?: string): string {
  const z = zonedParts(ms, zone);
  return `${z.y}-${pad2(z.mo)}-${pad2(z.d)} at ${pad2(z.h)}:${pad2(z.mi)} ${z.tz}`;
}

/** An instant's calendar and clock in `zone` (B.C.'s when left out), with the zone's short label. */
interface ZonedParts { y: number; mo: number; d: number; h: number; mi: number; tz: string }

function zonedParts(ms: number, zone?: string): ZonedParts {
  if (zone === undefined) {
    const w = bcWall(ms);
    return { y: w.getUTCFullYear(), mo: w.getUTCMonth() + 1, d: w.getUTCDate(), h: w.getUTCHours(), mi: w.getUTCMinutes(),
             tz: BC_TIME_LABEL };
  }
  try {
    const parts = new Intl.DateTimeFormat("en-CA", {
      timeZone: zone, year: "numeric", month: "2-digit", day: "2-digit",
      hour: "2-digit", minute: "2-digit", hourCycle: "h23", timeZoneName: "short",
    }).formatToParts(new Date(ms));
    const part = (t: string) => parts.find((p) => p.type === t)?.value;
    const [y, mo, d, h, mi, tz] = ["year", "month", "day", "hour", "minute", "timeZoneName"].map(part);
    if (y && mo && d && h && mi && tz) return { y: +y, mo: +mo, d: +d, h: +h, mi: +mi, tz };
    reportOnce("intl-zone", new Error(`Intl gave no zone parts${zone ? ` for ${zone}` : ""}`));
  } catch (e) {
    // No Intl, or no zone data: the device's own clock and its offset, below. (T368, owner Q6) Reported once a run.
    reportOnce("intl-zone", e);
  }
  const x = new Date(ms);
  const off = -x.getTimezoneOffset();
  const hh = Math.floor(Math.abs(off) / 60), mm = Math.abs(off) % 60;
  const tz = off === 0 ? "UTC" : `UTC${off < 0 ? "−" : "+"}${hh}${mm ? `:${pad2(mm)}` : ""}`;
  return { y: x.getFullYear(), mo: x.getMonth() + 1, d: x.getDate(), h: x.getHours(), mi: x.getMinutes(), tz };
}

/**
 * "14 Oct, 09:00 PDT" — an instant SHORT, for a time said inside a line of other words (a silent gauge's last data).
 * Local and labelled like `localStamp` (P16); the year is named only when it is not `ref`'s year in the same zone.
 */
export function shortStamp(ms: number, ref: number, zone?: string): string {
  const z = zonedParts(ms, zone);
  const year = z.y === zonedParts(ref, zone).y ? "" : ` ${z.y}`;
  return `${z.d} ${monthAbbr(z.mo - 1)}${year}, ${pad2(z.h)}:${pad2(z.mi)} ${z.tz}`;
}

/**
 * "14 Oct" — a CALENDAR DAY ("YYYY-MM-DD"), said as that day in every zone (parsed as an instant it is UTC midnight,
 * the evening before on this coast); the year only when it is not `ref`'s year in `zone`.
 */
export function shortDay(isoDay: string, ref: number, zone?: string): string {
  const [y, m, d] = isoDay.split("-").map(Number) as [number, number, number];
  return `${d} ${monthAbbr(m - 1)}${y === zonedParts(ref, zone).y ? "" : ` ${y}`}`;
}

/**
 * The `n` LOCAL days before a visit, oldest first — "the week leading up to it" (S1).
 *
 * The visit day itself is NOT one of them: its hour already has the weather window and the reading, and half a day
 * of daily totals would read as a whole one.
 */
export function leadUpDays(visitedAt: number, n = 7): string[] {
  const noon = startOfDay(visitedAt) + 12 * HOUR_MS;
  return Array.from({ length: n }, (_, i) => localDate(noon + (i - n) * DAY_MS));
}

/** The instant B.C.'s day holding `ms` began. */
const startOfDay = (ms: number): number => {
  const w = bcWall(ms);
  return fromBcWall(Date.UTC(w.getUTCFullYear(), w.getUTCMonth(), w.getUTCDate()));
};

/**
 * The visit moved by `n` days, or the visit unchanged when that would be ahead of `now`.
 *
 * REFUSED, NOT CLAMPED: the archive has nothing for tomorrow, and a spot recording a visit that has not happened is
 * not a spot.
 */
export function shiftVisitDay(at: number, n: number, now: number): number {
  const next = fromBcWall(bcWall(at).getTime() + n * DAY_MS);
  return next > now ? at : next;
}

/** The visit at hour `h` of its own (B.C.) day, on the hour; never later than `now`. */
export function setVisitHour(at: number, h: number, now: number): number {
  const w = bcWall(at);
  return Math.min(fromBcWall(Date.UTC(w.getUTCFullYear(), w.getUTCMonth(), w.getUTCDate(), h)), now);
}

/** What the visit step shows and which of its steppers may be pressed. */
export interface VisitSteps {
  /** 0 today, -1 yesterday, … */
  dayOffset: number;
  laterDay: boolean;
  earlierHour: boolean;
  laterHour: boolean;
  /** The visit's B.C. hour, 0-23. */
  hour: number;
  /** "09:00" */
  hourLabel: string;
  /** "Sun Aug 30 2026" */
  dayText: string;
}

export function visitSteps(at: number, now: number): VisitSteps {
  const w = bcWall(at);
  const h = w.getUTCHours();
  const dayOffset = Math.round((startOfDay(at) - startOfDay(now)) / DAY_MS);
  return {
    dayOffset,
    laterDay: dayOffset < 0,
    earlierHour: h > 0,
    laterHour: h < 23 && setVisitHour(at, h + 1, Infinity) <= now,
    hour: h,
    hourLabel: `${pad2(h)}:00`,
    // Date#toDateString's shape, on the B.C. clock; the names are the one table (flow.ts)
    dayText: `${weekdayAbbr(w.getUTCDay())} ${monthAbbr(w.getUTCMonth())} ${pad2(w.getUTCDate())} ${w.getUTCFullYear()}`,
  };
}

/**
 * The days a visit is almost always on — the drive home, the evening after, the next morning — as one tap each.
 * Deliberately not a calendar: the steppers handle the rest.
 */
export const VISIT_QUICK_DAYS: readonly (readonly [string, number])[] =
  [["Today", 0], ["Yesterday", -1], ["2 days ago", -2]];

