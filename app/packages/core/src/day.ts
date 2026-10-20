/**
 * Days, in the reader's own time zone (P12b).
 *
 * Two screens chose days with their own arithmetic: the map's day stepper (`MapDay`) and the visit step of adding a
 * spot (`SpotCapture`). Both are here now, beside the words for them, so a component draws what this returns.
 *
 * LOCAL, NOT UTC, throughout. A spot fished at 6 p.m. in Vancouver on the 30th is in UTC on the 31st, and the list
 * used to say so; the reader's day is the only day a reader means.
 */
import { monthAbbr } from "./flow";

const DAY_MS = 86_400_000;
const pad2 = (n: number) => String(n).padStart(2, "0");

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

/** "2026-08-30", the LOCAL day of an instant. */
export function localDate(ms: number): string {
  const d = new Date(ms);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
}

/** "2026-08-30 at 07:05", the LOCAL day and minute of an instant. */
export function localDateTime(ms: number): string {
  const d = new Date(ms);
  return `${localDate(ms)} at ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
}

const startOfDay = (ms: number): number => new Date(ms).setHours(0, 0, 0, 0);

/**
 * The visit moved by `n` days, or the visit unchanged when that would be ahead of `now`.
 *
 * REFUSED, NOT CLAMPED: the archive has nothing for tomorrow, and a spot recording a visit that has not happened is
 * not a spot.
 */
export function shiftVisitDay(at: number, n: number, now: number): number {
  const next = new Date(at);
  next.setDate(next.getDate() + n);
  return next.getTime() > now ? at : next.getTime();
}

/** The visit at hour `h` of its own day, on the hour; never later than `now`. */
export function setVisitHour(at: number, h: number, now: number): number {
  return Math.min(new Date(at).setHours(h, 0, 0, 0), now);
}

/** What the visit step shows and which of its steppers may be pressed. */
export interface VisitSteps {
  /** 0 today, -1 yesterday, … */
  dayOffset: number;
  laterDay: boolean;
  earlierHour: boolean;
  laterHour: boolean;
  /** The visit's local hour, 0-23. */
  hour: number;
  /** "09:00" */
  hourLabel: string;
  /** "Sun Aug 30 2026" */
  dayText: string;
}

export function visitSteps(at: number, now: number): VisitSteps {
  const d = new Date(at);
  const h = d.getHours();
  const dayOffset = Math.round((startOfDay(at) - startOfDay(now)) / DAY_MS);
  return {
    dayOffset,
    laterDay: dayOffset < 0,
    earlierHour: h > 0,
    laterHour: h < 23 && new Date(at).setHours(h + 1, 0, 0, 0) <= now,
    hour: h,
    hourLabel: `${pad2(h)}:00`,
    dayText: d.toDateString(),
  };
}

/**
 * The days a visit is almost always on — the drive home, the evening after, the next morning — as one tap each.
 * Deliberately not a calendar: the steppers handle the rest.
 */
export const VISIT_QUICK_DAYS: readonly (readonly [string, number])[] =
  [["Today", 0], ["Yesterday", -1], ["2 days ago", -2]];
