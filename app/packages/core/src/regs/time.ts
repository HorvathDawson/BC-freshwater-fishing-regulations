/**
 * Time words — a rule's dates, weekdays and hours as page v36 says them. Replaces page v36's `CALENDAR.months` (MON)
 * with the generated calendar's MONTHS.
 *
 * Ports (page_v36.js @ 7fb6809f, inventory names): fmtMd, whenDates, rangeTxt, timeCond, winTxt.
 */
import { MONTHS } from "../calendar.generated";
import type { ClockTime } from "../answers.generated";
import { WORDS } from "./words";

/** MMDD, page v36's day key (Jul 4 = 704). Feb 29 (229) is never produced. */
export type Md = number;

/** A rule's `fields.when` (the catalogue's season/time field). Not the licence `When` of the answers file. */
export interface RuleWhen {
  readonly dates?: readonly { readonly from_month: number; readonly from_day: number; readonly to_month: number; readonly to_day: number }[];
  readonly weekdays?: readonly string[];
  readonly hours?: { readonly start: ClockTime; readonly end: ClockTime } | null;
}

export const fmtMd = (md: Md): string => `${MONTHS[Math.floor(md / 100) - 1]} ${md % 100}`;

/** A rule's date windows as [from, to] MMDD pairs (a window may wrap the year: [901, 515]). */
export function whenDates(w: RuleWhen | null | undefined): [Md, Md][] {
  return (w?.dates ?? []).map((d) => [d.from_month * 100 + d.from_day, d.to_month * 100 + d.to_day]);
}

/** "Jul 4", "Jul 4–15", "Jul 4–Aug 2". */
export function rangeTxt(a: Md, b: Md): string {
  if (a === b) return fmtMd(a);
  return Math.floor(a / 100) === Math.floor(b / 100) ? `${fmtMd(a)}–${b % 100}` : `${fmtMd(a)}–${fmtMd(b)}`;
}

const clock = (x: ClockTime): string => x.at
  ? x.at
  : `${x.offset_min ? `${Math.abs(x.offset_min)} ${x.offset_min < 0 ? WORDS.MIN_BEFORE : WORDS.MIN_AFTER}` : ""}${x.solar}`;

/** The weekdays and hours a rule holds on, e.g. "Sat, Sun" or "60 min after sunset–60 min before sunrise"; "" for none. */
export function timeCond(w: RuleWhen | null | undefined): string {
  if (!w) return "";
  const bits: string[] = [];
  if (w.weekdays?.length) bits.push(w.weekdays.map((d) => d.slice(0, 3)).join(", "));
  if (w.hours) bits.push(`${clock(w.hours.start)}–${clock(w.hours.end)}`);
  return bits.join(", ");
}

/** The windows after a comma (", Jun 1–30, Sep 1–May 15"); "" for none. */
export function winTxt(wins: readonly (readonly [Md, Md])[]): string {
  return wins.length ? ", " + wins.map(([a, b]) => rangeTxt(a, b)).join(", ") : "";
}
