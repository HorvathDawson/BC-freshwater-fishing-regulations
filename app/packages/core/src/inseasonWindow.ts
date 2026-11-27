/**
 * (T1167, owner 2026-11-25 "2 weeks before to the end (Recommended)") WHEN AN IN-SEASON NOTICE IS SHOWN: the range it
 * states in its own text, read with its year (`inSeasonDating`); when it is in force (`inSeasonSpan`); and its banner on a
 * day (`inSeasonOn`): UPCOMING the `INSEASON_LEAD_DAYS` before a dated notice's first day, IN FORCE from its first day
 * through its last, then nothing. Its own module, with no other import than the emitted constant, so the feeds worker
 * reads THIS rule (to tell the owner of a range it cannot date, `undatableEvent`) and never a copy of it (AGENTS 40).
 * The Python twin is `pipeline.regs.inseason.matches.dating` / `span` / `on`; ONE case file pins both
 * (`inseason-span.parity.json`). Banner only: the answer and the map's colour stay the book's ("Banner only, but louder").
 */
import { INSEASON_LEAD_DAYS } from "./feedFormats.generated";

/** what the rule reads of a feed row: its effective dates and the worker's reading of its text */
export interface InSeasonDatable {
  readonly from?: unknown;
  readonly to?: unknown;
  readonly parsed?: unknown;
}

/** "2026-07-30" moved `n` days (UTC arithmetic on a calendar day: no clock, no time zone) */
export function addDays(day: string, n: number): string {
  const d = new Date(`${day}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + n);
  return d.toISOString().slice(0, 10);
}
/** a calendar day as ISO, or null when the month has no such day */
function isoOf(y: number, m: number, d: number): string | null {
  const t = new Date(Date.UTC(y, m - 1, d));
  return t.getUTCFullYear() === y && t.getUTCMonth() === m - 1 && t.getUTCDate() === d ? t.toISOString().slice(0, 10) : null;
}
/** a "YYYY-MM-DD" that is a real day: read back through `isoOf`, so the shape is not spelled a second time (the one
 * day test, `isIsoDay` in day.ts, drags the clock into the feeds worker; this module takes no import but its constant) */
const isDay = (v: unknown): v is string =>
  typeof v === "string" && v.length === 10 && isoOf(Number(v.slice(0, 4)), Number(v.slice(5, 7)), Number(v.slice(8))) === v;
/** a whole number (JSON has one number type: 7 and 7.0 are one value; true is none) */
const whole = (n: unknown): n is number => typeof n === "number" && Number.isSafeInteger(n);
const isObject = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

/** the range a notice states in its own text, with its year (`inSeasonDating`) */
export type InSeasonDating =
  | { readonly kind: "dated"; readonly from: string; readonly to: string }
  | { readonly kind: "undated" }
  | { readonly kind: "unreadable"; readonly why: string };

/** why a notice's range could not be dated: what the feeds worker tells the owner (`undatableEvent`) */
export const INSEASON_UNDATED = {
  noEnds: "its range has no whole start and end",
  noEffective: "it has no effective day to give its range a year",
  backwards: "its range ends before it starts in the same month",
  noSuchDay: "its range names a day the month does not have",
  notOnEffective: "its range does not open on its effective day, so its year cannot be read",
} as const;

/**
 * THE RANGE A NOTICE STATES IN ITS OWN TEXT, read with its year. The text's one range is the feeds worker's
 * `parsed.window` (month and day, its `parseChange`: a text with no range, or several, has none; a time of day — "from
 * 12:00 pm to midnight", "to 11:59 pm September 15" — is never a day of it). Its YEAR is read, never guessed: the range
 * must open on the notice's own effective day (`from`: "July 30, 2026" with "No fishing July 30 to September 15"), and it
 * ends on the next last day on or after the first (a range over the new year ends the year after: "Dec 1 to Feb 29" from
 * Dec 1, 2027 ends Feb 29, 2028). A rescind's quoted range is the rescinded rule's, not the notice's: undated. Anything
 * else that prints a range — not a whole start and end, an end before the start in the same month (never read as a year
 * on), a day the month does not have, no effective day, or a range that does not open on it ("August 15 to December 31"
 * posted Sept 16, a yearly closure) — is UNREADABLE: shown as undated, and the feeds worker tells the owner.
 */
export function inSeasonDating(row: InSeasonDatable): InSeasonDating {
  const parsed = isObject(row.parsed) ? row.parsed : null;
  const w = parsed?.window;
  if (w === undefined || w === null) return { kind: "undated" };
  const kinds = parsed!.kinds;
  if (Array.isArray(kinds) && kinds.includes("rescind")) return { kind: "undated" };
  if (!isObject(w)) return { kind: "unreadable", why: INSEASON_UNDATED.noEnds };
  const { from_month: fm, from_day: fd, to_month: tm, to_day: td } = w;
  if (!whole(fm) || !whole(fd) || !whole(tm) || !whole(td)) return { kind: "unreadable", why: INSEASON_UNDATED.noEnds };
  if (!isDay(row.from)) return { kind: "unreadable", why: INSEASON_UNDATED.noEffective };
  if (tm === fm && td < fd) return { kind: "unreadable", why: INSEASON_UNDATED.backwards };
  const y = Number(row.from.slice(0, 4));
  const wraps = tm < fm;
  const first = isoOf(y, fm, fd), last = isoOf(wraps ? y + 1 : y, tm, td);
  if (!first || !last) return { kind: "unreadable", why: INSEASON_UNDATED.noSuchDay };
  if (first !== row.from) return { kind: "unreadable", why: INSEASON_UNDATED.notOnEffective };
  return { kind: "dated", from: first, to: last };
}

/** When a row is in force, and whether it is DATED (a range in its text, or both ends in its effective cell) */
export interface InSeasonSpan { readonly from: string | null; readonly to: string | null; readonly dated: boolean }

/**
 * When a row is in force: the curated override first (the links' from / to, notice_waters.json `effective`), then its
 * dates. A row is DATED by the range its own text states (`inSeasonDating`) or by an effective cell printing both ends
 * ("July 4, 2025 to August 31, 2025"). When the text and the cell both give an end, the LATER one: the banner may outstay
 * the closure, never leave it early. An undated row keeps the rule of 2026-11-11 (T548, "A notice stops showing when the
 * Province drops it. Never invent an end date"): in force from its `from` until the page drops it.
 */
export function inSeasonSpan(row: InSeasonDatable, link?: { from?: string; to?: string }): InSeasonSpan {
  const d = inSeasonDating(row);
  const cellFrom = isDay(row.from) ? row.from : null, cellTo = isDay(row.to) ? row.to : null;
  const text = d.kind === "dated" ? d : null;
  const end = text && cellTo ? (cellTo > text.to ? cellTo : text.to) : text?.to ?? cellTo;
  return { from: link?.from ?? text?.from ?? cellFrom, to: link?.to ?? end, dated: !!text || (!!cellFrom && !!cellTo) };
}

/** a notice's banner on a day: in force, or upcoming (the lead days before a dated notice's first day) */
export type InSeasonPhase = "upcoming" | "in force";

/**
 * A ROW'S BANNER ON `day` ("YYYY-MM-DD", BC's day, the caller's `bcNow()`): "in force" from its first day through its
 * last (inclusive), "upcoming" in the `INSEASON_LEAD_DAYS` before the first day of a DATED row, null otherwise (not yet,
 * or over).
 */
export function inSeasonOn(span: InSeasonSpan, day: string): InSeasonPhase | null {
  if (span.to && span.to < day) return null;
  if (span.from && span.from > day) return span.dated && addDays(span.from, -INSEASON_LEAD_DAYS) <= day ? "upcoming" : null;
  return "in force";
}
