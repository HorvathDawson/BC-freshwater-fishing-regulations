/**
 * THE TAP (T05): the pure lookups that turn "this day, this angler" into store coordinates.
 *
 *   tapDay(day, year)               → the reading day (1..366, the leap calendar) and the date's REAL weekday
 *   segmentIndex(starts, day, …)    → the segment `s` of a key (`cell(akey, s)`)
 *   daySegments(starts, day)        → every segment sharing the day's start (one unless the key has moments)
 *   profileIndex(who, dims, …)      → the licence profile's index into `static licence.profiles`
 *
 * Ports: `pipeline/deliver/answers/encode.py` `segment_index` and `tap_key` (the reading day); page_v36.js `daySegs`,
 * `segAt` (which returns -1 where this throws: the data never reaches it), `segOf`, `weekdayOf`, `dayOf`.
 *
 * The calendar is THE CALENDAR SPEC (`calendar.generated.ts`, P06/D31), never re-derived here: Feb 29 READS Feb 28's
 * day (59) on its OWN real weekday — Fri 2036-02-29 on the Zymoetz below Limonite Creek reads Feb 28's regulations on a
 * Friday. `readingDay`/`weekdayOf`/`monthLastDay` (its leap-year rule) are that module's; this file only refuses a date
 * that does not exist.
 */
import { CALENDAR_DAYS, DAYS_BEFORE_MONTH, LEAP_DAY, monthLastDay, readingDay, weekdayOf, type Weekday } from "../calendar.generated";
import type { Moment } from "../answers.generated";
import type { Day, Who } from "./model";

/** A segment's moment (`static answers.moments`): the weekdays it holds, and its hours window or null. */
export type { Moment } from "../answers.generated";

export class TapError extends Error {}

/**
 * The tap's day: the leap-calendar day whose regulations `d` READS in `year` (`calendar_spec.reading_day`: Feb 29 → 59)
 * and the date's real weekday (`calendar_spec.weekday_of`). Throws TapError for a date that does not exist
 * (`calendar_spec.month_last_day`) — Feb 29 in a common year, Apr 31, month 13: the date pill never offers one, so it
 * is a programmer error, not a day to guess.
 */
export function tapDay(d: Day, year: number): { readonly day: number; readonly weekday: Weekday } {
  const last = monthLastDay(year, d.month);
  if (!Number.isInteger(year) || !Number.isInteger(d.month) || !Number.isInteger(d.day) || last === undefined || d.day < 1 || d.day > last)
    throw new TapError(`regs: ${year}-${d.month}-${d.day} is not a date`);
  return { day: readingDay(d.month, d.day), weekday: weekdayOf(year, d.month, d.day) };
}

/** The last j with starts[j] <= day (`bisect_right(starts, day) - 1`); starts are sorted and start at 1. */
function bisect(starts: readonly number[], day: number): number {
  if (!(Number.isInteger(day) && day >= 1 && day <= CALENDAR_DAYS)) throw new TapError(`regs: day ${day} is not 1..${CALENDAR_DAYS}`);
  let s = -1;
  for (let j = 0; j < starts.length; j++) if (starts[j]! <= day) s = j; else break;
  if (s < 0) throw new TapError(`regs: no segment starts by day ${day}`);
  return s;
}

/**
 * ANSWERS-SPEC §2 / `encode.segment_index`: s = bisect_right(starts, day) - 1; where the day's start repeats (the key
 * has moments), the moment holding `weekday` and (hours null, or `hours.in === inside`). `day` is the reading day
 * (`tapDay`). Throws TapError where Python raises: day out of range, a repeated start without a weekday, no moment
 * holds the weekday.
 */
export function segmentIndex(starts: readonly number[], day: number, moments: readonly Moment[] | null,
                             weekday: Weekday | null, inside = false): number {
  const s = bisect(starts, day);
  const rep = (s > 0 && starts[s - 1] === starts[s]) || (s + 1 < starts.length && starts[s + 1] === starts[s]);
  if (!moments || !rep) return s;
  if (!weekday) throw new TapError(`regs: day ${day} reads differently by weekday or hour — name one`);
  const first = starts.indexOf(starts[s]!);
  for (let i = first; i <= s; i++) {
    const m = moments[i];
    if (!m) throw new TapError(`regs: no moment ${i} for day ${day}`);
    if (m.weekdays.includes(weekday) && (m.hours === null || m.hours.in === inside)) return i;
  }
  throw new TapError(`regs: no moment of day ${day} holds ${weekday} (inside=${inside})`);
}

/** Every segment sharing the day's start (page_v36 `daySegs`): one unless the key has moments. */
export function daySegments(starts: readonly number[], day: number): readonly number[] {
  const s = bisect(starts, day);
  const out: number[] = [];
  for (let j = starts.indexOf(starts[s]!); j <= s; j++) out.push(j);
  return out;
}

/**
 * The segment of every leap-calendar day 1..366 (index d-1) over `year` — what T09's `displayByDay` reads each day's
 * display frame from, as taps.py regenerated taps.json: each day at ITS OWN weekday in `year`, outside any hours window;
 * day 60 (the leap calendar's Feb 29 slot, which no date reads: Feb 29 reads 59) at its first moment.
 */
export function yearSegments(starts: readonly number[], moments: readonly Moment[] | null, year: number): number[] {
  const out: number[] = [];
  for (let d = 1; d <= CALENDAR_DAYS; d++) {
    if (moments === null) { out.push(segmentIndex(starts, d, null, null)); continue; }
    if (d === LEAP_DAY) { out.push(daySegments(starts, d)[0]!); continue; }
    let m = 11;
    while (DAYS_BEFORE_MONTH[m]! >= d) m--;
    out.push(segmentIndex(starts, d, moments, weekdayOf(year, m + 1, d - DAYS_BEFORE_MONTH[m]!), false));
  }
  return out;
}

/** The app's two ages → the licence's `age` dimension values. */
export const AGE_DIM: Readonly<Record<Who["age"], string>> = { adult: "16_plus", u16: "under_16" };

const DIMS = ["residency", "age", "guidance", "status"] as const;

/**
 * `${residency}/${age}/${guidance}/${status}` in `static licence.profile_dims` order → its index into
 * `static licence.profiles`. Guidance defaults to "non_guided", status to "none" (the app asks neither yet). Throws
 * TapError when the dims are not the four this app knows (in that order), a value is not one the dim lists, or the
 * profile is absent.
 */
export function profileIndex(who: Who, dims: readonly (readonly [string, readonly string[]])[], profiles: readonly string[],
                             extra: { guidance?: "non_guided" | "guided"; status?: string } = {}): number {
  if (dims.length !== DIMS.length || dims.some(([name], i) => name !== DIMS[i]))
    throw new TapError(`regs: licence profile dims are ${JSON.stringify(dims.map(([n]) => n))}, not ${JSON.stringify(DIMS)}`);
  const vals = [who.residency, AGE_DIM[who.age], extra.guidance ?? "non_guided", extra.status ?? "none"];
  vals.forEach((v, i) => {
    if (!dims[i]![1].includes(v as string)) throw new TapError(`regs: ${DIMS[i]} ${JSON.stringify(v)} is not one of ${JSON.stringify(dims[i]![1])}`);
  });
  const key = vals.join("/");
  const ix = profiles.indexOf(key);
  if (ix < 0) throw new TapError(`regs: no licence profile ${key}`);
  return ix;
}
