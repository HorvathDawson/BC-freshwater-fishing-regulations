/**
 * THE CLOCK (T382; owner 2026-11-04: "unify this so that all clock sources are easier to find later … all get the date
 * from a single source so it is consistent and has no possibility of drift").
 *
 * The ONLY place app code reads the device clock. Two questions, two answers:
 *   an INSTANT   `now()` — epoch ms: ages, timeouts, elapsed time, log and report stamps, probes. The same on any clock.
 *   the DAY      `bcNow()` / `bcToday()` — which day it is, on the B.C. clock (one for the province, UTC−7 all year
 *                from 8 Mar 2026): day pickers, the regulations' day, the map's day, in-season, run timing, visits.
 *
 * `app/tools/clock.test.ts` fails on a `new Date()` with no arguments or a `Date.now()` anywhere else in app source.
 * A function that takes an injected `now` for its tests keeps it; its DEFAULT comes from here.
 */
import { pad2 } from "./day";

/** Now, as epoch ms: an instant (an age, a timeout, a stamp). Never a day — that is `bcNow()`. */
export function now(): number {
  return Date.now();
}

/**
 * (owner 2026-11-04: "bc time makes sense") THE REGULATIONS' CLOCK, ONE FOR THE PROVINCE. A day of the regulations is a
 * B.C. day, so "today" is B.C.'s date wherever the phone is (an Alberta phone near midnight is still on B.C.'s today).
 *
 * B.C. TIME IS UTC−7 ALL YEAR from 8 March 2026 (the province's permanent "Pacific time": the spring-forward that day
 * was the last change; 1 Nov 2026 has none). Before it, Pacific standard / daylight (UTC−8 / −7, second Sunday of March
 * to first Sunday of November, 02:00). WRITTEN HERE, NOT READ FROM THE DEVICE'S ZONE DATA: the tz databases shipping in
 * 2026 (2026a: Node, many phones) still turn `America/Vancouver` back to UTC−8 on 1 Nov 2026, which would put the
 * province's day an hour wrong all winter. The south-east (Golden, part of the East Kootenay) keeps its own clock by
 * local choice; the regulations' day is still the province's. Per-water zones: T383 (later).
 */
export const BC_PERMANENT_FROM = Date.UTC(2026, 2, 8, 10);      // 02:00 PST on 8 Mar 2026: UTC−7 from then on
export const BC_TIME_LABEL = "PT";

/** B.C.'s offset from UTC at an instant, in hours (−7, or −8 under the old standard time before 8 Mar 2026). */
export function bcOffsetHours(ms: number): number {
  if (ms >= BC_PERMANENT_FROM) return -7;
  const y = new Date(ms).getUTCFullYear();
  const sunday = (month: number, nth: number) => {            // the nth Sunday of `month`, 02:00 local, as UTC ms
    const first = new Date(Date.UTC(y, month, 1)).getUTCDay();
    return Date.UTC(y, month, 1 + ((7 - first) % 7) + 7 * (nth - 1));
  };
  const springs = sunday(2, 2) + 10 * 3_600_000;                // 02:00 PST = 10:00 UTC
  const falls = sunday(10, 1) + 9 * 3_600_000;                  // 02:00 PDT = 09:00 UTC
  return ms >= springs && ms < falls ? -7 : -8;
}

/**
 * THE ONE SOURCE OF THE REGULATIONS' DAY: now on the B.C. clock, as a Date whose LOCAL fields (getFullYear …
 * getMinutes) are B.C.'s wall clock — the shape every day-deciding reader takes (`Day`, `dayRange`, `regsDay`). For
 * deciding which DAY it is, never for elapsed time (an instant is `now()`).
 */
export function bcNow(ms: number = now()): Date {
  const w = new Date(ms + bcOffsetHours(ms) * 3_600_000);
  return new Date(w.getUTCFullYear(), w.getUTCMonth(), w.getUTCDate(), w.getUTCHours(), w.getUTCMinutes());
}

/** B.C.'s today at 00:00 (local fields), for a reader that compares whole days. */
export function bcToday(ms: number = now()): Date {
  const b = bcNow(ms);
  return new Date(b.getFullYear(), b.getMonth(), b.getDate());
}

/** "16:41 PT": an instant on the B.C. clock, labelled. */
export function bcClockTime(ms: number): string {
  const w = new Date(ms + bcOffsetHours(ms) * 3_600_000);
  return `${pad2(w.getUTCHours())}:${pad2(w.getUTCMinutes())} ${BC_TIME_LABEL}`;
}
