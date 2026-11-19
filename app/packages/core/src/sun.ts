/**
 * (owner 2026-11-04: "we should also add sunrise/set time somewhere using bc time for the location") SUNRISE AND SUNSET
 * at a point on a date — the sunrise equation (the sun's centre 0.833° under a level horizon: refraction and its
 * radius), good to a minute or two, which is what a day's line needs. Pure: the instants are UTC ms; the words are on
 * the B.C. clock (`bcClockTime`), as every regulations day is.
 */
import { bcClockTime } from "./clock";

const RAD = Math.PI / 180;
const J2000 = 2451545.0;
const UNIX_JD = 2440587.5;
const DAY_MS = 86_400_000;

export interface SunTimes {
  /** UTC ms; null where the sun does not set or rise that day (north of the Arctic circle — not in B.C.) */
  rise: number | null;
  set: number | null;
}

/** Sunrise and sunset at (`lat`, `lon`, degrees, east positive) on the calendar date `y`-`m`-`d`. */
export function sunTimes(lat: number, lon: number, y: number, m: number, d: number): SunTimes {
  const n = Math.ceil(Date.UTC(y, m - 1, d) / DAY_MS + UNIX_JD - J2000 + 0.0008);
  const jStar = n - lon / 360;
  const M = (357.5291 + 0.98560028 * jStar) % 360;
  const C = 1.9148 * Math.sin(M * RAD) + 0.02 * Math.sin(2 * M * RAD) + 0.0003 * Math.sin(3 * M * RAD);
  const L = (M + C + 180 + 102.9372) % 360;
  const transit = J2000 + jStar + 0.0053 * Math.sin(M * RAD) - 0.0069 * Math.sin(2 * L * RAD);
  const sinDec = Math.sin(L * RAD) * Math.sin(23.4397 * RAD);
  const cosDec = Math.cos(Math.asin(sinDec));
  const cosH = (Math.sin(-0.833 * RAD) - Math.sin(lat * RAD) * sinDec) / (Math.cos(lat * RAD) * cosDec);
  if (cosH < -1 || cosH > 1) return { rise: null, set: null };
  const h = Math.acos(cosH) / RAD / 360;
  const ms = (j: number) => Math.round((j - UNIX_JD) * DAY_MS);
  return { rise: ms(transit - h), set: ms(transit + h) };
}

export const SUN_WORDS = {
  /** "07:04–18:37 PT" */
  range: (from: string, to: string) => `${from}–${to}`,
  /** (T878) the day's sun under the date: "9:01–17:17" — no "PT" (the app is B.C.-only; owner 2026-11-18 look round) */
  day: (rise: string, set: string) => `${rise}–${set}`,
  /** (T878) what a screen reader says for it: "Sunrise 9:01, sunset 17:17 PT" */
  aria: (rise: string, set: string) => `Sunrise ${rise}, sunset ${set} PT`,
} as const;

/** The day's sun as a screen draws it: the line's words, its accessible label, and the two times alone ("9:01", "17:17"). */
export interface SunDay { text: string; aria: string; rise: string; set: string }

/** The B.C. clock time without its zone word: "05:07" of "05:07 PT". */
const bare = (ms: number) => bcClockTime(ms).replace(/ \S+$/, "");

/** "9:01" of "09:01 PT": the B.C. clock, the hour without its leading 0 and no zone word. */
const short = (ms: number) => bare(ms).replace(/^0(?=\d:)/, "");

/**
 * (T878, owner 2026-11-18) THE DAY'S SUN at a point, on the B.C. clock: the small line under the day pill ("9:01–17:17",
 * beside the sun icon), what a screen reader says for it ("Sunrise 9:01, sunset 17:17 PT") and the two times alone (the
 * rise / set look); null with no rise or set.
 */
export function sunLine(lat: number, lon: number, day: { y: number; m: number; d: number }): SunDay | null {
  const t = sunTimes(lat, lon, day.y, day.m, day.d);
  if (t.rise === null || t.set === null) return null;
  const rise = short(t.rise), set = short(t.set);
  return { text: SUN_WORDS.day(rise, set), aria: SUN_WORDS.aria(rise, set), rise, set };
}

/**
 * (T473) The hours a "daylight only" closure leaves open on a day at a point: from `beforeMin` before sunrise to `afterMin`
 * after sunset ("07:04–18:37 PT"), the offsets the rule's own words carry; null with no rise or set.
 */
export function daylightRange(lat: number, lon: number, day: { y: number; m: number; d: number }, afterMin: number, beforeMin: number): string | null {
  const t = sunTimes(lat, lon, day.y, day.m, day.d);
  if (t.rise === null || t.set === null) return null;
  return SUN_WORDS.range(bare(t.rise - beforeMin * 60_000), bcClockTime(t.set + afterMin * 60_000));
}

/**
 * (T898, owner 2026-11-19: "could give you the times for it on the section you are currently on") The hours a "daylight
 * only" closure CLOSES on a day at a point: from `afterMin` after that day's sunset to `beforeMin` before its sunrise —
 * the same sun as the line under the date (`sunLine`, the same point and day), on its clock ("18:21", "7:23": no "PT", no
 * leading 0). Both ends are the picked day's own sunset and sunrise, as that line says them (the next morning's sunrise
 * is a minute or two from it). Null with no rise or set.
 */
export function daylightClosed(lat: number, lon: number, day: { y: number; m: number; d: number }, afterMin: number,
                               beforeMin: number): { from: string; to: string } | null {
  const t = sunTimes(lat, lon, day.y, day.m, day.d);
  if (t.rise === null || t.set === null) return null;
  return { from: short(t.set + afterMin * 60_000), to: short(t.rise - beforeMin * 60_000) };
}

/** (T473, T898) A daylight-only closure's hours on one day at one point, given the rule's own offsets (minutes closed after
 *  sunset, before sunrise): the hours it leaves open (`daylightRange`) and the hours it closes (`daylightClosed`). */
export interface DaylightAt {
  open(afterMin: number, beforeMin: number): string | null;
  closed(afterMin: number, beforeMin: number): { from: string; to: string } | null;
}

/** (T473, T898) `DaylightAt` for the point and the day. */
export function daylightAt(lat: number, lon: number, day: { y: number; m: number; d: number }): DaylightAt {
  return { open: (a, b) => daylightRange(lat, lon, day, a, b), closed: (a, b) => daylightClosed(lat, lon, day, a, b) };
}
