/**
 * (owner 2026-11-04: "we should also add sunrise/set time somewhere using bc time for the location") SUNRISE AND SUNSET
 * at a point on a date — the sunrise equation (the sun's centre 0.833° under a level horizon: refraction and its
 * radius), good to a minute or two, which is what a day's line needs. Pure: the instants are UTC ms; the words are on
 * the B.C. clock (`bcClockTime`), as every regulations day is.
 */
import { bcClockTime } from "./day";

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
  /** "Sunrise 07:12 PST · sunset 16:41 PST" */
  line: (rise: string, set: string) => `Sunrise ${rise} · sunset ${set}`,
} as const;

/** The day's line at a point, on the B.C. clock: "Sunrise 07:12 PST · sunset 16:41 PST"; null with no rise or set. */
export function sunLine(lat: number, lon: number, day: { y: number; m: number; d: number }): string | null {
  const t = sunTimes(lat, lon, day.y, day.m, day.d);
  return t.rise === null || t.set === null ? null : SUN_WORDS.line(bcClockTime(t.rise), bcClockTime(t.set));
}
