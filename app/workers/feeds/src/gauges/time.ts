// T225: the clocks of the gauge feed, as plain numbers. An INSTANT is epoch ms plus the offset it was written with
// (minutes east of UTC: Python's aware datetime keeps both, and `zoned` writes the offset back out). A WALL clock is
// ms "as if UTC" (Python's naive datetime). A DAY is "YYYY-MM-DD". The Centre's clock is PST all year (forecast.py).

export interface Instant {
  ms: number;
  off: number;
}

export const CENTRE_OFF = -480; // forecast.CENTRE_UTC_OFFSET_H * 60
const MIN = 60_000;
const HOUR = 3_600_000;
export const DAY_MS = 86_400_000;

const DATE_RE = /^(\d{4})-?(\d{2})-?(\d{2})/;
const TIME_RE = /^(\d{2})(?::?(\d{2})(?::?(\d{2})(?:[.,](\d+))?)?)?/;
const TZ_RE = /^([+-])(\d{2})(?::?(\d{2})(?::?(\d{2})(?:\.(\d+))?)?)?$/;

const MDAYS = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
function daysIn(y: number, m: number): number {
  return m === 2 && y % 4 === 0 && (y % 100 !== 0 || y % 400 === 0) ? 29 : MDAYS[m - 1]!;
}

// civil <-> day number (days since 1970-01-01), proleptic Gregorian (H. Hinnant's algorithms): no Date objects on the
// hot path — every series stamp is read several times a run (measured: Date-based day strings were half the CPU)
function daysFromCivil(y: number, m: number, d: number): number {
  y -= m <= 2 ? 1 : 0;
  const era = Math.floor(y / 400);
  const yoe = y - era * 400;
  const doy = Math.floor((153 * (m + (m > 2 ? -3 : 9)) + 2) / 5) + d - 1;
  const doe = yoe * 365 + Math.floor(yoe / 4) - Math.floor(yoe / 100) + doy;
  return era * 146097 + doe - 719468;
}
function civilFromDays(z: number): [number, number, number] {
  z += 719468;
  const era = Math.floor(z / 146097);
  const doe = z - era * 146097;
  const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36524) - Math.floor(doe / 146096)) / 365);
  const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * doy + 2) / 153);
  const d = doy - Math.floor((153 * mp + 2) / 5) + 1;
  const m = mp + (mp < 10 ? 3 : -9);
  return [yoe + era * 400 + (m <= 2 ? 1 : 0), m, d];
}
const P2 = Array.from({ length: 100 }, (_, i) => String(i).padStart(2, "0"));
function ymd(y: number, m: number, d: number): string {
  return `${String(y).padStart(4, "0")}-${P2[m]}-${P2[d]}`;
}

/** `datetime.fromisoformat` (3.11+ forms that occur here): wall ms and the offset, or null where Python raises. */
const isoCache = new Map<string, { wall: number; off: number | null } | null>();
export function parseIso(s: string): { wall: number; off: number | null } | null {
  let got = isoCache.get(s);
  if (got === undefined) {
    got = parseIsoUncached(s);
    if (isoCache.size > 20_000) isoCache.clear(); // bounded: a station's stamps are read together
    isoCache.set(s, got);
  }
  return got;
}

function parseIsoUncached(s: string): { wall: number; off: number | null } | null {
  const d = DATE_RE.exec(s);
  if (!d) return null;
  const dateLen = d[0].length;
  if (dateLen !== 10 && dateLen !== 8) return null;
  const y = +d[1]!;
  const mo = +d[2]!;
  const da = +d[3]!;
  if (y < 1 || mo < 1 || mo > 12 || da < 1 || da > daysIn(y, mo)) return null;
  let rest = s.slice(dateLen);
  let h = 0;
  let mi = 0;
  let se = 0;
  let ms = 0;
  let off: number | null = null;
  if (rest.length) {
    rest = rest.slice(1); // any one separator character
    const t = TIME_RE.exec(rest);
    if (!t) return null;
    h = +t[1]!;
    mi = t[2] ? +t[2] : 0;
    se = t[3] ? +t[3] : 0;
    ms = t[4] ? Math.floor(+t[4].slice(0, 6).padEnd(6, "0") / 1000) : 0;
    if (h > 23 || mi > 59 || se > 59) return null;
    rest = rest.slice(t[0].length);
    if (rest.length) {
      if (rest === "Z") off = 0;
      else {
        const z = TZ_RE.exec(rest);
        if (!z) return null;
        const zh = +z[2]!;
        const zm = z[3] ? +z[3] : 0;
        if (zh > 23 || zm > 59) return null;
        off = (z[1] === "-" ? -1 : 1) * (zh * 60 + zm);
      }
    }
  }
  return { wall: daysFromCivil(y, mo, da) * DAY_MS + h * HOUR + mi * MIN + se * 1000 + ms, off };
}

/** forecast.instant: a stamp as an aware instant; naive is a Centre wall clock; unreadable null. */
export function instant(at: unknown): Instant | null {
  if (at === null || at === undefined) return null;
  const s = String(at).trim().replace(/Z/g, "+00:00");
  const p = parseIso(s);
  if (!p) return null;
  const off = p.off ?? CENTRE_OFF;
  return { ms: p.wall - off * MIN, off };
}

const BARE_DAY = /^\d{4}-\d{2}-\d{2}$/;
/** forecast.is_day */
export function isDay(at: unknown): boolean {
  return typeof at === "string" && BARE_DAY.test(at.trim());
}

function pad(n: number, w = 2): string {
  return String(n).padStart(w, "0");
}

/** "YYYY-MM-DDTHH:MM:SS" of a wall clock (seconds truncated, as isoformat(timespec="seconds")). */
export function wallIso(wall: number): string {
  const sec = Math.floor(wall / 1000);
  const days = Math.floor(sec / 86400);
  const r = sec - days * 86400;
  return `${dayOfNumber(days)}T${P2[Math.floor(r / 3600)]}:${P2[Math.floor((r % 3600) / 60)]}:${P2[r % 60]}`;
}

export function offStr(off: number): string {
  const sign = off < 0 ? "-" : "+";
  const a = Math.abs(off);
  return `${sign}${pad(Math.floor(a / 60))}:${pad(a % 60)}`;
}

/** isoformat(timespec="seconds") of an instant in its own offset. */
export function isoSeconds(t: Instant): string {
  return wallIso(t.ms + t.off * MIN) + offStr(t.off);
}

/** forecast.zoned */
export function zoned(at: string): string {
  if (isDay(at)) return at;
  const t = instant(at);
  return t ? isoSeconds(t) : at;
}

/** The Centre's naive wall clock of an epoch ms (`astimezone(CENTRE_ZONE).replace(tzinfo=None)`). */
export function centreWall(ms: number): number {
  return ms + CENTRE_OFF * MIN;
}

/** The day ("YYYY-MM-DD") of a wall clock. */
const dayCache = new Map<number, string>();
function dayOfNumber(n: number): string {
  let s = dayCache.get(n);
  if (s === undefined) {
    const [y, m, d] = civilFromDays(n);
    s = ymd(y, m, d);
    if (dayCache.size > 20_000) dayCache.clear();
    dayCache.set(n, s);
  }
  return s;
}
export function dayOf(wall: number): string {
  return dayOfNumber(Math.floor(wall / DAY_MS));
}

/** Midnight (wall ms) of a day; null for an unreadable one (date.fromisoformat raising). */
const dayMsCache = new Map<string, number | null>();
export function dayMs(day: string): number | null {
  let v = dayMsCache.get(day);
  if (v === undefined) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day);
    if (!m) v = null;
    else {
      const y = +m[1]!;
      const mo = +m[2]!;
      const d = +m[3]!;
      v = y < 1 || mo < 1 || mo > 12 || d < 1 || d > daysIn(y, mo) ? null : daysFromCivil(y, mo, d) * DAY_MS;
    }
    if (dayMsCache.size > 20_000) dayMsCache.clear();
    dayMsCache.set(day, v);
  }
  return v;
}

export function addDays(day: string, n: number): string {
  return dayOf(dayMs(day)! + n * DAY_MS);
}

/** tm_yday (1..366) of a wall clock. */
export function yday(wall: number): number {
  const n = Math.floor(wall / DAY_MS);
  const [y] = civilFromDays(n);
  return n - daysFromCivil(y, 1, 1) + 1;
}

/** Hours between two ms clocks (timedelta.total_seconds() / 3600.0). */
export function hours(a: number, b: number): number {
  return (a - b) / 1000 / 3600.0;
}

export { HOUR, MIN };
