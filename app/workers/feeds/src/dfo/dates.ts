// T225 follow-up (owner 2026-10-28): THE DFO DATE CELL, read robustly. The Python reader (dates.parse_date_window +
// catalogue.parse_date_range, ported as-is in typed.ts for the parity mode) misses whole families the pages print:
// "Apr 1 until further notice" (145 rows in the recorded corpus), "Aug 2 To Aug 31", "July 1 until Aug 8",
// "May 8 2018 until further notice", "October 1 to …" ('to' inside "October" split the cell), "1 Oct to 31 Dec",
// "Oct 1st to Dec 31st", "Sept. 15 through Oct 30", year suffixes, typos ("Aprl", "futher").
//
// The answer is one of:
//   empty      the cell is blank (the rule has no `when`: all year, as before)
//   tbd        "To be determined" (DFO has not set the row; the advisory path, as before)
//   ok         ranges (each may wrap the year end: "Nov 30 to Jan 15"), and `openEnded` when the cell says
//              "until further notice": the LAST range then runs on with no end (see salmon.ts for how it reads)
//   unparsed   anything else — NEVER guessed: the row's rules carry it as "dates unparsed — check DFO"
//
// Pure, never throws: the fuzz test (test/dfo.dates.test.ts) feeds it garbage.
import { getCloseMatches } from "./difflib";
import { LAST_DAY, type DateRange } from "./typed";

export type DateCell =
  | { kind: "empty" }
  | { kind: "tbd" }
  | { kind: "ok"; ranges: DateRange[]; openEnded: boolean }
  | { kind: "unparsed"; raw: string };

const FULL = ["january", "february", "march", "april", "may", "june", "july", "august", "september", "october", "november", "december"];

/** a month word → 1..12: any prefix of the full name of 3+ letters ("sept", "octob"), else ONE close match (typos). */
export function monthOf(word: string): number | null {
  const w = word.toLowerCase().replace(/\.$/, "");
  if (w.length < 3 || !/^[a-z]+$/.test(w)) return null;
  const i = FULL.findIndex((m) => m.startsWith(w));
  if (i >= 0) return i + 1;
  if (w.length < 4) return null; // three-letter typos are too ambiguous ("jum", "mat")
  const near = getCloseMatches(w, FULL, 2, 0.75);
  if (near.length === 1) return FULL.indexOf(near[0]!) + 1;
  // abbreviated typo ("Spet", "Agu"): compare with the same-length prefixes
  const pre = getCloseMatches(w, FULL.map((m) => m.slice(0, w.length)), 2, 0.75);
  return pre.length === 1 ? FULL.findIndex((m) => m.slice(0, w.length) === pre[0]) + 1 : null;
}

const RE_TBD = /\bto\s+be\s+determined\b|\btbd\b/i;
// "until further notice", "until futher notice", "until further notification", "ufn"
const RE_UFN = /\b(?:until|till|til)\s+fu\w*\s+noti\w*\b|\bufn\b/i;
const RE_LEAD = /^(?:(?:effective|from|starting|beginning|commencing|opens?|open\s+from|as\s+of|on)\s+)+/i;
const RE_YEAR = /,?\s*\b(?:19|20)\d{2}\b,?/g;
const RE_ORD = /(\d)\s*(?:st|nd|rd|th)\b/gi;
// a range separator: dashes, to, through, thru, until, till (word-bounded, so "October" is never cut)
const RE_SEP = /\s*(?:[-‐‑‒–—―−]+\s*(?:to\b)?|\bto\b|\bthrough\b|\bthru\b|\buntil\b|\btill?\b|\bup\s+to\b)\s*/i;
// several ranges in one cell: "Jul 1 to Aug 31, Oct 1 to Dec 31" / "…; …" / "… and …" / "… & …"
const RE_LIST = /\s*(?:[,;&]|\band\b)\s*/i;

type Point = { m: number; d: number | null };

/** "Oct 1" / "October 1" / "1 Oct" / "Oct." / "Oct" (day null) / bare "15" (month null — the range's other side) */
function point(text: string): Point | { m: null; d: number } | null {
  const t = text.trim().replace(/\.(?=\s|$)/g, "").replace(/\s+/g, " ");
  let m = /^([A-Za-z]+)\s*(\d{1,2})$/.exec(t);
  if (m) {
    const mo = monthOf(m[1]!);
    return mo ? { m: mo, d: Number(m[2]) } : null;
  }
  m = /^(\d{1,2})\s*([A-Za-z]+)$/.exec(t);
  if (m) {
    const mo = monthOf(m[2]!);
    return mo ? { m: mo, d: Number(m[1]) } : null;
  }
  m = /^([A-Za-z]+)$/.exec(t);
  if (m) {
    const mo = monthOf(m[1]!);
    return mo ? { m: mo, d: null } : null;
  }
  m = /^(\d{1,2})$/.exec(t);
  if (m) return { m: null, d: Number(m[1]) };
  return null;
}

const valid = (m: number, d: number) => m >= 1 && m <= 12 && d >= 1 && d <= LAST_DAY[m - 1]!;

/** one "A to B" (or a single point / month) → a range, or null */
function range(text: string, openEnd: boolean): DateRange | null {
  const parts = text.split(RE_SEP);
  if (parts.length === 0 || parts.length > 2 || parts.some((p) => p.trim() === "")) return null; // "Mar to": dangling
  const a = point(parts[0]!);
  if (!a || a.m === null) return null;
  if (parts.length === 1) {
    if (openEnd) {
      // "Apr 1 until further notice": the start; the end is open (salmon.ts reads the open end)
      const d = a.d ?? 1;
      return valid(a.m, d) ? { from_month: a.m, from_day: d, to_month: 12, to_day: 31 } : null;
    }
    if (a.d === null) return { from_month: a.m, from_day: 1, to_month: a.m, to_day: LAST_DAY[a.m - 1]! }; // "August"
    return valid(a.m, a.d) ? { from_month: a.m, from_day: a.d, to_month: a.m, to_day: a.d } : null; // one day
  }
  const b = point(parts[1]!);
  if (!b) return null;
  const fm = a.m;
  const fd = a.d ?? 1;
  const tm = b.m ?? a.m; // "Oct 1-15"
  const td = b.d ?? (b.m !== null ? LAST_DAY[b.m - 1]! : null);
  if (td === null) return null;
  if (b.m === null && a.d === null) return null; // "Oct - 15": no
  if (!valid(fm, fd) || !valid(tm, td)) return null;
  return { from_month: fm, from_day: fd, to_month: tm, to_day: td };
}

/** The DFO date cell → its reading (see the header). */
export function readDates(cell: string | null | undefined): DateCell {
  const raw = (cell ?? "").replace(/\*/g, "").replace(/\s+/g, " ").trim();
  if (!raw) return { kind: "empty" };
  if (RE_TBD.test(raw)) return { kind: "tbd" };
  let t = raw;
  let openEnded = false;
  if (RE_UFN.test(t)) {
    openEnded = true;
    t = t.replace(RE_UFN, " ").trim();
  }
  t = t
    .replace(RE_YEAR, " ")
    .replace(RE_ORD, "$1")
    .replace(/\s+/g, " ")
    .trim()
    .replace(RE_LEAD, "")
    .replace(/^[\s,;:]+|[\s,;:.]+$/g, "")
    .replace(/\s+(?:or|and)$/i, "");
  if (!t) {
    // "Until further notice" alone: in force now, with no start printed
    return openEnded ? { kind: "ok", ranges: [{ from_month: 1, from_day: 1, to_month: 12, to_day: 31 }], openEnded: true } : { kind: "unparsed", raw };
  }
  // one range first ("Oct 1 - 15" must not be split as a list), then a list of ranges
  const one = range(t, openEnded);
  if (one) return { kind: "ok", ranges: [one], openEnded };
  const items = t.split(RE_LIST).filter((p) => p.trim() !== "");
  if (items.length < 2) return { kind: "unparsed", raw };
  const ranges: DateRange[] = [];
  for (let i = 0; i < items.length; i++) {
    const r = range(items[i]!, openEnded && i === items.length - 1);
    if (!r) return { kind: "unparsed", raw };
    ranges.push(r);
  }
  return { kind: "ok", ranges, openEnded };
}
