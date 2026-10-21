/**
 * The Keep tab's year strip (T13): the status line, the 365-day bar, the today marker, the month initials and the legend.
 *
 * Owner rule 2026-10-21 (decisions.md): every answer drawn here is the PIPELINE's —
 * - open / closed on a day is the display frame's `status` (display_frame, one per book day: `inp.year.displayByDay`);
 * - the bar's per-day flags are the display frame's `strip` bits (answers 2.6), drawn as they are;
 * - the trout line's runs are the row's STORED year strip (rows 30 `rows[].year`, `HydRow.year`; decided by
 *   `pipeline/deliver/answers/year.py` with page v36's rule). Nothing here ranks, merges or decides a day: `runOf` only
 *   reads how long the stored state of the selected day lasts, and the trout line only SELECTS which stored row to word.
 *
 * Ports (inventory names @ 7fb6809f): page_v36 DAYS, daySegs (its length), nextOpen, runOf, waterStrip0, segBar (the
 * marker's idx/365); prototype buildKeep's "Closed from" → "Closed <span>" rewrite (round 20i), flagBar (the runs),
 * flagLegend, span. page v36 `strip`/`dayStatus` are NOT ported (the store's `rows[].year` replaces them).
 */
import { COMMON_YEAR_MONTH_LAST_DAY, MONTHS, readingDay, WEEKDAYS } from "../calendar.generated";
import type { CardModel, HydRow, YearRun } from "./card";
import type { DisplayFrame, RegsInputs } from "./inputs";
import type { Day, StripVM } from "./model";
import type { Settled } from "./settle";
import { MONTH1, WD3, WORDS } from "./words";

/** The 365 days of the book's year (no Feb 29), as page v36 DAYS: book day i → {month, day}. */
export const BOOK_DAYS: readonly Day[] = Object.freeze(COMMON_YEAR_MONTH_LAST_DAY.flatMap((n, m) =>
  Array.from({ length: n }, (_, d) => Object.freeze({ month: m + 1, day: d + 1 }))));

const BOOK_YEAR = 365;
/** page DAYS.indexOf(md); Feb 29 reads as Feb 28 (the page's year has no Feb 29). */
export function bookIndex(d: Day): number {
  const day = d.month === 2 && d.day === 29 ? 28 : d.day;
  let i = 0;
  for (let m = 0; m < d.month - 1; m++) i += COMMON_YEAR_MONTH_LAST_DAY[m]!;
  return i + day - 1;
}

/** "Jun 15" (page fmtMd over a Day). */
export const fmtDay = (d: Day): string => `${MONTHS[d.month - 1]} ${d.day}`;

/** The display frame of book day i: `inp.year.displayByDay` at the day's leap-calendar index (Feb 29 never asked). */
export function displayOnBookDay(inp: RegsInputs, i: number): DisplayFrame | null {
  const d = BOOK_DAYS[((i % BOOK_YEAR) + BOOK_YEAR) % BOOK_YEAR]!;
  return inp.year?.displayByDay[readingDay(d.month, d.day) - 1] ?? null;
}

/** page isClosedAt over a book day: the stored display frame says "closed". (settle.ts `isClosedOn` takes a leap day.) */
export const closedOnBookDay = (inp: RegsInputs, i: number): boolean => displayOnBookDay(inp, i)?.status === "closed";

/** page nextOpen: the next book day (wrapping, up to a year on) whose stored frame is not "closed"; null when none is. */
export function nextOpen(inp: RegsInputs, d: Day): Day | null {
  const i = bookIndex(d);
  for (let k = 1; k < 366; k++) { const j = (i + k) % BOOK_YEAR; if (!closedOnBookDay(inp, j)) return BOOK_DAYS[j]!; }
  return null;
}

/** How long the state of a day lasts in a run-length strip, and what comes after it (page runOf, verbatim). */
export interface StateRun { readonly now: string; readonly all: boolean; readonly from?: Day; readonly until?: Day; readonly next?: Day; readonly then?: string }

/** page runOf over a run-length strip of the 365 book days (`[[state, days]]`, the stored `rows[].year` shape). */
export function runOf(segs: readonly (readonly [string, number])[], d: Day): StateRun {
  const days = segs.flatMap(([st, n]) => Array.from({ length: n }, () => st));
  const i = bookIndex(d), now = days[i]!;
  let a = i, b = i, g = 0;
  while (days[(a + 364) % BOOK_YEAR] === now && g++ < BOOK_YEAR) a = (a + 364) % BOOK_YEAR;
  g = 0;
  while (days[(b + 1) % BOOK_YEAR] === now && g++ < BOOK_YEAR) b = (b + 1) % BOOK_YEAR;
  if (g >= BOOK_YEAR) return { now, all: true };
  return { now, all: false, from: BOOK_DAYS[a], until: BOOK_DAYS[b], next: BOOK_DAYS[(b + 1) % BOOK_YEAR], then: days[(b + 1) % BOOK_YEAR] };
}

/** The per-day closed/open runs of the water (page waterSegs): "closed" where the stored frame says so, else "keep". */
export function waterSegs(inp: RegsInputs): [string, number][] {
  const out: [string, number][] = [];
  for (let i = 0; i < BOOK_YEAR; i++) {
    const s = closedOnBookDay(inp, i) ? "closed" : "keep", l = out[out.length - 1];
    if (l && l[0] === s) l[1]++; else out.push([s, 1]);
  }
  return out;
}

/** prototype span over book indices: "Jun 1–30", "Jul 4–Nov 15", "Jun 1". */
export function spanWords(a: number, b: number): string {
  const A = BOOK_DAYS[a]!, B = BOOK_DAYS[b]!;
  return a === b ? fmtDay(A) : A.month === B.month ? `${fmtDay(A)}–${B.day}` : `${fmtDay(A)}–${fmtDay(B)}`;
}

/** page daySegs(w, md).length: how many segments share the start on or before the day (one unless the key has moments). */
export function daySegCount(starts: readonly number[], d: Day): number {
  const day = readingDay(d.month, d.day);
  let i = 0;
  for (let j = 0; j < starts.length; j++) if (starts[j]! <= day) i = j;
  return i - starts.indexOf(starts[i]!) + 1;
}

/** prototype flagBar's runs: the stored strip bits of each book day, run-length (a day with no frame or a tidal frame: null). */
export function stripRuns(inp: RegsInputs): { bits: number | null; days: number }[] {
  const out: { bits: number | null; days: number }[] = [];
  for (let i = 0; i < BOOK_YEAR; i++) {
    const f = displayOnBookDay(inp, i), bits = f && f.status !== "tidal" ? f.strip : null, l = out[out.length - 1];
    if (l && l.bits === bits) l.days++; else out.push({ bits, days: 1 });
  }
  return out;
}

/** prototype flagLegend: bit 1 for a closed run, else bit 0 and each of 2/4/8/16 a run carries; ascending; the data's labels. */
export function legendOf(inp: RegsInputs, runs: readonly { bits: number | null }[]): StripVM["legend"] {
  const bits = new Set<number>();
  for (const { bits: f } of runs) {
    const v = f ?? 0;
    if (v & 1) bits.add(1); else { bits.add(0); for (const b of [2, 4, 8, 16]) if (v & b) bits.add(b); }
  }
  return [...bits].sort((a, b) => a - b).map((bit) => ({
    bit, mark: bit === 0 ? "✓" as const : bit === 1 ? "✕" as const : null,
    label: inp.statics.strip.find((x) => x.bit === bit)?.label ?? "",
  }));
}

/** The fish a "Trout …" line speaks for (page v36 waterStrip0 TR, round UR5): a match key, never said. */
export const TROUT_LINE_FISH: readonly string[] = Object.freeze(["RB", "CT", "GB"]);
const trouty = (r: HydRow) => r.members.some((S) => TROUT_LINE_FISH.includes(S));

/** page waterStrip0's trout line: the stored release/closed trout row (none beside a trout keep row), worded over its
 *  STORED year strip; null when the card has no such row. */
export function troutLine(card: CardModel, d: Day): string | null {
  if (card.rows.some((r) => r.kind === "keep" && trouty(r))) return null;
  const tr = card.rows.find((r) => !r.pool && (r.kind === "closed" || r.kind === "release") && trouty(r));
  if (!tr) return null;
  const ru = runOf(tr.year as readonly YearRun[], d);
  return WORDS.TROUT_LINE(tr.kind === "closed" ? WORDS.TROUT_CLOSED : WORDS.TROUT_RELEASE,
                          ru.all ? WORDS.ALL_YEAR : WORDS.UNTIL(fmtDay(ru.until!)));
}

const sameDay = (a: Day, b: Day) => a.month === b.month && a.day === b.day;
const SOON_WINDOW = 21;

/**
 * The year strip of the selected day `inp.day` (`today` only picks "today" over "on <day>"). Null where page v36 draws
 * none: tidal water (T14's banner) or no display frame (outside B.C.).
 */
export function stripOf(inp: RegsInputs, settled: Settled, card: CardModel, today: Day): StripVM | null {
  const frame = settled.status;
  if (!frame || frame.status === "tidal" || inp.key?.tidal) return null;
  const day = inp.day, i = bookIndex(day);
  const openNow = frame.status !== "closed";
  const partShut = openNow && settled.inpart.some((x) => x.now && x.rule.export.family === "retention");
  const runs = stripRuns(inp);

  const line: { kind: "status" | "when" | "soon" | "soonOpen"; text: string }[] = [
    { kind: "status", text: !openNow ? WORDS.STRIP_CLOSED : partShut ? WORDS.STRIP_OPEN_EXCEPT : WORDS.STRIP_OPEN },
    { kind: "when", text: (sameDay(day, today) ? WORDS.TODAY_LC : WORDS.ON_DAY(fmtDay(day)))
      + (inp.year?.moments && daySegCount(inp.year.segStarts, day) > 1 ? WORDS.WEEKDAY_TAG(WD3[WEEKDAYS.indexOf(inp.weekday)]!) : "") },
  ];
  if (!openNow) {
    const n = nextOpen(inp, day);
    if (n) line.push({ kind: "soonOpen", text: WORDS.OPENS(fmtDay(n)) });
  } else {
    let soon: string | null = null;
    for (let k = 1; k <= SOON_WINDOW; k++) {
      const j = (i + k) % BOOK_YEAR;
      if (closedOnBookDay(inp, j)) { soon = WORDS.CLOSED_FROM(fmtDay(BOOK_DAYS[j]!)); break; }
    }
    if (soon !== null) {
      // prototype round 20i: "Closed from Jun 1" gives the whole span from the strip's own runs — the first closed run
      // starting after the selected day; none found keeps "Closed from …"
      let at = 0;
      for (const r of runs) {
        if (at > i && r.bits != null && (r.bits & 1)) { soon = WORDS.CLOSED_SPAN(spanWords(at, at + r.days - 1)); break; }
        at += r.days;
      }
    } else soon = troutLine(card, day);
    if (soon !== null) line.push({ kind: "soon", text: soon });
  }
  return {
    status: openNow ? "open" : "closed", line, runs, todayAt: i / BOOK_YEAR, months: MONTH1, legend: legendOf(inp, runs),
  };
}
