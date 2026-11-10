/**
 * THE WORDS FOR A GAUGE'S STANDING, AND ITS FIGURE — said once (T583; AGENTS 23, 33, 40).
 *
 * One standing ("the 20th percentile for the date") was worded five ways: "Low for the time of year", "Below normal · 20th
 * percentile for the date", "Reads 2 in 10", "p20th", and a rounded figure beside a different phrase on the public gauge
 * page. Every surface reads THIS module (card, rows, map dot, chart labels, alert page, spot, the public gauge page and
 * its share card, which import it from core), and `tools/one-standing-words.test.ts` fails if a standing phrase or a
 * percentile format is composed anywhere else.
 *
 * THE FORMS, all built from one band table (`BAND_WORDS`) and one ordinal (`percentileOrdinal`):
 *   band       "Below normal"                                     the band alone (legend, day strip, hover, colour keys)
 *   headline   "Below normal for the time of year"                the top of a screen, before any number
 *   short      "Below normal · 20th percentile"                   a row, a chart label, an alert line
 *   long       "Below normal · 20th percentile for the date"      the gauge card's standing line
 *   reads      "Reads below normal · 20th percentile."            a donor's row
 *   percentile "20th percentile"                                  the bare rank: a map dot, a chip, a sentence (`percentileSaid`)
 *   inTen      "lower than 8 days in 10 at this time of year"     the estimate's sentence, where a count says it better
 *   place      "≤10th percentile"                  a day whose value is on or past the envelope's edge
 *
 * NOTHING HERE IS A SECOND OPINION: each takes the percentile the arithmetic produced (or the band `standing()` chose
 * from it) and only chooses words for it.
 */
import { ordinal, percentileOrdinal, standing, type Standing } from "./flow";

/** The six bands, once. `band` is the sentence-case word; `headline` is what the water is doing, in a screen's first line. */
const BAND_WORDS: Record<Standing, { band: string; headline: string }> = {
  "much-below": { band: "Much below normal", headline: "Much below normal for the time of year" },
  below:        { band: "Below normal",      headline: "Below normal for the time of year" },
  normal:       { band: "Normal",            headline: "Normal for the time of year" },
  above:        { band: "Above normal",      headline: "Above normal for the time of year" },
  "much-above": { band: "Much above normal", headline: "Much above normal for the time of year" },
  "no-record":  { band: "No record",         headline: "No record to compare against" },
};

/** "Below normal" — the band alone. */
export const bandWord = (s: Standing): string => BAND_WORDS[s].band;

/** "Below normal for the time of year" — the top of a screen. */
export const standingHeadline = (s: Standing): string => BAND_WORDS[s].headline;

/** "20th percentile" — a percentile (0–1) as a reader sees it, inside a sentence. */
export const percentileSaid = (p: number): string => `${percentileOrdinal(p)} percentile`;

/** "the median" (50), else "the 75th percentile" — an alert's line, from a whole-number percentile. */
export const percentileName = (pct: number): string => (pct === 50 ? "the median" : `the ${ordinal(pct)} percentile`);

/** "the 10th to the 90th percentile" — a span of whole-number percentiles, in a sentence. */
export const percentileSpan = (lo: number, hi: number): string => `the ${ordinal(lo)} to the ${ordinal(hi)} percentile`;

/** "3rd–27th percentile" — an estimate's interval (whole numbers), clamped to 1–99: the envelope stops at p10 / p90, so a range may not claim the record's own ends. */
export const percentileRange = (lo: number, hi: number): string => {
  const bound = (n: number) => ordinal(Math.max(1, Math.min(99, Math.round(n))));
  return `${bound(lo)}–${bound(hi)} percentile`;
};

/** "near the 20th percentile" — a histogram tick's place on the scale, aloud (0–1). */
export const percentileNear = (p: number): string => `near the ${percentileSaid(p)}`;

/** "Below normal · 20th percentile" — a row, a map dot, a chart label. Null when there is no percentile. */
export function standingShort(percentile: number | null, band: Standing | null = null): string | null {
  return percentile === null ? null : `${bandWord(band ?? standing(percentile))} · ${percentileSaid(percentile)}`;
}

/** "Below normal · 20th percentile for the date" — the gauge card's line, or the admission there is nothing to rank by. */
export function standingLong(percentile: number | null, band: Standing | null = null): string {
  if (percentile === null) return "There is a reading here, but no record to compare it against.";
  return `${standingShort(percentile, band)} for the date`;
}

/** "Reads below normal · 20th percentile." — a donor's row. */
export function standingReads(percentile: number): string {
  const s = standingShort(percentile)!;
  return `Reads ${s[0]!.toLowerCase()}${s.slice(1)}.`;
}

/** Where a day's value sits: a percentile (0–1), or ON or past the envelope's edge (the feed clamps a value on p10 / p90 too). */
export type Place = "below" | "above" | number;

/** "≤10th percentile", "63rd percentile" — the chip under a day's value. */
export const placeChip = (p: Place): string =>
  p === "below" ? "≤10th percentile" : p === "above" ? "≥90th percentile" : percentileSaid(p);

/** "at or below the 10th percentile for the day", "the 63rd percentile for the day" — a day's place, in a sentence. */
export const placeSaid = (p: Place, scope: "day" | "date"): string =>
  p === "below" ? `at or below the 10th percentile for the ${scope}`
  : p === "above" ? `at or above the 90th percentile for the ${scope}`
  : `the ${percentileSaid(p)} for the ${scope}`;

/**
 * "lower than 7 days in 10" — a percentile as a count out of ten.
 *
 * OUT OF TEN AND NOT OUT OF A HUNDRED, because the underlying number is not good to a hundredth: the transfer error alone
 * is ±11.7 points at best. Ten is the precision the evidence supports and also the one people picture.
 */
export function inTen(percentile: number): string {
  const below = Math.round(percentile * 10);
  if (below <= 0) return "lower than almost every day on record for this time of year";
  if (below >= 10) return "higher than almost every day on record for this time of year";
  const side = below <= 5
    ? `lower than ${10 - below} days in 10`
    : `higher than ${below} days in 10`;
  return `${side} at this time of year`;
}

/** A value as a reader reads it: three figures, no false precision. THE figure, for the app, the chart and the public page. */
export function figure(v: number): string {
  const a = Math.abs(v);
  if (a >= 100) return String(Math.round(v));
  if (a >= 10) return String(Math.round(v * 10) / 10);
  return String(Math.round(v * 100) / 100);
}
