/**
 * THE WORDS FOR A GAUGE'S STANDING, AND ITS FIGURE — said once (T583; AGENTS 23, 33, 40).
 *
 * One standing ("the 20th percentile for the date") was worded five ways: "Low for the time of year", "Below normal · 20th
 * percentile for the date", "Reads 2 in 10", "p20th", and a rounded figure beside a different phrase on the public gauge
 * page. Every surface reads THIS module (card, rows, map dot, chart labels, alert page, spot, the public gauge page and
 * its share card, which import it from core), and `tools/one-standing-words.test.ts` fails if a standing phrase or a
 * percentile format is composed anywhere else.
 *
 * THE FORMS, all built from one band table (`BAND_WORDS`):
 *   band       "Below normal"                                     the band alone (legend, day strip, hover, colour keys)
 *   headline   "Below normal for the time of year"                the top of a screen, before any number
 *   short      "Below normal · lower than 8 days in 10"           a row, a chart label, an alert line
 *   long       "Below normal · lower than 8 days in 10 for the date"  the gauge card's standing line
 *   reads      "Reads below normal · lower than 8 days in 10."    a donor's row
 *   daysInTen  "lower than 8 days in 10"                          the bare rank in a sentence (T598; was "20th percentile")
 *   daysShort  "8 in 10 days higher"                              THE SHORT FORM, where the long one does not fit: a map
 *                                                                 dot label, a day's chip, a histogram tick. Same fact
 *                                                                 turned round: lower than 8 days in 10 = 8 in 10 days higher.
 *   inTen      "lower than 8 days in 10 at this time of year"     the estimate's sentence
 *   place      "9+ in 10 days higher"                             a day whose value is on or past the envelope's edge
 *
 * THE ROUNDING RULE (T598, one, here): a percentile p (0-1) at or under the middle becomes
 * n = round((1 - p) x 10), above it n = round(p x 10), half UP, so a rank and its mirror give the same n (25th and 75th
 * are both 8). Low: "lower than n days in 10" (short "n in 10 days higher"); high: "higher than n days in 10" (short
 * "n in 10 days lower"). THE ENDS: n = 10 (p under 0.05, or from 0.95) never prints "10 days in 10": it says "lower than almost every day on record" / "higher than almost every day on record" (short:
 * "almost all days higher" / "almost all days lower"). A reading ON the envelope's edge (p10 / p90 clamp) says "9+ in 10
 * days higher" / "9+ in 10 days lower". The word for the old rank is not reader-facing anywhere (the gate refuses it).
 *
 * NOTHING HERE IS A SECOND OPINION: each takes the percentile the arithmetic produced (or the band `standing()` chose
 * from it) and only chooses words for it.
 */
import { standing, type Standing } from "./flow";

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

/**
 * THE ROUNDING RULE, once: of the ten days, how many does the reading beat, counted from the side it is on, rounded half
 * UP so a rank and its mirror say the same count (25th and 75th are both "8"). `count` 0-10; 0 and 10 are the ends.
 */
function side(p: number): { low: boolean; count: number } {
  const low = p <= 0.5;
  const q = low ? 1 - p : p;
  return { low, count: Math.floor(q * 10 + 0.5 + 1e-9) };
}

/** "lower than 8 days in 10" — a percentile (0–1) as a count out of ten, inside a sentence. Ends: see the header. */
export function daysInTen(p: number): string {
  const { low, count } = side(p);
  if (count >= 10) return `${low ? "lower" : "higher"} than almost every day on record`;
  return `${low ? "lower" : "higher"} than ${count} days in 10`;
}

/** "8 in 10 days higher" — THE SHORT FORM, for a map dot, a chip, a tick. The same fact as `daysInTen`, turned round. */
export function daysShort(p: number): string {
  const { low, count } = side(p);
  if (count >= 10) return `almost all days ${low ? "higher" : "lower"}`;
  return `${count} in 10 days ${low ? "higher" : "lower"}`;
}

/** "about 8 in 10 days higher" — a histogram tick's place on the scale, aloud (0–1). */
export const daysNear = (p: number): string => `about ${daysShort(p)}`;

/** The alert line's level: "the median", else "the level lower than 8 days in 10" — from a whole-number rank 10-90. */
export const levelName = (pct: number): string => (pct === 50 ? "the median" : `the level ${daysInTen(pct / 100)}`);

/** "10 to 90" — the whole-number scale a custom alert level is typed on, in a sentence. */
export const levelSpan = (lo: number, hi: number): string => `${lo} to ${hi}`;

/**
 * "Lower than 7 to 9 days in 10" — an estimate's interval (whole percents), said as the two counts it spans, held to 9
 * (never "10 in 10"). Both ends on one side share its word; an interval across the middle says both.
 */
export const daysRange = (lo: number, hi: number): string => {
  const a = side(lo / 100), b = side(hi / 100);
  const n = (s: { count: number }) => Math.min(9, s.count);
  const up = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);
  if (a.low !== b.low) return up(`${a.low ? "lower" : "higher"} than ${n(a)} days in 10 to ${b.low ? "lower" : "higher"} than ${n(b)} days in 10`);
  const [x, y] = [n(a), n(b)].sort((u, v) => u - v);
  const word = a.low ? "Lower" : "Higher";
  return x === y ? `${word} than ${x} days in 10` : `${word} than ${x} to ${y} days in 10`;
};

/** An alert preset's chip: "Median", else the short form for that level ("8 in 10 days higher"). */
export const levelChip = (pct: number): string => (pct === 50 ? "Median" : daysShort(pct / 100));

/** "Below normal · lower than 8 days in 10" — a row, a chart label. Null when there is no percentile. */
export function standingShort(percentile: number | null, band: Standing | null = null): string | null {
  return percentile === null ? null : `${bandWord(band ?? standing(percentile))} · ${daysInTen(percentile)}`;
}

/** "Below normal · lower than 8 days in 10 for the date" — the gauge card's line, or the admission there is nothing to rank by. */
export function standingLong(percentile: number | null, band: Standing | null = null): string {
  if (percentile === null) return "There is a reading here, but no record to compare it against.";
  return `${standingShort(percentile, band)} for the date`;
}

/** "Reads below normal · lower than 8 days in 10." — a donor's row. */
export function standingReads(percentile: number): string {
  const s = standingShort(percentile)!;
  return `Reads ${s[0]!.toLowerCase()}${s.slice(1)}.`;
}

/** Where a day's value sits: a percentile (0–1), or ON or past the envelope's edge (the feed clamps a value on p10 / p90 too). */
export type Place = "below" | "above" | number;

/** "9+ in 10 days higher", "8 in 10 days lower" — the chip under a day's value (short form). */
export const placeChip = (p: Place): string =>
  p === "below" ? "9+ in 10 days higher" : p === "above" ? "9+ in 10 days lower" : daysShort(p);

/** "lower than at least 9 days in 10 for the day", "lower than 8 days in 10 for the day" — a day's place, in a sentence. */
export const placeSaid = (p: Place, scope: "day" | "date"): string =>
  p === "below" ? `lower than at least 9 days in 10 for the ${scope}`
  : p === "above" ? `higher than at least 9 days in 10 for the ${scope}`
  : `${daysInTen(p)} for the ${scope}`;

/**
 * "lower than 7 days in 10 at this time of year" — a percentile as a count out of ten, the estimate's sentence.
 *
 * OUT OF TEN AND NOT OUT OF A HUNDRED, because the underlying number is not good to a hundredth: the transfer error alone
 * is ±11.7 points at best. Ten is the precision the evidence supports and also the one people picture.
 */
export function inTen(percentile: number): string {
  return `${daysInTen(percentile)}${side(percentile).count >= 10 ? " for this time of year" : " at this time of year"}`;
}

/** A value as a reader reads it: three figures, no false precision. THE figure, for the app, the chart and the public page. */
export function figure(v: number): string {
  const a = Math.abs(v);
  if (a >= 100) return String(Math.round(v));
  if (a >= 10) return String(Math.round(v * 10) / 10);
  return String(Math.round(v * 100) / 100);
}
