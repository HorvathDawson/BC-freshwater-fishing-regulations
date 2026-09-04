/**
 * What a gauge reading means, and when a gauge is entitled to speak at all.
 *
 * 15.7 m3/s is meaningless on its own — it is a flood on one creek and a drought on
 * another. The number that means something is where today sits in that station's own
 * record for this day of the year.
 */

/** Percentile bands from the station's own history: p10, p25, p50, p75, p90. */
export type Band = readonly [number, number, number, number, number];

export type Standing =
  | "much-below" | "below" | "normal" | "above" | "much-above" | "no-record";

export function standing(percentile: number | null): Standing {
  if (percentile === null) return "no-record";
  if (percentile < 0.10) return "much-below";
  if (percentile < 0.25) return "below";
  if (percentile < 0.75) return "normal";
  if (percentile < 0.90) return "above";
  return "much-above";
}

export function standingWord(s: Standing): string {
  return {
    "much-below": "MUCH BELOW NORMAL", below: "BELOW NORMAL", normal: "NORMAL",
    above: "ABOVE NORMAL", "much-above": "MUCH ABOVE NORMAL", "no-record": "NO RECORD",
  }[s];
}

/*
 * PLAIN ENGLISH FOR A PERCENTILE ------------------------------------------------------
 *
 * "31st percentile for the date" is precise and, to most people, not information. The
 * words below are the same fact said in a way somebody who has never met a percentile can
 * act on, and they live here rather than in a component so the sheet, a saved spot and the
 * map legend cannot each invent their own phrasing for the same number.
 *
 * NOTHING HERE IS A SECOND OPINION. Every function takes the percentile the arithmetic
 * produced and only chooses words for it. Where the range is wide the words say so, rather
 * than picking the midpoint and sounding confident.
 */

/**
 * "1st", "2nd", "4th" — an ordinal, once, for the whole app.
 *
 * THERE WERE THREE OF THESE and they disagreed. The map's gauge label rounded, the status
 * chip kept a decimal below one per cent, and the estimate's interval clamped to 1–99, so
 * one percentile could read "p0.4th" on a dot and "1st" in the sheet describing that same
 * dot. Each was locally reasonable; together they were the app contradicting itself in the
 * one place a reader is most likely to compare two numbers.
 */
export function ordinal(n: number): string {
  const t = Math.abs(Math.round(n)) % 100;
  const suffix = t >= 11 && t <= 13 ? "th"
    : (["th", "st", "nd", "rd"][Math.round(Math.abs(n)) % 10] ?? "th");
  return `${n}${suffix}`;
}

/**
 * A percentile as a reader sees it: `p4th`, or `p0.4th` down in the tail.
 *
 * ONE DECIMAL BELOW ONE PER CENT, and that is not fussiness — British Columbia in September
 * is full of rivers between the 0th and the 1st percentile, and rounding them all to "p1st"
 * throws away the only distinction that matters down there. Above 1% a whole number is all
 * the underlying number supports.
 *
 * Takes 0–1, like every percentile in this app.
 */
export function percentileLabel(p: number): string {
  const pct = p * 100;
  return `p${ordinal(pct < 1 ? Number(pct.toFixed(1)) : Math.round(pct))}`;
}

/**
 * A catchment, at a readable precision — "742 km²", or "3.4 km²" for a small one.
 *
 * TWO OF THESE DISAGREED. The donor panel kept a decimal below 10 km², because at that size
 * the difference between 3 and 3.4 is most of the size ratio the reader is being asked to
 * judge a gauge by; the gauge badge always rounded, so the same creek read "3 km²" under the
 * map and "3.4 km²" in the panel two taps away. Same class of bug as the four month tables:
 * both were right on their own and the pair was wrong. The panel's rule is the better one
 * and it is now the only one.
 */
export function catchmentLabel(km2: number | null | undefined): string {
  if (km2 == null || !Number.isFinite(km2) || km2 <= 0) return "—";
  return km2 < 10 ? `${km2.toFixed(1)} km²` : `${Math.round(km2).toLocaleString()} km²`;
}

/**
 * The months, once.
 *
 * THERE WERE FOUR OF THESE: full names here, title-case abbreviations in the chart axis,
 * and the same upper-case abbreviations written out twice more in the date pill and the
 * date sheet. Nothing had gone wrong yet — but a calendar is a table every screen needs and
 * exactly the kind of thing that ends up spelled three ways, the way the percentile did.
 */
export const MONTHS: readonly string[] = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

/** "Sep". Callers that want SEP uppercase it; the table stays one table. */
export const monthAbbr = (month0: number): string =>
  (MONTHS[month0] ?? "").slice(0, 3);

/** The month a reader would name, from a date — "early September", "late June". */
export function seasonPhrase(when: Date): string {
  const month = MONTHS[when.getMonth()]!;
  const d = when.getDate();
  return `${d <= 10 ? "early" : d <= 20 ? "mid" : "late"} ${month}`;
}

/**
 * "lower than about 7 days in 10" — a percentile as a count out of ten.
 *
 * OUT OF TEN AND NOT OUT OF A HUNDRED, because the underlying number is not good to a
 * hundredth: the transfer error alone is ±11.7 points at best. Ten is the precision the
 * evidence supports and also the one people picture.
 *
 * Phrased as "lower/higher than N of 10" rather than "in the Nth percentile" because the
 * comparison is the part that means something, and it is the part a percentile hides.
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

/**
 * The headline a reader sees before any number: what the water is doing, in four words.
 *
 * Deliberately NOT `standingWord`. That one is a technical label for a band ("MUCH BELOW
 * NORMAL"), used where the band itself is the point — a legend, a chart axis. This is the
 * sentence at the top of a screen, and it is written the way a person would say it.
 */
export function plainStanding(s: Standing): string {
  return {
    "much-below": "Very low for the time of year",
    below: "Low for the time of year",
    normal: "About normal for the time of year",
    above: "High for the time of year",
    "much-above": "Very high for the time of year",
    "no-record": "No record to compare against",
  }[s];
}

/**
 * How sure to sound, from the width of the interval.
 *
 * The panel's honest output is a RANGE, and a range of 24 points and a range of 6 points
 * are different claims that a bare "7th–31st" presents identically. These words are the
 * difference, and they are calibrated to the measurement: the best possible donor is out by
 * 11.7 points, so an interval under about 25 points wide (±12) is as good as this method
 * gets and deserves to be called reasonably sure.
 */
export function confidenceWord(plusMinus: number): string {
  if (plusMinus <= 13) return "fairly confident";
  if (plusMinus <= 19) return "roughly";
  return "very roughly";
}

/**
 * Percentile bands are stored every 5 days, not every day, and interpolated here.
 * Measured against the full daily envelope for five stations: mean error 0.44%, worst
 * 5.6%, for 325 KB instead of 4.5 MB province-wide.
 *
 * p0 and p100 are deliberately NOT stored this way — every worst case was the record
 * maximum, which is one storm and does not interpolate (82% error). The extremes are
 * cosmetic; the middle is the answer.
 */
export function bandAt(pentads: readonly (Band | null)[], dayOfYear: number): Band | null {
  const step = 5;
  const lo = Math.floor((dayOfYear - 1) / step) * step;
  const a = pentads[lo / step] ?? null;
  const b = pentads[Math.min(lo / step + 1, pentads.length - 1)] ?? null;
  if (a === null) return b;
  if (b === null) return a;
  const f = (dayOfYear - 1 - lo) / step;
  return [0, 1, 2, 3, 4].map((i) => a[i]! + (b[i]! - a[i]!) * f) as unknown as Band;
}

/**
 * How much of a gauge's watershed this reach actually is, by FWA stream magnitude.
 *
 * THE APP DOES NOT DECIDE THIS. There used to be a `gaugeTrust()` here that banded
 * `reach / gauge` — asymmetric, where the pipeline's rule is symmetric, and with no
 * drainage gate at all, which is the whole of what stopped SLESSE CREEK NEAR VEDDER
 * CROSSING speaking for the Chilliwack. It could not have had one: the gate needs FWA
 * watershed codes for two million nodes, and none of that ships to a client.
 *
 * So the band is read, never computed. `pipeline/gauges/consume/shed.py` decides it once and writes
 * it to `section_gauge.trust`; the names and floors below are generated from that same file
 * so the two cannot drift, and `pipeline.tools.emit_gauge_policy --check` fails the build if
 * they do. Changing the rule means changing `shed.py` and rebuilding — which is the point.
 *
 * The Fraser at Hope drains 216,600 km2. Without a floor it "describes" every creek in
 * the valley — 601 reaches on the Chilliwack window alone. With it, 4.
 */
export { TRUST_BANDS, TRUST_FLOOR, type GaugeTrust } from "./gauge-policy.generated";

import type { GaugeTrust as Trust } from "./gauge-policy.generated";

/**
 * What a band is called, in the two places the app says it.
 *
 * ONE TABLE, TWO RENDERINGS, and the reason is a bug that shipped: `GaugeBadge` composed
 * `` `It ${trustWord(t)}.` `` while `trustWord` returned a fragment that only reads after
 * "It" for ONE of the three bands. `fair` rendered "It a major branch of it." and `weak`
 * rendered "It the trend, not the level." — 97.7% of gauged sections in the province
 * bundle, since only 2,724 of 119,103 are `good`.
 *
 * A test aimed at that exact line passed the whole time, because it asserted the fragment
 * it had just interpolated. So the sentence is BUILT HERE, whole, where a test can pin the
 * finished string — a caller that only concatenates cannot reintroduce the fault.
 *
 * There is no entry for "no gauge", because that is not a band — it is the ABSENCE of a
 * row in `section_gauge`, and the caller has to handle a null link before it gets here.
 * A fourth enum value read like a fourth outcome and invited exactly the bug where a reach
 * with no station showed a word instead of a silence.
 */
const TRUST_PHRASE: Record<Trust, { shows: string; clause: string }> = {
  good: { shows: "this water",               clause: "It describes this water" },
  fair: { shows: "a major branch of it",     clause: "It describes a major branch of it" },
  weak: { shows: "the trend, not the level", clause: "It shows the trend, not the level" },
};

/**
 * The band as a VALUE in a table — the right-hand side of "so it shows …".
 * Never interpolate this into prose; that is what `gaugeSentence` is for.
 */
export function trustWord(t: Trust): string {
  return TRUST_PHRASE[t].shows;
}

/**
 * The finished sentence a gauge badge shows: the band, qualified by whether the station is
 * still reporting. All nine combinations are built HERE rather than spliced at the call
 * site, because splicing at the call site is the bug this replaces.
 *
 * `live`: `true` reporting · `false` stopped · `null` we could not check.
 */
export function gaugeSentence(t: Trust, live: boolean | null): string {
  const c = TRUST_PHRASE[t].clause;
  if (live === true) return `${c}.`;
  if (live === null)
    return `${c}. We could not reach the live feed, so whether it is reporting today is ` +
           "unknown.";
  return `${c}, but the station has stopped reporting — there is a record here, not a ` +
         "reading.";
}

/** What the sheet says when nothing is entitled to speak for this water at all. */
export const NO_GAUGE = "no station speaks for this water";

/** …and when a station speaks for the water but has nothing to say about this stretch. */
export const NO_READING_HERE = "no reading on this stretch";
