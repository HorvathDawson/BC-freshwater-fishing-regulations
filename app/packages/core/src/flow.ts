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
 * The Fraser at Hope drains 216,600 km2. Without a floor it "describes" every creek in
 * the valley — 601 reaches on the Chilliwack window alone. With it, 4.
 */
export type GaugeTrust = "good" | "fair" | "weak" | "none";

export const TRUST_FLOOR = { good: 0.10, fair: 0.01, weak: 0.001 } as const;

export function gaugeTrust(reachMagnitude: number, gaugeMagnitude: number): GaugeTrust {
  if (gaugeMagnitude <= 0) return "none";
  const share = reachMagnitude / gaugeMagnitude;
  if (share >= TRUST_FLOOR.good) return "good";
  if (share >= TRUST_FLOOR.fair) return "fair";
  if (share >= TRUST_FLOOR.weak) return "weak";
  return "none";
}

/** What the sheet says. "none" is why a reach is drawn dotted, as if ungauged. */
export function trustWord(t: GaugeTrust): string {
  return {
    good: "describes this water",
    fair: "a major branch of it",
    weak: "the trend, not the level",
    none: "no station speaks for this water",
  }[t];
}
