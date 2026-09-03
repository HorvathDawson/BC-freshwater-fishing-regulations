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
 * What the sheet says about a band.
 *
 * There is no entry for "no gauge", because that is not a band — it is the ABSENCE of a
 * row in `section_gauge`, and the caller has to handle a null link before it gets here.
 * A fourth enum value read like a fourth outcome and invited exactly the bug where a reach
 * with no station showed a word instead of a silence.
 */
export function trustWord(t: Trust): string {
  return {
    good: "describes this water",
    fair: "a major branch of it",
    weak: "the trend, not the level",
  }[t];
}

/** What the sheet says when nothing is entitled to speak for this water at all. */
export const NO_GAUGE = "no station speaks for this water";
