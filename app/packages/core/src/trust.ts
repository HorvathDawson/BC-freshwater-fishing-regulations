/**
 * How wrong a transferred reading is, as a function of how different the two catchments
 * are — and the vocabulary a screen uses to say so.
 *
 * MIRRORS `pipeline/atlas/gauges/panel.py`. `tools/trust-ladder.test.ts` holds the two
 * equal, the same way the magnitude zoom ladder is held: drift is a red test rather than a
 * screen quietly promising a precision the pipeline never claimed.
 *
 * IT IS MEASURED, NOT NAMED. The design this replaces had `good` / `fair` / `weak` — three
 * labels for bands nobody had put a number on, which meant they could not be compared,
 * could not be combined, and could not tell a reader how wrong the answer might be. These
 * come from 9,495 nested gauge pairs, every pair in British Columbia where both ends have a
 * real drainage area and ten years of record, compared day by day.
 *
 * THE TOP ROW IS THE POINT. Even a donor of nearly identical size is out by 11.7 percentile
 * points at the median. So NO estimate built from a donor is precise, at any distance, and
 * every one of them must be drawn as an interval rather than a number. That is also why the
 * eligibility bound could be widened so far: the honesty lives in the interval, not in the
 * threshold.
 */

/** What a reader is told about a donor. Ordered from nearest to most distant. */
export type TrustClass = "close" | "near" | "distant" | "far" | "remote";

/**
 * `(area ratio at or below, class, ± percentile points)`, ascending.
 *
 * The ratio is `max(area) / min(area)` and so is always ≥ 1 and symmetric — it does not
 * matter which of the pair is the donor.
 */
export const ERROR_BY_RATIO: readonly (readonly [number, TrustClass, number])[] = [
  [10, "close", 11.7],
  [100, "near", 15.5],
  [1_000, "distant", 18.5],
  [10_000, "far", 20.6],
  [Infinity, "remote", 22.3],
];

/** Past this the interval cannot separate a low river from a high one, so say nothing. */
export const MAX_USEFUL_SPREAD = 40;

export interface Trust {
  klass: TrustClass;
  /** Half-width of the interval, in PERCENTILE POINTS (0–100), not in fractions. */
  plusMinus: number;
}

/** The class and error bars for a donor this far from the target in catchment size. */
export function trustFor(areaRatio: number): Trust {
  const r = Number.isFinite(areaRatio) && areaRatio >= 1 ? areaRatio : Infinity;
  for (const [limit, klass, plusMinus] of ERROR_BY_RATIO)
    if (r <= limit) return { klass, plusMinus };
  const last = ERROR_BY_RATIO[ERROR_BY_RATIO.length - 1]!;
  return { klass: last[1], plusMinus: last[2] };
}

/**
 * An estimate at a point, as the app must carry it.
 *
 * `percentile` IS 0–1, matching the feed and the existing `standings` map, and every OTHER
 * number here is in percentile POINTS (0–100). That split is deliberate and is stated in
 * both places it appears: the value is a probability and the error is a width, they are
 * read in different units by a person, and a single scale for both is how a ±0.117 ends up
 * rendered as "12%" of something it is not a percentage of.
 *
 * THERE IS NO SENTINEL. The old map carried -0.01 for "gauged, but no history", which every
 * consumer had to know about and any arithmetic silently swallowed. Absence is `null` here,
 * and the reason for it is a separate, readable field.
 */
export interface Estimate {
  /** 0–1. The combined percentile for the day. */
  percentile: number;
  /** Percentile POINTS. Half-width of the honest interval around `percentile`. */
  plusMinus: number;
  /** The weakest donor's class — a panel is only as trustworthy as what it leans on. */
  trust: TrustClass;
  /** How much the donors disagreed, in percentile POINTS. Wide means show a range. */
  spread: number;
  /** How many donors contributed. One is normal; the panel is not always a crowd. */
  donors: number;
}

/** Why there is no estimate. A reader is owed the reason, not a blank. */
export type NoEstimate =
  | "no-station"        // nothing within reach on this water
  | "no-record"         // stations exist but none has enough history for a percentile
  | "regulated"         // the only candidates are dam-controlled and cannot speak for rain
  | "too-uncertain"     // the interval is wider than MAX_USEFUL_SPREAD
  | "offline";          // the feed did not answer; say nothing rather than something stale

export type Answer = { ok: true; value: Estimate } | { ok: false; why: NoEstimate };

/** The interval to draw, in percentile POINTS, clamped to the scale. */
export function interval(e: Estimate): readonly [number, number] {
  const mid = e.percentile * 100;
  const half = Math.max(e.plusMinus, e.spread / 2);
  return [Math.max(0, mid - half), Math.min(100, mid + half)];
}
