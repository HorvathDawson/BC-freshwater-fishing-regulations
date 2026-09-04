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

/**
 * The donor's error in percentile points, INTERPOLATED between the measured rows.
 *
 * MIRRORS `error_for` in panel.py. The rows are five points on a log axis, so this is
 * linear in log10(ratio) between them. Taking the row's number instead makes the ladder a
 * step function, and every donor inside one band then weighs exactly the same.
 *
 * Below 10x it is FLAT at 11.7 — the best the method was ever measured to do. Two donors
 * closer than that are genuinely indistinguishable to this arithmetic, and inventing an
 * ordering between them would be precision the calibration does not support.
 */
export function errorFor(areaRatio: number): number {
  const r = Number.isFinite(areaRatio) && areaRatio >= 1 ? areaRatio : Infinity;
  const [loLimit, , loErr] = ERROR_BY_RATIO[0]!;
  if (r <= loLimit) return loErr;
  let prevLimit = loLimit, prevErr = loErr;
  for (const [limit, , err] of ERROR_BY_RATIO.slice(1)) {
    if (!Number.isFinite(limit)) return err;
    if (r <= limit) {
      const f = (Math.log10(r) - Math.log10(prevLimit))
              / (Math.log10(limit) - Math.log10(prevLimit));
      return prevErr + (err - prevErr) * f;
    }
    prevLimit = limit; prevErr = err;
  }
  return ERROR_BY_RATIO[ERROR_BY_RATIO.length - 1]![2];
}

/**
 * The class and error bars for a donor this far from the target in catchment size.
 *
 * The CLASS is the row the ratio falls in — a word, and a word cannot be interpolated. The
 * error bars are `errorFor`, which can be and is.
 */
export function trustFor(areaRatio: number): Trust {
  const r = Number.isFinite(areaRatio) && areaRatio >= 1 ? areaRatio : Infinity;
  for (const [limit, klass] of ERROR_BY_RATIO)
    if (r <= limit) return { klass, plusMinus: errorFor(r) };
  const last = ERROR_BY_RATIO[ERROR_BY_RATIO.length - 1]!;
  return { klass: last[1], plusMinus: errorFor(r) };
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
  /** The class matching `plusMinus`: the donors' error averaged BY WEIGHT, not the worst. */
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


/* ------------------------------------------------------------------ combining ---- */

/** What a donor contributes: its own percentile for the day, and its facts. */
export interface Contribution {
  /** 0–1, the donor's percentile today, against its OWN record. */
  percentile: number;
  role: "up" | "down";
  /** The donor's catchment, km². */
  areaKm2: number;
  years: number;
}

/** Mirrors `weight_of` in panel.py; `tools/trust-ladder.test.ts` holds the two together. */
const DOWNSTREAM_PENALTY = 0.85;
const RECORD_FULL_YEARS = 20;
const MIN_TOTAL_WEIGHT = 0.05;

/**
 * The three things a donor's weight is made of, unmultiplied.
 *
 * Exported so a screen can SHOW the working — "counts for 83% because it is nearly the
 * same size, sits upstream, and has ten years" — without restating the formula in a
 * component, where it would drift from the one the answer was computed with. `weightFor`
 * is this multiplied out, and nothing else may reimplement either.
 */
export interface WeightFactors {
  /**
   * How informative this donor is, against the best possible one — `(11.7 / its error)²`.
   *
   * NOT the catchment overlap, which is what it used to be and what the name still suggests
   * from a distance. The overlap is the INPUT (it gives the area ratio); this is what the
   * measured error ladder makes of it. A donor of identical size is 1; the most distant one
   * admitted is 0.28.
   */
  share: number;
  /** 1 upstream, `DOWNSTREAM_PENALTY` below — a gauge below you has extra water in it. */
  role: number;
  /** Record length against `RECORD_FULL_YEARS`, capped at 1. Ten years is worth half. */
  record: number;
}

/** The error of the best donor there is. Weights are expressed against it. */
const BEST_ERROR = ERROR_BY_RATIO[0]![2];

export function weightFactors(share: number, role: "up" | "down",
                              years: number): WeightFactors {
  return {
    // INVERSE VARIANCE, not the catchment overlap — see `weight_of` in panel.py for the
    // whole argument. In short: `share` assumes the error grows in proportion to the size
    // difference, and the 9,495-pair calibration says it goes 11.7 -> 22.3 points across
    // four orders of magnitude. A 1,000x donor is about half as informative, not a
    // thousandth, and treating it as a thousandth is what left 244,719 of 249,237 sections
    // holding a panel that said nothing.
    share: share > 0 ? (BEST_ERROR / errorFor(1 / share)) ** 2 : 0,
    role: role === "up" ? 1 : DOWNSTREAM_PENALTY,
    record: Math.min(1, Math.max(0, years / RECORD_FULL_YEARS)),
  };
}

export function weightFor(share: number, role: "up" | "down", years: number): number {
  const f = weightFactors(share, role, years);
  return f.share * f.role * f.record;
}

/** Φ, the normal CDF, via the error function. */
function normCdf(z: number): number {
  // Abramowitz & Stegun 7.1.26 — good to 1.5e-7, which is far past what a percentile
  // rounded to a whole point can notice.
  const t = 1 / (1 + 0.3275911 * Math.abs(z) / Math.SQRT2);
  const y = 1 - (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t
                  - 0.284496736) * t + 0.254829592) * t * Math.exp(-(z * z) / 2);
  return z >= 0 ? 0.5 * (1 + y) : 0.5 * (1 - y);
}

/**
 * Φ⁻¹, in closed form (Acklam's rational approximation).
 *
 * WAS SIXTY ITERATIONS OF BISECTION, on the reasoning that this is "called a handful of
 * times per tap". That was true when only the sheet used it. The map now runs the same
 * arithmetic over every reach in the viewport — thousands of them, on every pan — and sixty
 * evaluations of the error function per donor is the difference between colour appearing
 * and colour arriving. Measured: 5.15 ms against 0.23 ms for 5,000 reaches of three donors.
 *
 * It agrees with the bisection to 2.0e-5 across p = 0.001..0.999, which is four orders of
 * magnitude finer than a percentile rounded to a whole point can express. `trust.test.ts`
 * holds the two together so the approximation cannot drift.
 */
export function probit(p: number): number {
  const q = Math.min(1 - 1e-9, Math.max(1e-9, p));
  const A = [-3.969683028665376e1, 2.209460984245205e2, -2.759285104469687e2,
             1.38357751867269e2, -3.066479806614716e1, 2.506628277459239];
  const B = [-5.447609879822406e1, 1.615858368580409e2, -1.556989798598866e2,
             6.680131188771972e1, -1.328068155288572e1];
  const C = [-7.784894002430293e-3, -3.223964580411365e-1, -2.400758277161838,
             -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const D = [7.784695709041462e-3, 3.224671290700398e-1, 2.445134137142996,
             3.754408661907416];
  // The central branch is a ratio of polynomials in (q - 1/2); the tails are the same shape
  // in sqrt(-2 ln q), because the central fit loses all its accuracy out there.
  const LOW = 0.02425;
  if (q < LOW) {
    const t = Math.sqrt(-2 * Math.log(q));
    return (((((C[0]! * t + C[1]!) * t + C[2]!) * t + C[3]!) * t + C[4]!) * t + C[5]!)
         / ((((D[0]! * t + D[1]!) * t + D[2]!) * t + D[3]!) * t + 1);
  }
  if (q > 1 - LOW) {
    const t = Math.sqrt(-2 * Math.log(1 - q));
    return -(((((C[0]! * t + C[1]!) * t + C[2]!) * t + C[3]!) * t + C[4]!) * t + C[5]!)
          / ((((D[0]! * t + D[1]!) * t + D[2]!) * t + D[3]!) * t + 1);
  }
  const t = q - 0.5, r = t * t;
  return (((((A[0]! * r + A[1]!) * r + A[2]!) * r + A[3]!) * r + A[4]!) * r + A[5]!) * t
       / (((((B[0]! * r + B[1]!) * r + B[2]!) * r + B[3]!) * r + B[4]!) * r + 1);
}

/**
 * A panel plus today's readings → one answer, or a reason there is none.
 *
 * COMBINED IN PROBIT SPACE. Percentiles are uniform on [0,1] and averaging uniforms
 * concentrates toward 0.5 — so a point served by three donors would report closer to normal
 * than one served by one, which is backwards: the app would play down extremes exactly
 * where it knows the most. Mapping each to a normal quantile, averaging there, and mapping
 * back removes that.
 *
 * THE INTERVAL IS THE WIDER OF TWO THINGS: how far apart the donors are, and how wrong a
 * donor at this distance is known to be. Donor agreement alone would report maximum
 * confidence when three gauges on one river agree — which they do because they are the same
 * river, not because the answer is certain.
 */
export function estimate(targetAreaKm2: number | null,
                         contributions: readonly Contribution[]): Answer {
  if (!contributions.length) return { ok: false, why: "no-station" };
  if (!targetAreaKm2 || targetAreaKm2 <= 0) return { ok: false, why: "no-station" };

  const votes: { z: number; w: number; ratio: number }[] = [];
  for (const c of contributions) {
    if (!(c.percentile > 0 && c.percentile < 1)) continue;
    if (!c.areaKm2 || c.areaKm2 <= 0) continue;
    const ratio = Math.max(targetAreaKm2, c.areaKm2) / Math.min(targetAreaKm2, c.areaKm2);
    const w = weightFor(Math.min(targetAreaKm2, c.areaKm2)
                        / Math.max(targetAreaKm2, c.areaKm2), c.role, c.years);
    if (w > 0) votes.push({ z: probit(c.percentile), w, ratio });
  }
  if (!votes.length) return { ok: false, why: "no-record" };
  const total = votes.reduce((a, v) => a + v.w, 0);
  if (total < MIN_TOTAL_WEIGHT) return { ok: false, why: "too-uncertain" };

  const mean = votes.reduce((a, v) => a + v.z * v.w, 0) / total;
  const varZ = votes.reduce((a, v) => a + v.w * (v.z - mean) ** 2, 0) / total;
  const sd = Math.sqrt(varZ);
  const spread = (normCdf(mean + sd) - normCdf(mean - sd)) * 100;

  /*
   * TRUST FOLLOWS THE WEIGHTS, not the worst member.
   *
   * This took the most distant donor, on the reasoning that a close gauge does not redeem
   * a far one because they all voted. True in principle and wrong in proportion: weight
   * falls with distance, so the far ones barely vote. Real example from the built bundle —
   * a 1.34 km2 section with donors at 3.4, 32 and 207 km2. The nearest is 2.5x away and
   * carries almost all the weight; the farthest is 155x away and carries 0.006 of it. The
   * worst-member rule reported that panel as "distant" on the strength of a donor that
   * moved the answer by a fraction of a point.
   *
   * So the error is the weighted mean of each donor's own error, which is what "they all
   * voted" actually means once you account for how much.
   */
  const plusMinus = votes.reduce((a, v) => a + trustFor(v.ratio).plusMinus * v.w, 0) / total;
  // The CLASS is a word, and a word cannot be averaged — so it is the class of the ratio
  // that error corresponds to, which keeps the two halves of the label consistent.
  const klass = ERROR_BY_RATIO.find(([, , e]) => plusMinus <= e)?.[1]
    ?? ERROR_BY_RATIO[ERROR_BY_RATIO.length - 1]![1];

  const value: Estimate = {
    percentile: normCdf(mean),
    plusMinus,
    trust: klass,
    spread,
    donors: votes.length,
  };
  const [lo, hi] = interval(value);
  if (hi - lo > MAX_USEFUL_SPREAD) return { ok: false, why: "too-uncertain" };
  return { ok: true, value };
}
