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

import { MIN_RECORD_YEARS, MIN_TOTAL_WEIGHT, type RecordQuantity } from "./gauge-policy.generated";

export { MIN_RECORD_YEARS, RECORD_QUANTITIES, type RecordQuantity } from "./gauge-policy.generated";

/**
 * A station's record, in years, PER QUANTITY (T110): `{ discharge, level }`, 0 where it has none — the bundle's
 * `discharge_years` / `level_years`. Never one number for both: 07ED001 has 59 years of discharge and 4 of level.
 */
export type RecordYears = Readonly<Record<RecordQuantity, number>>;

/**
 * THE ONE READ of a station's record: the years it has in the quantity shown. A Flow answer weighs the discharge
 * record, a Level answer the level one, because each percentile was ranked against that history. Mirrors
 * `record_years` in pipeline/gauges/feed/record.py; nothing else reduces a record to one number.
 */
export function recordYears(record: RecordYears, quantity: RecordQuantity): number {
  return record[quantity] ?? 0;
}

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
  | "offline"           // the feed did not answer; say nothing rather than something stale
  | "own-gauge"         // a gauged lake: its own station reads it, so nothing is estimated (T105 review)
  | "no-forecast"       // a day ahead that no forecast run for these gauges reaches
  | "flow-forecast-only"; // a day ahead under Level: the runs here forecast FLOW that day, no stage (T143)

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
  /** On the same blue line as the reach — see `PanelMember.sameRiver`. */
  sameRiver?: boolean;
  /**
   * This donor is the reach's OWN gauge and may lead the answer: its rank (1 the nearest along the water), absent or
   * 0 when it may not. The pipeline decides it (`may_lead` in panel.py: not far, not much smaller); see `leadShares`.
   */
  lead?: number;
  /**
   * This donor sits in the panel's RESERVE — past its first `CORE_MEMBERS` — and stands back rather than silence the
   * core (`answering`). Absent or false: a core member, or a caller with no panel order.
   */
  reserve?: boolean;
}

/**
 * THE CORE OF A PANEL: its first this-many members, best weight first — the panel as it stood before the reserve
 * (`MAX_MEMBERS` 4, the best four overall). MIRRORS `CORE_MEMBERS` in panel.py; `tools/trust-ladder.test.ts` holds the
 * two equal.
 *
 * The panel behind it is a ranked reserve, every station that could describe the water (owner 2026-10-22, "estimates
 * use every gauge that could inform a section, chosen live"), so that a silent member leaves an answer. A reserve
 * member is there to ANSWER, never to silence: measured on the live graph (feed 2026-10-21T18:00Z), letting every
 * reporting reserve member vote made 54 Level sections (384 km) lose an answer their core gave on the same feed —
 * the Liard and the Thompson, where far tributary gauges disagree and the spread gate refuses. So where the reserve
 * would make the answer too uncertain while a core member reports, the lowest-weight reserve members stand back,
 * one at a time. The core's own disagreement still refuses, exactly as it did; and where the whole core is silent the
 * reserve IS the panel, and its disagreement refuses too.
 */
export const CORE_MEMBERS = 4;

/* ---------------------------------------------------------- a water's own gauge comes first --- */

/**
 * OWNER 2026-10-22: "own gauge should be main (unless far from click) low magnitude compared to click spot", and "it
 * can still use auxiliary gauges". MIRRORS `OWN_LEAD_SHARE` in panel.py; `tools/trust-ladder.test.ts` holds the number
 * equal and `tools/one-formula.test.ts` runs both implementations of `leaderOf` / `leadShares` side by side.
 *
 * A leading own gauge carries at least this share of the answer; the auxiliaries split the rest in their usual
 * proportion. WHO may lead is the pipeline's (it alone has the water, the distance and both catchments) and arrives
 * as each member's `lead` rank; this is only what leading means.
 */
export const OWN_LEAD_SHARE = 0.7;

/**
 * The index of the vote that leads: the best-ranked own gauge (`lead` 1 is the nearest) whose weight is above zero —
 * i.e. which is REPORTING the quantity asked for. That is the whole of "never mix quantities": a level-only own gauge
 * has no discharge reading, so it is not a vote in Flow and cannot lead it; in Level it can. A quiet rank 1 hands the
 * lead to rank 2. Null when no own gauge may lead.
 */
export function leaderOf(ranks: readonly number[], weights: readonly number[]): number | null {
  let best: number | null = null;
  for (let i = 0; i < ranks.length; i++) {
    const r = ranks[i] ?? 0, w = weights[i] ?? 0;
    if (r > 0 && w > 0 && (best === null || r < ranks[best]!)) best = i;
  }
  return best;
}

/**
 * Each vote's share of the answer, 0..1, summing to 1: the usual weights normalised, then — with a leader — the leader
 * raised to at least `OWN_LEAD_SHARE` and the others scaled into what is left. A leader already carrying more keeps
 * it: leading is a floor. Alone, it is the whole answer. MIRRORS `lead_shares` in panel.py.
 */
export function leadShares(weights: readonly number[], lead: number | null): number[] {
  const total = weights.reduce((a, w) => a + w, 0);
  if (total <= 0) return weights.map(() => 0);
  const norm = weights.map((w) => w / total);
  if (lead === null || !(weights[lead]! > 0) || weights.filter((w) => w > 0).length < 2) return norm;
  const own = Math.max(OWN_LEAD_SHARE, norm[lead]!);
  const rest = total - weights[lead]!;
  return weights.map((w, i) => (i === lead ? own : (1 - own) * w / rest));
}

/** Mirrors `weight_of` in panel.py; `tools/trust-ladder.test.ts` holds the two together. */
const DOWNSTREAM_PENALTY = 0.85;
const RECORD_FULL_YEARS = 20;
// MIN_TOTAL_WEIGHT is generated from panel.py (`gauge-policy.generated`, T125 item 6), never typed here.

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
  /** Record length (in the quantity asked) against `RECORD_FULL_YEARS`, capped at 1. Ten years is worth half; under
   *  `MIN_RECORD_YEARS` it is 0 — such a record ranks nothing (the panel's gate, T110). */
  record: number;
}

/** The error of the best donor there is. Weights are expressed against it. */
const BEST_ERROR = ERROR_BY_RATIO[0]![2];

/**
 * How much worse a donor on ANOTHER river is than one on your own, in error terms.
 *
 * MIRRORS `TRIBUTARY_ERROR_FACTOR` in panel.py, and unlike `ERROR_BY_RATIO` it is a
 * JUDGEMENT rather than a measurement: the 9,495-pair calibration was run over nested pairs
 * without asking whether the pair shared a channel, so it has no opinion here. Doubling the
 * error — quartering the weight — is conservative next to the 54-point disagreement the
 * Skeena produced when the two were treated alike.
 */
const TRIBUTARY_ERROR_FACTOR = 2;

export function weightFactors(share: number, role: "up" | "down",
                              years: number,
                              sameRiver = true): WeightFactors {
  return {
    // INVERSE VARIANCE, not the catchment overlap — see `weight_of` in panel.py for the
    // whole argument. In short: `share` assumes the error grows in proportion to the size
    // difference, and the 9,495-pair calibration says it goes 11.7 -> 22.3 points across
    // four orders of magnitude. A 1,000x donor is about half as informative, not a
    // thousandth, and treating it as a thousandth is what left 244,719 of 249,237 sections
    // holding a panel that said nothing.
    share: share > 0
      ? (BEST_ERROR / (errorFor(1 / share) * (sameRiver ? 1 : TRIBUTARY_ERROR_FACTOR))) ** 2
      : 0,
    role: role === "up" ? 1 : DOWNSTREAM_PENALTY,
    record: recordFactor(years),
  };
}

/**
 * THE RECORD FACTOR — how much a percentile ranked against `years` of history counts, 0..1: `years / RECORD_FULL_YEARS`
 * capped at 1, and 0 under `MIN_RECORD_YEARS` (a record that short ranks nothing). ONE function for the reach panels
 * (`weightFactors`) and the basin field (`basinStanding`), mirroring `weight_of`'s `rec` in panel.py: the basin field
 * had its own copy of the ratio, which the floor (T110) did not reach.
 */
export function recordFactor(years: number): number {
  return years < MIN_RECORD_YEARS ? 0 : Math.min(1, Math.max(0, years / RECORD_FULL_YEARS));
}

export function weightFor(share: number, role: "up" | "down", years: number,
                          sameRiver = true): number {
  const f = weightFactors(share, role, years, sameRiver);
  return f.share * f.role * f.record;
}

/** Φ, the normal CDF, via the error function. */
export function normCdf(z: number): number {
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
  return answering(targetAreaKm2, contributions).answer;
}

type Vote = { z: number; w: number; ratio: number; sameRiver: boolean; lead: number; reserve: boolean; at: number };

/**
 * `estimate`, and which contributions STOOD BACK from it (their indices): the reserve members (`CORE_MEMBERS`) left
 * out because bringing them in made the answer too uncertain while a core member reports. The lowest-weight reserve
 * vote stands back first, then the next, until the answer is usable or no reserve vote is left. A table under the
 * answer gives those zero, as the answer did.
 */
export function answering(targetAreaKm2: number | null, contributions: readonly Contribution[]):
    { answer: Answer; stoodBack: ReadonlySet<number> } {
  const stoodBack = new Set<number>();
  if (!contributions.length) return { answer: { ok: false, why: "no-station" }, stoodBack };
  if (!targetAreaKm2 || targetAreaKm2 <= 0) return { answer: { ok: false, why: "no-station" }, stoodBack };

  const votes: Vote[] = [];
  contributions.forEach((c, at) => {
    if (!(c.percentile > 0 && c.percentile < 1)) return;
    if (!c.areaKm2 || c.areaKm2 <= 0) return;
    const ratio = Math.max(targetAreaKm2, c.areaKm2) / Math.min(targetAreaKm2, c.areaKm2);
    const same = c.sameRiver !== false;
    const w = weightFor(Math.min(targetAreaKm2, c.areaKm2)
                        / Math.max(targetAreaKm2, c.areaKm2), c.role, c.years, same);
    if (w > 0) votes.push({ z: probit(c.percentile), w, ratio, sameRiver: same, lead: c.lead ?? 0,
                            reserve: c.reserve === true, at });
  });
  let answer = combineVotes(votes);
  if (!votes.some((v) => !v.reserve)) return { answer, stoodBack };
  while (!answer.ok && answer.why === "too-uncertain") {
    let drop = -1;
    votes.forEach((v, i) => { if (v.reserve && (drop < 0 || v.w <= votes[drop]!.w)) drop = i; });
    if (drop < 0) break;
    stoodBack.add(votes[drop]!.at);
    votes.splice(drop, 1);
    answer = combineVotes(votes);
  }
  return { answer, stoodBack };
}

function combineVotes(votes: readonly Vote[]): Answer {
  if (!votes.length) return { ok: false, why: "no-record" };
  const total = votes.reduce((a, v) => a + v.w, 0);
  if (total < MIN_TOTAL_WEIGHT) return { ok: false, why: "too-uncertain" };

  // A WATER'S OWN GAUGE COMES FIRST (owner 2026-10-22): the shares below are the usual weights normalised, with the
  // leading own gauge — if one may lead and is reporting — raised to OWN_LEAD_SHARE. Everything after reads `shares`.
  const leader = leaderOf(votes.map((v) => v.lead), votes.map((v) => v.w));
  const shares = leadShares(votes.map((v) => v.w), leader);
  const mean = votes.reduce((a, v, i) => a + v.z * shares[i]!, 0);
  const varZ = votes.reduce((a, v, i) => a + shares[i]! * (v.z - mean) ** 2, 0);
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
  const plusMinus = votes.reduce((a, v, i) => a + trustFor(v.ratio).plusMinus * shares[i]!, 0);
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
  /*
   * A DIRECT MEASUREMENT IS NOT OVERRULED BY ITS NEIGHBOURS.
   *
   * The spread refusal exists for a panel assembled entirely from other water: four
   * gauges on four tributaries disagreeing by fifty points genuinely cannot tell a low
   * river from a high one, and saying so is right.
   *
   * It is wrong the moment a gauge is standing IN this river and reporting. The Skeena is
   * the case: three of its own gauges at the 77th, 78th and 82nd percentile, plus the
   * Babine at the 24th — and the app drew "no baseline" over a river it was directly
   * measuring three times, because one tributary disagreed. The tributary is already
   * discounted fourfold by `sameRiver`; discarding the whole answer on its account throws
   * away the measurement to honour the inference.
   *
   * The disagreement is not hidden — the panel says "these gauges disagree by N points"
   * above the working, and every donor is listed with what it read. What changes is that
   * the reader is given the number the river's own gauges produced.
   */
  // A leading own gauge is a measurement of this water too, whatever blue line FWA drew it on.
  const measured = votes.some((v) => v.sameRiver) || leader !== null;
  const [lo, hi] = interval(value);
  if (!measured && hi - lo > MAX_USEFUL_SPREAD)
    return { ok: false, why: "too-uncertain" };
  return { ok: true, value };
}


/* ------------------------------------------------------------------- the basin field --- */

/*
 * NO SECOND CONSTANT. This was `BASIN_RECORD_FULL_YEARS = 30`, under a comment saying it
 * mirrored the panel's — and the panel's is 20 (`RECORD_FULL_YEARS` above, and
 * `panel.py:117`). A comment asserting agreement while the numbers differed is worse than
 * two numbers with no comment: it tells the next reader the reconciliation is already done.
 *
 * The basin field weights a gauge by record length for the same reason the reach panels do,
 * so it is the same ceiling, and there is now only one place to change it.
 */

/** One gauge's contribution to a watershed group's colour. */
export interface BasinVote {
  /** Where this station's reading sits in its own history, 0..1. */
  percentile: number;
  /** Its catchment — how much of the group it observes. */
  areaKm2: number | null;
  /** Years of record — how well its own baseline is known. */
  years: number;
}

/**
 * A watershed group's standing, from every gauge standing in it.
 *
 * THE FIELD USED TO ELECT ONE STATION per group — the largest catchment — and colour the
 * group by it. That made the answer hostage to that one gauge in two ways, both of which
 * showed up as a blank group on a river anyone can see is gauged: the elected station might
 * have no climatology at all (13 groups, the Skeena among them), or it might simply not be
 * transmitting this hour (Chilliwack, which has six gauges and went grey because the one
 * named was quiet).
 *
 * THE METRIC. Percentiles are bounded and non-linear — the distance from the 50th to the
 * 60th is not the distance from the 88th to the 98th — so averaging them directly flattens
 * exactly the extremes the map exists to show. They are combined in PROBIT SPACE instead,
 * through the same `probit`/`normCdf` pair `estimate` uses for the reaches, so a catchment
 * and the rivers inside it cannot disagree because of arithmetic.
 *
 * THE WEIGHTS are catchment times record:
 *
 *   · CATCHMENT, because a station's drainage area is how much of the group it actually
 *     observes. A gauge on the mainstem speaks for most of the group; a creek gauge speaks
 *     for a corner of it. Weighting is by AREA SHARE rather than raw area so the number is
 *     a proportion of what is observed here, not a quantity that a big group would inflate.
 *   · RECORD (`recordFactor`), saturating at RECORD_FULL_YEARS, because a percentile is a claim about
 *     history and a three-year baseline is a weaker claim than a ninety-year one — the
 *     same ceiling as the reach panels, and now literally the same function — floor included:
 *     a record under MIN_RECORD_YEARS weighs nothing here either (T110).
 *
 * What is NOT here is a direction penalty or an area-ratio error term. Those exist in the
 * panel because it TRANSFERS a reading from a donor to a specific reach it is not standing
 * in. Nothing is transferred here: every one of these gauges is inside the group, measuring
 * part of the thing being coloured, and inventing a distance term would give the field a
 * precision it has not got.
 */
export function basinStanding(votes: readonly BasinVote[]): number | null {
  let sumW = 0;
  let sumWZ = 0;
  const totalArea = votes.reduce((a, v) => a + Math.max(0, v.areaKm2 ?? 0), 0);
  for (const v of votes) {
    if (!Number.isFinite(v.percentile)) continue;
    // A group of stations that all report no catchment still has to answer, so share falls
    // back to an equal split rather than to zero weight for everybody.
    const share = totalArea > 0 ? Math.max(0, v.areaKm2 ?? 0) / totalArea
                                : 1 / Math.max(1, votes.length);
    const record = recordFactor(v.years);   // the panels' factor, floor and all (T110)
    const w = share * record;
    if (w <= 0) continue;
    sumW += w;
    sumWZ += w * probit(v.percentile);
  }
  // Nothing reporting, or nothing with any record: the group says nothing, which the field
  // draws as the same unmeasured grey a group with no gauge gets.
  if (sumW <= 0) return null;
  return normCdf(sumWZ / sumW);
}
