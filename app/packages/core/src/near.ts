/**
 * WATER NEAR A TOWN — which of them to list first.
 *
 * "Water near Smithers" is 91 waters within 25 km. Nearest-first opened on Chicken Lake
 * Creek, Dahlie Creek and Kathlyn Creek, with the Bulkley — the river the town stands on —
 * fourth and the Zymoetz and Telkwa far down the list: sorted by the one thing a reader
 * asking "where can I fish near here" cares least about on its own. So the order is a
 * SCORE — how much water this is, discounted by how far away it is:
 *
 *     score = importance / (1 + km / D0)
 *
 *     importance = 1                                     every water counts for something
 *                + W.mag     · n(magnitude, REF.mag)     how much water drains through it
 *                + W.length  · n(pieces,    REF.pieces)  how long it is
 *                + W.towns   · n(towns,     REF.towns)   how much ground it covers
 *                + W.gauged  · [a gauge reads it]
 *                + W.stocked · [it is stocked]
 *                + W.listed  · [the synopsis names it]
 *
 *     n(x, ref) = min(1, ln(1 + x) / ln(1 + ref))  —  0 for nothing, 1 at `ref` and above
 *
 * WHY THIS SHAPE.
 *
 *  - LOGS, because every size signal spans orders of magnitude. The Fraser's magnitude is
 *    296,885 and a decent creek's is 300; linear, the Fraser would be every other water's
 *    importance a thousand times over and the list would be "the Fraser, then by distance".
 *    Normalised to 0..1 each, no one signal can swamp the rest.
 *  - A HYPERBOLIC decay, not exp(−d/D0). With an exponential the Fraser 20 km away loses to a
 *    nameless trickle at 2 km (e^−2 is 0.14); 1/(1 + d/D0) is 0.2 there, which keeps a big
 *    river in the list while still letting a good creek on the doorstep lead.
 *  - The baseline 1, so a water with no signals at all (most small creeks) is ranked by
 *    distance among its peers rather than tied at zero.
 *
 * WHAT THE BUNDLE HAS, AND WHAT IT DOES NOT. Magnitude is `section_gauge.mag`, the same
 * figure search ranks by; length is the count of reaches the water is cut into (the bundle
 * holds no metres); "towns" is how many places it comes within 25 km of, which is the only
 * measure of extent a LAKE has. There is NO lake area in the bundle, so a lake — which has
 * no magnitude either — takes `LAKE_MAG` in place of one: zero would say "a trickle", and
 * the lakes are half of what people fish near a town. `stocked` reads `stock_water`, empty
 * in the current build, so it is inert until that table is filled. `listed` is whether the
 * synopsis has an entry for the water — a notability signal only; nothing about what the
 * entry says is read, shown or implied.
 *
 * The tuning lives in ONE block, `NEAR_RANK`. Change a number there and the pinned cases in
 * `near.test.ts` say whether the intended order still holds.
 */

/** What the bundle says about how much a water is. Facts only; the weighing is below. */
export interface WaterSignals {
  /** FWA stream magnitude of its biggest reach. Null for a lake, and for an unmeasured stream. */
  mag: number | null;
  /** How many reaches it is cut into — the bundle's only measure of length. */
  pieces: number;
  /** How many places it comes within 25 km of — extent, for a lake the only one there is. */
  towns: number;
  /** A hydrometric station reads it. */
  gauged: boolean;
  /** It has stocking records. */
  stocked: boolean;
  /** The synopsis has an entry naming it. */
  listed: boolean;
}

/**
 * THE TUNING. Every number the order depends on, in one place.
 *
 * Read the weights as "how many units of baseline importance the signal is worth at full
 * strength". Magnitude leads because it is the truest measure of how much water there is.
 */
export const NEAR_RANK = {
  /**
   * Distance scale, km: a water D0 away counts half what it would on the doorstep; at the
   * 25 km edge, a sixth. 5 km is the size of a town and its outskirts.
   */
  D0: 5,
  W: {
    mag: 3,
    length: 1,
    /** Half: a creek in the dense Fraser valley is "near" many places without being big. */
    towns: 0.5,
    /** Half: small creeks carry old gauges too; a gauge says someone cared, not how much. */
    gauged: 0.5,
    stocked: 1,
    listed: 1,
  },
  /** Where each size signal reaches 1. The Fraser is ~297k; Kamloops Lake is near 60 towns. */
  REF: { mag: 100_000, pieces: 100, towns: 200 },
  /**
   * A lake's stand-in for the magnitude it cannot have, as a fraction of full: 0.45 is a
   * stream of magnitude ~180, a respectable creek. Without it no lake reached the first ten
   * near Kamloops, whose lakes are what the town fishes.
   */
  LAKE_MAG: 0.45,
} as const;

/** A signal on 0..1: log-scaled against its reference, capped at 1. */
export function normSignal(x: number | null, ref: number): number {
  if (x === null || !(x > 0)) return 0;
  return Math.min(1, Math.log1p(x) / Math.log1p(ref));
}

/** How much water this is, before distance. At least 1. */
export function importance(kind: string, s: WaterSignals): number {
  const { W, REF } = NEAR_RANK;
  const mag = s.mag === null && kind === "lake" ? NEAR_RANK.LAKE_MAG : normSignal(s.mag, REF.mag);
  return 1
    + W.mag * mag
    + W.length * normSignal(s.pieces, REF.pieces)
    + W.towns * normSignal(s.towns, REF.towns)
    + (s.gauged ? W.gauged : 0)
    + (s.stocked ? W.stocked : 0)
    + (s.listed ? W.listed : 0);
}

/** THE OBJECTIVE: importance, discounted by distance. */
export function nearScore(h: { kind: string; km: number; signals: WaterSignals }): number {
  return importance(h.kind, h.signals) / (1 + Math.max(0, h.km) / NEAR_RANK.D0);
}

/**
 * Waters near a town, most worth listing first. Ties (rare — equal signals AND distance)
 * fall to the nearer, then to the name, so the order never depends on the input's.
 */
export function rankNear<T extends { name: string; kind: string; km: number;
                                     signals: WaterSignals }>(hits: readonly T[]): T[] {
  return hits
    .map((h) => ({ h, s: nearScore(h) }))
    .sort((a, b) => b.s - a.s || a.h.km - b.h.km || a.h.name.localeCompare(b.h.name))
    .map((x) => x.h);
}
