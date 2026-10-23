/**
 * "LIKE YOUR VISIT?" (T51, owner 2026-10-22): how alike each of the coming days looks to the day a spot was fished, as
 * a percentage — "when you view a spot … compare how it was at that time to the next 5 days … a percentage score of
 * similarity to when the spot was gone to."
 *
 * PURE, AND THE ONE PLACE THE FORMULA AND ITS WORDS LIVE. The app computes it from the spot's frozen record and the
 * live feed and forecast (no regulation logic is involved); `@app/data/spots` gathers the inputs, `@app/ui` arranges
 * them, the strip draws what this returns.
 *
 * THE SCORE. A weighted mean of per-factor likenesses over the factors known on BOTH days:
 *
 *   likeness(factor) = max(0, 1 − |then − that day| / SPAN[factor])         (1 = the same, 0 = a span or more apart)
 *   score            = Σ WEIGHT·likeness / Σ WEIGHT, over the factors with both values; shown as a whole per cent
 *
 * so an identical day is 100 %, and one factor moving alone lowers the score by exactly its weight's share of how far
 * it moved (flow 25 percentile points apart alone: 1 − 0.45 × 0.5 = 77.5 → 78 %). A factor unknown on either side is
 * left out and the rest re-normalise — and the day SAYS which were left out; it is never filled with a guess.
 *
 * FLOW WEIGHS MOST (the owner cares about flow) and is FLOW ONLY: the gauge's DISCHARGE percentile for the date, never
 * a level (decisions.md "NEVER MIX QUANTITIES"). A visit with no flow is scored on the weather alone, said as that
 * ("No flow then to compare"); a coming day no forecast reaches the same ("No flow forecast for this day").
 */
import { monthAbbr, percentileOrdinal, weekdayAbbr } from "./flow";
import { localDate, shiftDay } from "./day";
import { agoWords } from "./menu";

/** The factors, in the order a day's working lists them. */
export type LikeFactorKey = "flow" | "temp" | "rain" | "wind" | "cloud";
export const LIKE_FACTORS: readonly LikeFactorKey[] = ["flow", "temp", "rain", "wind", "cloud"];

/** Each factor's weight; they sum to 1. Flow most (owner), then the air and the rain before, then wind and cloud. */
export const LIKE_WEIGHTS: Readonly<Record<LikeFactorKey, number>> =
  { flow: 0.45, temp: 0.2, rain: 0.2, wind: 0.1, cloud: 0.05 };

/**
 * How far apart a factor must be to count as not alike at all (likeness 0), in its own unit. Linear in between.
 *   flow   0.5   half the record apart (the 20th percentile against the 70th)
 *   temp   12    °C, the air at the visit's hour
 *   rain   15    mm, the rain over the LIKE_RAIN_DAYS days before
 *   wind   25    km/h, at the visit's hour
 *   cloud  100   % cover, at the visit's hour (clear against overcast)
 */
export const LIKE_SPANS: Readonly<Record<LikeFactorKey, number>> =
  { flow: 0.5, temp: 12, rain: 15, wind: 25, cloud: 100 };

/** The one-word read: at or above `verySimilar` (a fraction of 1), at or above `similar`, else different. */
export const LIKE_READS = { verySimilar: 0.85, similar: 0.65 } as const;

/** How many coming days the strip shows: today and the next four. */
export const LIKE_DAYS = 5;

/** The rain factor's window: the days before (the visit's own lead-up, or the coming day's). */
export const LIKE_RAIN_DAYS = 3;

/** One day's conditions, every value nullable. The weather values are at the visit's own hour of day. */
export interface LikeConditions {
  /** The gauge's DISCHARGE percentile for the date, 0–1. Never a level. */
  flow: number | null;
  tempC: number | null;
  /** Rain over the LIKE_RAIN_DAYS days before, mm. */
  rainMm: number | null;
  windKph: number | null;
  cloudPct: number | null;
}

export type LikeRead = "very similar" | "similar" | "different";

export interface LikeFactor {
  key: LikeFactorKey;
  /** 0–1, or null when it was left out. */
  likeness: number | null;
  /** Its share of this day's score (its weight re-normalised over the factors scored), 0–1; 0 when left out. */
  share: number;
  /** The factor said whole: both values, how far apart, how alike, its share — or why it was left out. */
  line: string;
}

export interface LikeDay {
  /** The LOCAL day, "2026-10-24". */
  date: string;
  /** 0–100, whole; null when no factor could be compared. */
  score: number | null;
  read: LikeRead | null;
  /** "weather" when flow could not be compared and the score is the weather's alone. */
  basis: "all" | "weather" | null;
  /** Why flow is not in it, when it is not ("No flow then to compare."), else null. */
  note: string | null;
  factors: LikeFactor[];
}

const round1 = (v: number) => Math.round(v * 10) / 10;
const pct = (f: number) => `${Math.round(f * 100)}%`;

/** A factor's likeness: 1 identical, 0 a span or more apart. Exported so the arithmetic is pinned alone. */
export function likeness(key: LikeFactorKey, then: number, that: number): number {
  return Math.max(0, 1 - Math.abs(then - that) / LIKE_SPANS[key]);
}

export function likeRead(score01: number): LikeRead {
  return score01 >= LIKE_READS.verySimilar ? "very similar"
    : score01 >= LIKE_READS.similar ? "similar" : "different";
}

const READS: Readonly<Record<LikeRead, string>> =
  { "very similar": "Very similar", similar: "Similar", different: "Different" };
const LABELS: Readonly<Record<LikeFactorKey, string>> =
  { flow: "Flow", temp: "Air temperature", rain: "Rain in the 3 days before", wind: "Wind", cloud: "Cloud" };

export const LIKE_WORDS = {
  heading: "Like your visit?",
  intro: "How alike each of the next five days looks to the day you were here. Flow counts most, then the air and "
    + "the rain in the three days before, then wind and cloud. Tap a day to see why.",
  visit: "Your visit",
  today: "Today",
  tomorrow: "Tomorrow",
  noFlowThen: "No flow then to compare: scored on the weather only.",
  noFlowAhead: "No flow forecast for this day: scored on the weather only.",
  noScore: "Nothing to compare for this day.",
  nothingThen: "Nothing was recorded at this visit to compare with: no flow, no weather.",
  never: "The forecast has not been fetched yet. It is fetched when you are online.",
  weatherOnly: "weather only",
  dash: "—",
  reads: READS,
  labels: LABELS,
} as const;

/** One value in its unit: "42nd percentile", "14°C", "2.5 mm", "8 km/h", "40%". */
function said(key: LikeFactorKey, v: number): string {
  switch (key) {
    case "flow": return `${percentileOrdinal(v)} percentile`;
    case "temp": return `${Math.round(v)}°C`;
    case "rain": return `${round1(v)} mm`;
    case "wind": return `${Math.round(v)} km/h`;
    case "cloud": return `${Math.round(v)}%`;
  }
}

/** How far apart, in words that say which way: "13 points higher", "5° cooler", "the same". */
function apart(key: LikeFactorKey, then: number, that: number): string {
  const d = that - then;
  const n = (v: number) => Math.abs(v);
  switch (key) {
    case "flow": {
      const pts = Math.round(n(d) * 100);
      return pts === 0 ? "the same" : `${pts} points ${d > 0 ? "higher" : "lower"}`;
    }
    case "temp": {
      const deg = Math.round(n(d));
      return deg === 0 ? "the same" : `${deg}° ${d > 0 ? "warmer" : "cooler"}`;
    }
    case "rain": {
      const mm = round1(n(d));
      return mm === 0 ? "the same" : `${mm} mm ${d > 0 ? "wetter" : "drier"}`;
    }
    case "wind": {
      const k = Math.round(n(d));
      return k === 0 ? "the same" : `${k} km/h ${d > 0 ? "windier" : "calmer"}`;
    }
    case "cloud": {
      const c = Math.round(n(d));
      return c === 0 ? "the same" : `${c} points ${d > 0 ? "cloudier" : "clearer"}`;
    }
  }
}

/**
 * One day against the visit. `today` says the day's flow is the gauge's reading NOW rather than a forecast; `hour` is
 * the visit's hour of day, "09:00", which the weather values are at.
 */
export function likeDay(then: LikeConditions, that: LikeConditions & { date: string },
                        o: { today?: boolean; hour?: string } = {}): LikeDay {
  const val = (c: LikeConditions, k: LikeFactorKey): number | null =>
    k === "flow" ? c.flow : k === "temp" ? c.tempC : k === "rain" ? c.rainMm : k === "wind" ? c.windKph : c.cloudPct;
  const known = LIKE_FACTORS.filter((k) => val(then, k) !== null && val(that, k) !== null);
  const total = known.reduce((s, k) => s + LIKE_WEIGHTS[k], 0);
  const at = o.hour ? ` at ${o.hour}` : "";
  const factors: LikeFactor[] = LIKE_FACTORS.map((k) => {
    const a = val(then, k), b = val(that, k);
    const label = LIKE_WORDS.labels[k];
    if (a === null || b === null) {
      const why = a === null
        ? (k === "flow" ? "no flow then to compare" : "not recorded at the visit")
        : (k === "flow" ? "no flow forecast for this day" : "no forecast for this day");
      return { key: k, likeness: null, share: 0, line: `${label}: ${why}. Not scored.` };
    }
    const l = likeness(k, a, b);
    const share = LIKE_WEIGHTS[k] / total;
    const when = k === "flow" ? (o.today ? "now" : "forecast") : k === "rain" ? "forecast" : `forecast${at}`;
    return { key: k, likeness: l, share,
             line: `${label}: ${said(k, a)} then, ${said(k, b)} ${when}. ${cap(apart(k, a, b))}: ${pct(l)} alike, `
               + `${pct(share)} of the score.` };
  });
  if (!known.length)
    return { date: that.date, score: null, read: null, basis: null, note: LIKE_WORDS.noScore, factors };
  const s01 = factors.reduce((s, f) => s + (f.likeness ?? 0) * f.share, 0);
  const flowIn = known.includes("flow");
  return {
    date: that.date, score: Math.round(s01 * 100), read: likeRead(s01),
    basis: flowIn ? "all" : "weather",
    note: flowIn ? null : then.flow === null ? LIKE_WORDS.noFlowThen : LIKE_WORDS.noFlowAhead,
    factors,
  };
}

const cap = (s: string) => s.charAt(0).toUpperCase() + s.slice(1);

/** True when the visit holds nothing at all to compare with. */
export function likeNothingThen(then: LikeConditions): boolean {
  return LIKE_FACTORS.every((k) => (k === "flow" ? then.flow : k === "temp" ? then.tempC : k === "rain" ? then.rainMm
    : k === "wind" ? then.windKph : then.cloudPct) === null);
}

/** The LIKE_DAYS local days from `now`'s day: today first. */
export function likeDays(now: number): string[] {
  const d = new Date(now);
  return Array.from({ length: LIKE_DAYS }, (_, i) => localDate(shiftDay(d, i).getTime()));
}

/** A coming day's column head: "Today", "Tomorrow", else the weekday "Sat"; and its date, "24 Oct". */
export function likeDayHead(date: string, now: number): { top: string; sub: string } {
  const [y, m, d] = date.split("-").map(Number) as [number, number, number];
  const days = likeDays(now);
  const top = date === days[0] ? LIKE_WORDS.today : date === days[1] ? LIKE_WORDS.tomorrow
    : weekdayAbbr(new Date(y, m - 1, d).getDay());
  return { top, sub: `${d} ${monthAbbr(m - 1)}` };
}

/** The visit's column: "Your visit" over its date, with the year when it is not this year: "30 Aug 2025". */
export function likeVisitHead(visitedAt: number, now: number): { top: string; sub: string } {
  const v = new Date(visitedAt);
  const year = v.getFullYear() === new Date(now).getFullYear() ? "" : ` ${v.getFullYear()}`;
  return { top: LIKE_WORDS.visit, sub: `${v.getDate()} ${monthAbbr(v.getMonth())}${year}` };
}

/** The cell's number: "82%", or a dash for no score. */
export const likeScoreText = (d: LikeDay): string => (d.score === null ? LIKE_WORDS.dash : `${d.score}%`);

/** The day said whole, for a screen reader and a test: "Today, 24 Oct: 82%, very similar to your visit." */
export function likeDayLabel(d: LikeDay, now: number): string {
  const h = likeDayHead(d.date, now);
  const head = `${h.top}, ${h.sub}`;
  if (d.score === null || !d.read) return `${head}: ${LIKE_WORDS.noScore}`;
  const to = d.read === "different" ? "from" : "to";
  return `${head}: ${d.score}%, ${d.read} ${to} your visit${d.basis === "weather" ? ", on the weather only" : ""}.`;
}

/**
 * How old the forecast is: "Forecast checked 2h ago." — and, when the last try could not reach it, that this is the
 * last one fetched: "Offline: showing the forecast checked 5h ago." Null `fetchedAt`: never fetched.
 */
export function likeAgeLine(fetchedAt: number | null, now: number, offline: boolean): string {
  if (fetchedAt === null || !Number.isFinite(fetchedAt)) return LIKE_WORDS.never;
  const ago = agoWords(new Date(fetchedAt).toISOString(), now);
  return offline ? `Offline: showing the forecast checked ${ago}.` : `Forecast checked ${ago}.`;
}
