/**
 * A SPOT HOLDS SEVERAL VISITS, EACH RATED; THE COMING DAYS ARE READ AGAINST THEM (T294, owner 2026-10-29: "add a way to
 * update a spot with additional visits and you can rank the visit then we can use that to instead predict how good the
 * day will be").
 *
 * PURE, AND THE ONE PLACE THE RATING SCALE, THE PREDICTION AND THEIR WORDS LIVE. The per-visit likeness is T284's
 * `likeDay` unchanged (flow + lead-up rain, then cloud + pressure, then air + wind); this only weighs the ratings by it.
 *
 * THE PREDICTION. For one coming day, every RATED visit that could be compared (a score) counts:
 *
 *   weight(visit) = (likeness / 100) ^ PREDICT_POWER          (the most alike visits count most; 0 alike counts nothing)
 *   prediction    = Σ weight · rating / Σ weight               (a rating, 1 … RATING.max, to one decimal)
 *
 * so one rated visit predicts its own rating, two equally alike visits predict their mean, and a visit twice as alike
 * counts 2³ = 8 times as much. It is how YOUR RATED VISITS compare, never a claim about fish: the words say so.
 */
import { monthAbbr } from "./flow";
import { LIKE_READS, likeDayHead, type LikeDay } from "./likeVisit";

/**
 * THE RATING SCALE (owner: default 1–5 stars). A rating is a whole number 1 … `max`, stored as that number; `words[n-1]`
 * says it. Changing the scale later means mapping the stored numbers — a 4 on a 5-scale is not a 4 on a 4-scale.
 */
export const RATING = { max: 5, words: ["Poor", "Fair", "OK", "Good", "Great"] } as const;

/** A stored rating that is one: a whole number in 1 … RATING.max. Anything else reads as not rated. */
export const isRating = (v: unknown): v is number =>
  typeof v === "number" && Number.isInteger(v) && v >= 1 && v <= RATING.max;

/** How sharply the most alike visits outweigh the rest (see the header). */
export const PREDICT_POWER = 3;

/** "4★" — a whole rating said short. */
export const starsText = (n: number): string => `${n}★`;

/** A rating's word: "Good". A prediction is rounded to the nearest whole rating first. */
export const ratingWord = (n: number): string =>
  RATING.words[Math.min(RATING.max, Math.max(1, Math.round(n))) - 1]!;

export const VISIT_WORDS = {
  /** the visits block's heading */
  heading: "Your visits",
  /** the button that records another visit to the same spot */
  add: "Add a visit",
  addDetail: "Records the water and the weather for another day you fished here, so the coming days can be read against it.",
  /** the add-a-visit page's title, over the same day-and-hour steps as adding a spot */
  addTitle: "When was this visit?",
  /** while the new visit's water and weather are read */
  reading: "Reading the water for this visit",
  /** the rating block on a visit */
  rate: "How was this visit?",
  rateDetail: "Your own rating. The coming days are read against your rated visits.",
  unrated: "Not rated",
  /** the star drawn lit (the rating reaches it) and unlit */
  starOn: "★", starOff: "☆",
  /** a star's name to a screen reader: "Rate this visit 4 of 5: Good" */
  rateLabel: (n: number) => `Rate this visit ${n} of ${RATING.max}: ${ratingWord(n)}`,
  /** the same star when it is the visit's rating: pressing it clears the rating */
  clearLabel: (n: number) => `Rated ${n} of ${RATING.max}. Press to clear the rating`,
  /** a visit chip's name to a screen reader */
  chipLabel: (date: string, rating: number | null) =>
    `Visit on ${date}, ${rating === null ? "not rated" : `rated ${rating} of ${RATING.max}`}`,
  remove: "Remove visit", removeLabel: "Remove this visit",
} as const;

/** Removing one visit from a spot that has others, asked first. */
export const VISIT_CONFIRM_WORDS = {
  label: "Confirm remove visit",
  title: "Remove this visit?",
  detail: "Its water, weather, notes, photos and rating are recorded nowhere else. The spot and its other visits stay. "
    + "This cannot be undone.",
  keep: "Keep it",
  go: "Remove",
} as const;

export const PREDICT_WORDS = {
  intro: "How good each of the next five days may be, going by how you rated your visits here and how alike each day "
    + "looks to them. The river's flow and the rain in the three days before count most, then cloud and air pressure, "
    + "then air temperature and wind. It is how your rated visits compare, not a promise of fish.",
  /** the band over the days: whose ratings */
  top: "From your ratings",
  /** the mark on the coming day your ratings favour most */
  best: "Best going by your ratings",
  /** how many rated visits a day's prediction rests on */
  restsOn: (n: number) => (n === 1 ? "From 1 rated visit" : `From ${n} rated visits`),
  /** the visit the day is most like, with its rating, date and how alike: "Most like your 5★ visit on 12 Oct (84% alike)" */
  mostLike: (rating: number, date: string, score: number) =>
    `Most like your ${starsText(rating)} visit on ${date} (${score}% alike)`,
  /** no rated visit is even "similar" to the day (LIKE_READS.similar) */
  far: "None of your rated visits looks much like this day.",
  /** no rated visit could be compared with the day at all */
  noScore: "None of your rated visits can be compared with this day.",
  /** shown under the strip while some visits are rated and some are not */
  unratedLeftOut: (n: number) => (n === 1 ? "1 visit is not rated and is left out." : `${n} visits are not rated and are left out.`),
  /** with no rated visit: how the strip says it compares to one visit instead */
  rateToPredict: "Rate your visits to see how good each coming day may be by your own ratings.",
} as const;

/** One rated visit, scored against one coming day (core `likeDay`). */
export interface RatedLike {
  id: string;
  visitedAt: number;
  rating: number;
  day: LikeDay;
}

export interface DayPrediction {
  date: string;
  /** The predicted rating, 1 … RATING.max to one decimal; null when no rated visit could be compared. */
  value: number | null;
  /** How many rated visits it rests on (those with a score). */
  rests: number;
  /** The visit the day is most like: the highest likeness; on a tie the most recent visit. */
  top: RatedLike | null;
  /** True when even the most alike visit is not "similar" (below LIKE_READS.similar). */
  far: boolean;
}

/** One day's prediction from its rated visits' likenesses (the header's formula). */
export function predictDay(date: string, scored: readonly RatedLike[]): DayPrediction {
  const counted = scored.filter((s) => isRating(s.rating) && s.day.score !== null);
  if (!counted.length) return { date, value: null, rests: 0, top: null, far: false };
  const top = counted.reduce((a, b) => (b.day.score! > a.day.score! || (b.day.score === a.day.score && b.visitedAt > a.visitedAt)
    ? b : a));
  const w = counted.map((s) => (s.day.score! / 100) ** PREDICT_POWER);
  const sum = w.reduce((a, b) => a + b, 0);
  // every visit 0 % alike: nothing to weigh, said as "not much like any", never a made-up mean
  const value = sum > 0 ? Math.round((counted.reduce((a, s, i) => a + w[i]! * s.rating, 0) / sum) * 10) / 10 : null;
  return { date, value, rests: counted.length, top, far: top.day.score! < LIKE_READS.similar * 100 };
}

/** A visit's date, short: "12 Oct", with the year when it is not this year: "30 Aug 2025". */
export function visitDateText(visitedAt: number, now: number): string {
  const v = new Date(visitedAt);
  const year = v.getFullYear() === new Date(now).getFullYear() ? "" : ` ${v.getFullYear()}`;
  return `${v.getDate()} ${monthAbbr(v.getMonth())}${year}`;
}

/** The prediction's number: "4.2★", or a dash. */
export const predictText = (p: DayPrediction): string => (p.value === null ? "—" : `${p.value.toFixed(1)}★`);

/** The day said whole, for a screen reader and a test: "Today, 24 Oct: about 4.2 of 5, good, from 3 rated visits." */
export function predictLabel(p: DayPrediction, now: number): string {
  const h = likeDayHead(p.date, now);
  const head = `${h.top}, ${h.sub}`;
  if (p.value === null) return `${head}: ${PREDICT_WORDS.noScore}`;
  return `${head}: about ${p.value.toFixed(1)} of ${RATING.max}, ${ratingWord(p.value).toLowerCase()}, `
    + `${PREDICT_WORDS.restsOn(p.rests).toLowerCase()}.`;
}

/** The plain reasons under a day: the visit it is most like, then why (that visit's own per-factor reasons). */
export function predictReasons(p: DayPrediction, now: number): string[] {
  if (!p.top) return [];
  return [PREDICT_WORDS.mostLike(p.top.rating, visitDateText(p.top.visitedAt, now), p.top.day.score!),
          ...p.top.day.factors.flatMap((f) => (f.plain === null ? [] : [f.plain]))];
}
