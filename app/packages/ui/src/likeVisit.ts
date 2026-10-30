/**
 * "Like your visit?" (T51): what a saved spot's forecast strip draws, decided here (rule 25), and the hook that keeps
 * it — the last fetched forecast read from the device at once (offline works, and says how old it is), then fetched
 * fresh when online.
 *
 * Every word and every number is core's (`likeDay` and its words); the inputs are `@app/data/spots`'. This only
 * chooses which, in order.
 */
import { useEffect, useMemo, useState } from "react";
import { isRating, LIKE_WORDS, likeAgeLine, likeDay, likeDayHead, likeDayLabel, likeDays, likeNothingThen, likeScoreText,
         likeVisitHead, PREDICT_WORDS, predictDay, predictLabel, predictReasons, predictText, ratingWord, starsText,
         visitDateText } from "@app/core";
import { comingConditions, fetchLike, latestVisitId, NOTHING_FETCHED, readLike, spotVisits, visitConditions, visitHour,
         visitSpot, writeLike, type LikeFetched, type LikeSources, type LikeStore, type Spot,
         type VisitSpot } from "@app/data/spots";
import type { Connectivity } from "./imagery";

export interface LikeDayVM {
  date: string;
  /** "Today" / "Tomorrow" / "Sat", over "24 Oct". */
  top: string;
  sub: string;
  /** "82%", or a dash. */
  score: string;
  /** "Very similar" / "Similar" / "Different"; null with no score. */
  read: string | null;
  /** why flow is not in the score ("No flow forecast for this day: scored on the weather only.") — T284; "low-water model" when ELF forecast the day's flow (T171). */
  basis: string | null;
  /** (T284) The day that looks most like the visit (the highest score; the earlier on a tie); at most one. */
  best: boolean;
  /** (T284) One short plain reason per factor that was scored: "Air temperature: 4° cooler". */
  reasons: string[];
  /** The day said whole (a screen reader, a test). */
  label: string;
  /** What a tap shows: why flow is out (if it is), then each factor's line. */
  why: string[];
}

export interface LikeVisitView {
  /**
   * (T294) "predict": the days read against EVERY rated visit, a predicted rating each (core `predictDay`); "like": no
   * visit is rated, so the days are compared with one visit, as T51 / T284 did.
   */
  mode: "like" | "predict";
  heading: string;
  intro: string;
  /** (T294) The mark on the best day: "Closest to your visit", or "Best going by your ratings". */
  best: string;
  /** (T294) A line under the days: how to get a prediction (nothing rated), or which visits were left out (unrated). */
  hint: string | null;
  visit: { top: string; sub: string; label: string };
  /** Null when the visit recorded nothing to compare with (`note` says so). */
  days: LikeDayVM[] | null;
  note: string | null;
  /** How old the forecast is, or that it has not been fetched. */
  age: string;
}

/** The older of the two halves' fetch times: the forecast is as old as its oldest half. */
const oldest = (f: LikeFetched): number | null => {
  const t = [f.weatherAt, f.flowAt].filter((v): v is number => v !== null);
  return t.length ? Math.min(...t) : null;
};

/**
 * The strip for a spot. (T294) With any visit rated, every coming day is read against every rated visit and predicted
 * from their ratings (`predictView`); with none, it compares the days with visit `shown` (default the latest), as before.
 */
export function likeVisitView(spot: Spot, f: LikeFetched, offline: boolean, now: number,
                              shown?: string | null): LikeVisitView {
  const visits = spotVisits(spot).map((v) => visitSpot(spot, v.id));
  const rated = visits.filter((v) => isRating(v.rating) && !likeNothingThen(visitConditions(v)));
  if (rated.length) return predictView(rated, visits.length - rated.length, f, offline, now);
  const one = likeOne(visitSpot(spot, shown ?? latestVisitId(spot)), f, offline, now);
  return { ...one, hint: PREDICT_WORDS.rateToPredict };
}

/** (T294) The coming days predicted from the rated visits, each scored with T284's `likeDay` at its own hour. */
function predictView(rated: VisitSpot[], unrated: number, f: LikeFetched, offline: boolean, now: number): LikeVisitView {
  const list = rated.map((v) => `${starsText(v.rating!)} ${visitDateText(v.visitedAt, now)}`).join(" · ");
  const scored = likeDays(now).map((date) => predictDay(date, rated.map((v) => {
    const hour = visitHour(v);
    const that = comingConditions(f, [date], hour)[0]!;
    return { id: v.visitId, visitedAt: v.visitedAt, rating: v.rating!,
             day: likeDay(visitConditions(v), that, { today: !!f.flowNow && date === f.flowNow, hour }) };
  })));
  const top = scored.reduce<number | null>((m, d) => (d.value !== null && (m === null || d.value > m) ? d.value : m), null);
  const bestDate = top === null ? null : scored.find((d) => d.value === top)!.date;
  const days = scored.map((p): LikeDayVM => {
    const h = likeDayHead(p.date, now);
    const rests = p.value === null ? PREDICT_WORDS.noScore : PREDICT_WORDS.restsOn(p.rests);
    return { date: p.date, top: h.top, sub: h.sub, score: predictText(p),
             read: p.value === null ? null : ratingWord(p.value),
             basis: p.far ? `${rests}. ${PREDICT_WORDS.far}` : rests,
             best: p.date === bestDate,
             reasons: predictReasons(p, now),
             label: predictLabel(p, now),
             why: p.top ? [...(p.top.day.note ? [p.top.day.note] : []), ...p.top.day.factors.map((x) => x.line),
                           ...(p.top.day.caveat ? [p.top.day.caveat] : [])] : [] };
  });
  return { mode: "predict", heading: LIKE_WORDS.heading, intro: PREDICT_WORDS.intro, best: PREDICT_WORDS.best,
           hint: unrated ? PREDICT_WORDS.unratedLeftOut(unrated) : null,
           visit: { top: PREDICT_WORDS.top, sub: list, label: `${PREDICT_WORDS.top}: ${list}.` },
           days, note: null, age: likeAgeLine(oldest(f), now, offline) };
}

/** One visit against the coming days (T51 / T284). */
function likeOne(spot: Spot, f: LikeFetched, offline: boolean, now: number): LikeVisitView {
  const then = visitConditions(spot);
  const v = likeVisitHead(spot.visitedAt, now);
  const visit = { ...v, label: `${v.top}, ${v.sub}: the day the others are compared with.` };
  const base = { mode: "like" as const, heading: LIKE_WORDS.heading, intro: LIKE_WORDS.intro, best: LIKE_WORDS.best,
                 hint: null, visit, age: likeAgeLine(oldest(f), now, offline) };
  if (likeNothingThen(then)) return { ...base, days: null, note: LIKE_WORDS.nothingThen };
  const hour = visitHour(spot);
  const days = likeDays(now);
  // "now" only for the day the gauge's reading is for — never a forecast for today read off yesterday's feed
  const scored = comingConditions(f, days, hour).map((c) => likeDay(then, c, { today: !!f.flowNow && c.date === f.flowNow, hour }));
  const top = scored.reduce<number | null>((m, d) => (d.score !== null && (m === null || d.score > m) ? d.score : m), null);
  const bestDate = top === null ? null : scored.find((d) => d.score === top)!.date;
  const vms = scored.map((d): LikeDayVM => {
    const h = likeDayHead(d.date, now);
    return { date: d.date, top: h.top, sub: h.sub, score: likeScoreText(d),
             read: d.read ? LIKE_WORDS.reads[d.read] : null,
             // under the score: "weather only" with no flow, "low-water model" when ELF forecast the flow (T171)
             basis: d.basis === "weather" ? d.note ?? LIKE_WORDS.weatherOnly : d.caveat === LIKE_WORDS.carried ? LIKE_WORDS.carried : d.caveat ? LIKE_WORDS.lowWater : null,
             best: d.date === bestDate,
             reasons: d.factors.flatMap((x) => (x.plain === null ? [] : [x.plain])),
             label: likeDayLabel(d, now),
             why: [...(d.note ? [d.note] : []), ...d.factors.map((x) => x.line), ...(d.caveat ? [d.caveat] : [])] };
  });
  return { ...base, days: vms, note: null };
}

/**
 * The strip for one saved spot. `spot` null (no spot open): nothing, and nothing fetched. The last fetched forecast is
 * read from `store` at once; then both halves are fetched (`fetchLike`) and kept — a half that could not be reached
 * stays as last fetched, and the view says it is offline.
 */
export function useLikeVisit(spot: Spot | null, sources: LikeSources, store?: LikeStore,
                             now: () => number = Date.now, connectivity?: Connectivity,
                             /** (T294) the visit shown, compared with while no visit is rated; absent: the latest */
                             shown?: string | null): LikeVisitView | null {
  const id = spot?.id ?? null;
  const [state, setState] = useState<{ id: string | null; f: LikeFetched; offline: boolean }>(
    { id: null, f: NOTHING_FETCHED, offline: false });
  // "refreshes when online" (T51 acceptance): a strip that could not reach the forecast asks again the moment the
  // device says it is back, not only the next time the spot is opened
  const [retry, setRetry] = useState(0);
  const offline = state.offline;
  useEffect(() => {
    if (!spot || !offline || !connectivity?.watches) return;
    return connectivity.subscribe((up) => { if (up) setRetry((n) => n + 1); });
  }, [id, offline, connectivity]);
  const { weather, feed } = sources;
  useEffect(() => {
    if (!spot) return;
    let live = true;
    const had = readLike(store, spot.id);
    setState({ id: spot.id, f: had, offline: false });
    void fetchLike(spot, had, { weather, feed }, now()).then(({ fetched, offline }) => {
      if (!live) return;
      writeLike(store, spot.id, fetched);
      setState({ id: spot.id, f: fetched, offline });
    });
    return () => { live = false; };
    // a spot's place, station and record do not change while it is open; its id is what a new fetch is for
  }, [id, weather, feed, store, retry]);
  return useMemo(() => (spot && state.id === spot.id ? likeVisitView(spot, state.f, state.offline, now(), shown) : null),
                 [spot, state, shown]);
}
