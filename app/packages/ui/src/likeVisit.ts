/**
 * "Like your visit?" (T51): what a saved spot's forecast strip draws, decided here (rule 25), and the hook that keeps
 * it — the last fetched forecast read from the device at once (offline works, and says how old it is), then fetched
 * fresh when online.
 *
 * Every word and every number is core's (`likeDay` and its words); the inputs are `@app/data/spots`'. This only
 * chooses which, in order.
 */
import { useEffect, useMemo, useState } from "react";
import { LIKE_WORDS, likeAgeLine, likeDay, likeDayHead, likeDayLabel, likeDays, likeNothingThen, likeScoreText,
         likeVisitHead } from "@app/core";
import { comingConditions, fetchLike, NOTHING_FETCHED, readLike, visitConditions, visitHour, writeLike,
         type LikeFetched, type LikeSources, type LikeStore, type Spot } from "@app/data/spots";
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
  /** "weather only" when flow is not in the score. */
  basis: string | null;
  /** The day said whole (a screen reader, a test). */
  label: string;
  /** What a tap shows: why flow is out (if it is), then each factor's line. */
  why: string[];
}

export interface LikeVisitView {
  heading: string;
  intro: string;
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

export function likeVisitView(spot: Spot, f: LikeFetched, offline: boolean, now: number): LikeVisitView {
  const then = visitConditions(spot);
  const v = likeVisitHead(spot.visitedAt, now);
  const visit = { ...v, label: `${v.top}, ${v.sub}: the day the others are compared with.` };
  const base = { heading: LIKE_WORDS.heading, intro: LIKE_WORDS.intro, visit, age: likeAgeLine(oldest(f), now, offline) };
  if (likeNothingThen(then)) return { ...base, days: null, note: LIKE_WORDS.nothingThen };
  const hour = visitHour(spot);
  const days = likeDays(now);
  // "now" only for the day the gauge's reading is for — never a forecast for today read off yesterday's feed
  const vms = comingConditions(f, days, hour).map((c): LikeDayVM => {
    const d = likeDay(then, c, { today: !!f.flowNow && c.date === f.flowNow, hour });
    const h = likeDayHead(d.date, now);
    return { date: d.date, top: h.top, sub: h.sub, score: likeScoreText(d),
             read: d.read ? LIKE_WORDS.reads[d.read] : null,
             basis: d.basis === "weather" ? LIKE_WORDS.weatherOnly : null,
             label: likeDayLabel(d, now),
             why: [...(d.note ? [d.note] : []), ...d.factors.map((x) => x.line)] };
  });
  return { ...base, days: vms, note: null };
}

/**
 * The strip for one saved spot. `spot` null (no spot open): nothing, and nothing fetched. The last fetched forecast is
 * read from `store` at once; then both halves are fetched (`fetchLike`) and kept — a half that could not be reached
 * stays as last fetched, and the view says it is offline.
 */
export function useLikeVisit(spot: Spot | null, sources: LikeSources, store?: LikeStore,
                             now: () => number = Date.now, connectivity?: Connectivity): LikeVisitView | null {
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
  return useMemo(() => (spot && state.id === spot.id ? likeVisitView(spot, state.f, state.offline, now()) : null),
                 [spot, state]);
}
