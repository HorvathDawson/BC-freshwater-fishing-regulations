/**
 * A spot's visits on its page (T294): the row of visits to pick from, the shown visit's rating, and "Add a visit" —
 * decided here (rule 25), drawn by `SpotScreen`. Every word is core's (`VISIT_WORDS`), every record `@app/data/spots`'.
 */
import { useState } from "react";
import { isRating, RATING, setVisitHour, shiftVisitDay, starsText, VISIT_WORDS, visitDateText, visitSteps,
         type VisitSteps, reportFailure } from "@app/core";
import type { WaterSource } from "@app/data";
import { addVisit, captureVisit, spotVisits, visitSpot, type HydatDaily, type Spot, type WeatherSource } from "@app/data/spots";
import { spotSaveFailed } from "./spots";

export interface VisitChipVM {
  id: string;
  /** "12 Oct" */
  date: string;
  /** "5★", or "Not rated" */
  rated: string;
  on: boolean;
  label: string;
}

export interface VisitsView {
  heading: string;
  chips: VisitChipVM[];
  /** The shown visit's rating block: its stars (1 … max), which are lit, and each one's name. */
  rate: { heading: string; detail: string; value: number | null;
          stars: { n: number; on: boolean; label: string }[]; word: string };
  /** True when the shown visit can be removed (the spot has another). */
  canRemove: boolean;
}

/** The visits block for a spot, with visit `shown` picked (an unknown id: the latest). */
export function visitsView(spot: Spot, shown: string | null | undefined, now: number): VisitsView {
  const visits = spotVisits(spot);
  const on = visitSpot(spot, shown).visitId;
  const cur = visits.find((v) => v.id === on)!;
  const value = isRating(cur.rating) ? cur.rating : null;
  return {
    heading: VISIT_WORDS.heading,
    chips: visits.map((v) => {
      const date = visitDateText(v.visitedAt, now);
      const r = isRating(v.rating) ? v.rating : null;
      return { id: v.id, date, rated: r === null ? VISIT_WORDS.unrated : starsText(r), on: v.id === on,
               label: VISIT_WORDS.chipLabel(date, r) };
    }),
    rate: {
      heading: VISIT_WORDS.rate, detail: VISIT_WORDS.rateDetail, value,
      word: value === null ? VISIT_WORDS.unrated : RATING.words[value - 1]!,
      stars: Array.from({ length: RATING.max }, (_, i) => i + 1).map((n) => ({
        n, on: value !== null && n <= value,
        label: n === value ? VISIT_WORDS.clearLabel(n) : VISIT_WORDS.rateLabel(n) })),
    },
    canRemove: visits.length > 1,
  };
}

/** A star pressed: that rating, or none when it is the rating already (press again to clear). */
export const nextRating = (current: number | null, pressed: number): number | null => (current === pressed ? null : pressed);

export interface AddVisitOptions {
  source: WaterSource;
  weather?: WeatherSource;
  feed?: { index(): Promise<any> };
  history?: { hydat?: HydatDaily; online?: () => boolean };
  now?: () => number;
}

export interface AddVisitApi {
  visitedAt: number;
  steps: VisitSteps;
  shiftDay(n: number): void;
  toDay(offset: number): void;
  setHour(h: number): void;
  busy: boolean;
  error: string | null;
  /** Read the water for the visit and save it on the spot; resolves to the new visit's id, or null (error set). */
  record(spot: Spot, onSave: (spot: Spot) => void | Promise<void>): Promise<string | null>;
  /** Back to now, nothing read (a new add). */
  reset(): void;
}

/** "Add a visit": the day and hour (the same steps as adding a spot), the capture and the save. */
export function useAddVisit(opts: AddVisitOptions | null): AddVisitApi {
  const now = opts?.now ?? Date.now;
  const [visitedAt, setVisitedAt] = useState(() => now());
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  return {
    visitedAt, busy, error, steps: visitSteps(visitedAt, now()),
    shiftDay: (n) => setVisitedAt((v) => shiftVisitDay(v, n, now())),
    toDay: (offset) => setVisitedAt((v) => shiftVisitDay(v, offset - visitSteps(v, now()).dayOffset, now())),
    setHour: (h) => setVisitedAt((v) => setVisitHour(v, h, now())),
    reset: () => { setVisitedAt(now()); setError(null); setBusy(false); },
    record: async (spot, onSave) => {
      if (!opts) return null;
      setError(null);
      setBusy(true);
      try {
        const t = now();
        const visit = await captureVisit(spot, { source: opts.source, weather: opts.weather, feed: opts.feed,
                                                 ...(opts.history ? { history: opts.history } : {}), visitedAt, now: t });
        await onSave(addVisit(spot, visit, t));
        return visit.id;
      } catch (e) {
        reportFailure(e);
        setError(spotSaveFailed(e));
        return null;
      } finally {
        setBusy(false);
      }
    },
  };
}
