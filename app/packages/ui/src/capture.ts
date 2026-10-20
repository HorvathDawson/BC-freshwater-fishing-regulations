/**
 * Adding a spot, as a hook (P12b): the visit's day and hour, the capture and the save.
 *
 * This was inside `SpotCapture`: the never-ahead-of-now clamp, the call to `captureSpot`, and the save of the draft
 * with what was typed. The component keeps only which step it is on and draws what this returns. The day arithmetic
 * itself is `core/day.ts`.
 */
import { useState } from "react";
import { setVisitHour, shiftVisitDay, visitSteps, type VisitSteps } from "@app/core";
import type { ItemId, RegsSource, SectionId } from "@app/data";
import { captureSpot, type Spot, type WeatherSource } from "@app/data/spots";
import { spotSaveFailed } from "./spots";

export interface CaptureOptions {
  source: RegsSource;
  weather?: WeatherSource;
  /** The live index, so the estimate is frozen as the reader saw it. */
  feed?: { index(): Promise<any> };
  /** The clock. A test passes a fixed one. */
  now?: () => number;
}

/** The water picked on the map. `section` is used to look things up while capturing and is never stored. */
export interface CaptureWater { section: SectionId; item: ItemId | null; name: string | null }

export interface CaptureSpotApi {
  /** When they were there. Starts at now: the common case, and a default nobody has to touch. */
  visitedAt: number;
  steps: VisitSteps;
  /** Move the visit by days; a step into the future is refused. */
  shiftDay(n: number): void;
  /** Go to a day by its offset from today (a quick chip). */
  toDay(offset: number): void;
  /** Set the hour; one later than now becomes now. */
  setHour(h: number): void;
  /** The spot as captured, before it is saved. */
  draft: Spot | null;
  /** A capture or save that failed, said whole (P01). */
  error: string | null;
  clearError(): void;
  /** Read the water for the visit. True when a draft is ready; false with `error` set. */
  capture(water: CaptureWater, at: { lat: number; lon: number }, title: string): Promise<boolean>;
  /** Save the draft with what was typed. A failure sets `error` and the sheet stays open. */
  save(title: string, notes: string, onSaved: (spot: Spot) => void | Promise<void>): Promise<void>;
}

export function useCaptureSpot(opts: CaptureOptions): CaptureSpotApi {
  const now = opts.now ?? Date.now;
  const [visitedAt, setVisitedAt] = useState(() => now());
  const [draft, setDraft] = useState<Spot | null>(null);
  const [error, setError] = useState<string | null>(null);
  const steps = visitSteps(visitedAt, now());

  return {
    visitedAt, steps, draft, error,
    clearError: () => setError(null),
    shiftDay: (n) => setVisitedAt((v) => shiftVisitDay(v, n, now())),
    toDay: (offset) => setVisitedAt((v) => shiftVisitDay(v, offset - visitSteps(v, now()).dayOffset, now())),
    setHour: (h) => setVisitedAt((v) => setVisitHour(v, h, now())),
    capture: async (water, at, title) => {
      setError(null);
      try {
        const spot = await captureSpot({
          feed: opts.feed, source: opts.source, weather: opts.weather, at, visitedAt,
          item: water.item, section: water.section, waterName: water.name,
          title: title || water.name || "",
        });
        setDraft(spot);
        return true;
      } catch (e) {
        setError(spotSaveFailed(e));
        return false;
      }
    },
    save: async (title, notes, onSaved) => {
      if (!draft) return;
      setError(null);
      try {
        await onSaved({ ...draft, title: title || draft.title, notes, updatedAt: now() });
      } catch (e) {
        setError(spotSaveFailed(e));
      }
    },
  };
}
