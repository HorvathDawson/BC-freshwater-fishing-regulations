/**
 * The spots, as a hook (P01). Every save and delete is AWAITED and its failure is returned to the caller, which shows
 * it and keeps the sheet open. The list is re-read after each write. A weather refresh writes through `patch`, so it
 * fills a spot only if nobody changed or deleted it meanwhile.
 */
import { useState } from "react";
import type { Spot } from "@app/data/spots";
import { useAsync, type Async } from "./async";

/** What the hook needs of a store (`openSpots()` from @app/data/spots). */
export interface SpotsStoreLike {
  list(): Promise<Spot[]>;
  put(spot: Spot): Promise<void>;
  remove(id: string): Promise<void>;
  patch(id: string, change: (spot: Spot) => Spot): Promise<void>;
}

/** The sentence a reader sees when a spot could not be written (the store's own words for a corrupt file). */
export function spotSaveFailed(e: unknown): string {
  if (e instanceof Error && e.name === "SpotsCorrupt") return e.message;
  const why = e instanceof Error ? e.message : String(e);
  return `This spot was not saved (${why}). It is still on screen; try again.`;
}

export function useSpots(store: SpotsStoreLike): {
  spots: Async<Spot[]>;
  save(spot: Spot): Promise<void>;
  remove(id: string): Promise<void>;
  /** Fill in what spots could not know when they were made; `fill` returns the same object when there is nothing. */
  refresh(fill: (spot: Spot) => Promise<Spot>): Promise<void>;
  error: string | null;
} {
  const [version, setVersion] = useState(0);
  const [error, setError] = useState<string | null>(null);
  const spots = useAsync(() => store.list(), `spots#${version}`, true, "spots");
  const write = async (job: () => Promise<void>) => {
    try {
      await job();
      setError(null);
    } catch (e) {
      setError(spotSaveFailed(e));
      throw e;
    } finally {
      setVersion((v) => v + 1);
    }
  };
  return {
    spots,
    error: error ?? (spots.state === "failed" ? spotSaveFailed(spots.error) : null),
    save: (spot) => write(() => store.put(spot)),
    remove: (id) => write(() => store.remove(id)),
    refresh: (fill) => write(async () => {
      for (const s of await store.list()) {
        const filled = await fill(s);
        // only onto the spot as it was read: an edit made meanwhile wins, and the next refresh fills it
        if (filled !== s) await store.patch(s.id, (cur) => (cur.updatedAt === s.updatedAt ? filled : cur));
      }
    }),
  };
}
