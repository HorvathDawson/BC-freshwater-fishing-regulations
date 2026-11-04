/**
 * The spots, as a hook (P01). Every save and delete is AWAITED and its failure is returned to the caller, which shows
 * it and keeps the sheet open. The list is re-read after each write. A weather refresh writes through `patch`, so it
 * fills a spot only if nobody changed or deleted it meanwhile.
 */
import { reportFailure } from "@app/core";
import { useEffect, useRef, useState } from "react";
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
      reportFailure(e);
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
        // (T314) one spot's failed fill never stops the rest: it keeps what it holds and is asked again next time
        const filled = await fill(s).catch(() => s);
        // only onto the spot as it was read: an edit made meanwhile wins, and the next refresh fills it
        if (filled !== s) await store.patch(s.id, (cur) => (cur.updatedAt === s.updatedAt ? filled : cur));
      }
    }),
  };
}

/** What the reconnect refresh needs of the platform's connectivity (`Connectivity`, imagery.ts). */
export interface OnlineLike {
  online(): boolean;
  subscribe(on: (online: boolean) => void): () => void;
}

/** (T255) After a reconnect: the first try waits for the line to settle, then retries while a spot still waits. */
const RECONNECT_TRIES_MS = [3_000, 20_000, 60_000] as const;

/**
 * (T255) BACK ONLINE, THE SPOTS FILL IN BY THEMSELVES. A spot made with no signal holds no weather, no week before and no
 * day's flow (`needsRefresh`); until now only the Spots page's Refresh filled them. When the device comes back online
 * the same refresh the button runs (`refresh`, App's) is run — first after `RECONNECT_TRIES_MS[0]` (a phone says
 * "connected" before its line carries anything: the emulator's first try failed), then again at each later delay while
 * a spot still waits. A spot waiting while the app opens online (or the list loads) is tried at once. One at a time;
 * its failure is the refresh's own to say; the next offline → online starts the tries again. `pending` false: nothing
 * is asked.
 */
export function useRefreshOnReconnect(c: OnlineLike | null, pending: boolean, refresh: () => Promise<void>,
                                      tries: readonly number[] = RECONNECT_TRIES_MS): void {
  const pendingRef = useRef(pending);
  pendingRef.current = pending;
  const refreshRef = useRef(refresh);
  refreshRef.current = refresh;
  const cRef = useRef(c);
  cRef.current = c;
  const busy = useRef(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const api = useRef({
    run() {
      if (busy.current || !pendingRef.current || !cRef.current?.online()) return;
      busy.current = true;
      refreshRef.current().catch(() => undefined).finally(() => { busy.current = false; });
    },
    /** try at `tries[n]`, then the next delay, while a spot waits and the device is online */
    from(n: number) {
      if (timer.current) clearTimeout(timer.current);
      if (n >= tries.length) { timer.current = null; return; }
      timer.current = setTimeout(() => {
        timer.current = null;
        if (!pendingRef.current || !cRef.current?.online()) return;
        api.current.run();
        api.current.from(n + 1);
      }, tries[n]);
    },
  });
  // a spot waiting while online: once per change of `pending` (a refresh that cannot fill it does not loop)
  useEffect(() => { if (pending && c?.online()) api.current.run(); }, [pending, c]);
  useEffect(() => {
    if (!c) return;
    let was = c.online();
    const off = c.subscribe((on) => {
      if (on && !was) api.current.from(0);
      if (!on && timer.current) { clearTimeout(timer.current); timer.current = null; }
      was = on;
    });
    return () => { off(); if (timer.current) clearTimeout(timer.current); };
  }, [c]);
}
