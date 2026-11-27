import { useEffect, useState } from "react";
import { bcToday, nextBcMidnight, now, wallIso } from "@app/core";

/** Longest the hook sleeps: a timer a phone suspended in the background fires late, so it re-reads the clock at least this often. */
const BEAT_MS = 60_000;

/** (T605 item 3) Who re-reads the clock when the app comes back to the front. */
const onActive = new Set<() => void>();

/**
 * (T605 item 3) THE APP CAME BACK TO THE FRONT: said by the shell, which owns the platform's signal (shared ui may not
 * use `document` / AppState). Every `useBcToday` re-reads the clock at once, so a phone opened the morning after shows
 * the new day in its first frame back, not up to a minute later.
 */
/** (T1205) Hear the same signal without a clock of one's own: the gauge judgement re-runs when the app is back at the front. */
export function onAppActive(f: () => void): () => void {
  onActive.add(f);
  return () => { onActive.delete(f); };
}

/** (T1205) Who is told the app left the front (the background): a timer of the shared UI stops there. */
const onInactive = new Set<() => void>();
export function onAppInactive(f: () => void): () => void {
  onInactive.add(f);
  return () => { onInactive.delete(f); };
}
/** The app went to the background: said by the shell (`useResumed`), beside `appBecameActive`. */
export function appBecameInactive(): void {
  for (const f of [...onInactive]) f();
}

export function appBecameActive(): void {
  for (const f of [...onActive]) f();
}

/**
 * (T551, review 2026-11-09 #1) "TODAY" THAT ROLLS OVER. B.C.'s today (`bcToday`, midnight, wall fields) as state: it moves
 * to the new day at the next B.C. midnight, and at once when the app comes back to the front (`appBecameActive`, the
 * shell's word; the heartbeat still re-reads the clock within a minute either way: no DOM or AppState here). Every `picked ?? today` that a memo or an effect holds takes this
 * as its dependency, so the map's status, the salmon and gear colours and Conditions keep up with the clock without a tap.
 * The same Date object is returned until the day changes.
 */
export function useBcToday(): Date {
  const [today, setToday] = useState(() => bcToday());
  useEffect(() => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    const check = () => {
      const t = bcToday();
      setToday((was) => (wallIso(was) === wallIso(t) ? was : t));
    };
    const arm = () => {
      clearTimeout(timer);
      timer = setTimeout(() => { check(); arm(); }, Math.min(BEAT_MS, Math.max(1_000, nextBcMidnight(now()) - now() + 250)));
    };
    // back at the front: the clock is read now, and the timer re-armed for the midnight after it
    const active = () => { check(); arm(); };
    onActive.add(active);
    arm();
    check();
    return () => {
      onActive.delete(active);
      clearTimeout(timer);
    };
  }, []);
  return today;
}
