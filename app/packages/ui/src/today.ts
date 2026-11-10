import { useEffect, useState } from "react";
import { bcToday, nextBcMidnight, now, wallIso } from "@app/core";

/** Longest the hook sleeps: a timer a phone suspended in the background fires late, so it re-reads the clock at least this often. */
const BEAT_MS = 60_000;

/**
 * (T551, review 2026-11-09 #1) "TODAY" THAT ROLLS OVER. B.C.'s today (`bcToday`, midnight, wall fields) as state: it moves
 * to the new day at the next B.C. midnight, and within a minute of the app coming back to the front (a suspended timer
 * is overdue on return and fires at once; the heartbeat re-reads the clock either way: no DOM or AppState here). Every `picked ?? today` that a memo or an effect holds takes this
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
    arm();
    check();
    return () => {
      clearTimeout(timer);
    };
  }, []);
  return today;
}
