/**
 * LEAVING, BY THE ROUTE (G1 review). A screen's cleanup on the way out — the spot screen ending an edit and deleting the
 * photos it added — must run however the screen is left: the ‹ button, Android's system back, the browser's back, a
 * tab change. Only the ‹ can run code before it pops; the platform backs pop the route directly (`useBack`). So the
 * cleanup follows the route instead: `onLeave` runs once whenever `key` (what is open, e.g. the open spot's id) stops
 * being the value it was, and never for null (nothing was open). It reads the latest `onLeave`, so it sees the state
 * the screen had when it was left; a cleanup that already ran (the ‹ ends the edit first) finds nothing left to do.
 */
import { useEffect, useRef } from "react";

export function useOnLeave<K>(key: K | null, onLeave: () => void): void {
  const latest = useRef(onLeave);
  latest.current = onLeave;
  const was = useRef<K | null>(key);
  useEffect(() => {
    if (was.current !== null && was.current !== key) latest.current();
    was.current = key;
  }, [key]);
}
