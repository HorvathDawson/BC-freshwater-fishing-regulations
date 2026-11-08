/**
 * (T498, owner 2026-11-08 on 0.6.9: "tapping a stream opens the pane janky and flickery — the page shows, then its
 * content loads in") A WATER'S PANE IS SHOWN WHOLE. Measured on the emulator (testbed `pane-open`): a cold tap showed
 * FIVE screens in 250 ms — the map; the pane's frame with only the name; the page without its Salmon card; the page with
 * it; the page pushed down by the access notice (and the strip losing its Map view, fixed in `openWater.ts`).
 *
 * So a pane opened over the browse map is BUILT HIDDEN: the map stays on screen (untouchable) while the pane mounts
 * under it at its final size, and the pane is shown in one frame once the view in front says it has everything it
 * draws (`usePaneReady`) — or once `PANE_HOLD_MS` has passed, when it shows its own waiting state as before (a slow
 * line: the reader is never left looking at a map that ignores them). ONE PATH: every way in (a map tap, a search result,
 * a spot, the Runs sheet, a deep path from the map) changes the route from browse to a water, and that change is what is
 * held, here, for all of them. A start on a water (a landing) has no map to hold: shown at once. Held for the
 * Regulations view only (the Shell's call): the Conditions and Map views carry a live map of their own, and building
 * one under the held browse map crashed MapLibre's render thread on the emulator (two Vulkan maps, one let go).
 */
import { createContext, useContext, useEffect, useState } from "react";

/** How long the map is held over a pane still building: past it, the pane shows its own waiting state. */
export const PANE_HOLD_MS = 1200;

/** The view in front says it is drawn whole (nothing it draws still loading). Null: no pane is being held. */
export const PaneReadyContext = createContext<(() => void) | null>(null);

/**
 * A water view's word that it is drawn whole: `ready` once nothing it shows is still coming (an error or an
 * unavailable answer is drawn whole too — the reader should see it, not the map).
 */
export function usePaneReady(ready: boolean): void {
  const say = useContext(PaneReadyContext);
  useEffect(() => { if (ready) say?.(); }, [ready, say]);
}

/**
 * The hold, keyed on whether a water is open (`open`) and whether a map is on screen to hold over it (`overMap`): a
 * browse → water change made while the map was showing is held until the pane is ready or `PANE_HOLD_MS` runs out.
 * Returns whether the pane is held now, and the ready callback for `PaneReadyContext`.
 */
export function usePaneHold(open: boolean, overMap: boolean, holdMs = PANE_HOLD_MS): { holding: boolean; ready: () => void } {
  // derived in render (React's "state from a prop change"): the render that opens the pane already holds it, so no
  // frame of a half-built pane is ever committed
  const [s, setS] = useState({ open, holding: false });
  let cur = s;
  if (s.open !== open) {
    cur = { open, holding: open && overMap };
    setS(cur);
  }
  const [ready] = useState(() => () => setS((x) => (x.holding ? { ...x, holding: false } : x)));
  useEffect(() => {
    if (!cur.holding) return;
    const t = setTimeout(ready, holdMs);
    return () => clearTimeout(t);
  }, [cur.holding, holdMs, ready]);
  return { holding: cur.holding, ready };
}
