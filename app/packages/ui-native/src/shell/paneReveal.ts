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
 * held, here, for all of them. A start on a water (a landing) has no map to hold: shown at once.
 *
 * (T508) EVERY VIEW, NOT ONLY REGULATIONS. A pane is whole when EVERY part of it that is still coming has arrived: each
 * part says so itself — a view's content (`usePaneReady`), and each live map in the pane (`usePaneMap`: its map has
 * drawn what it was handed, `MapProps.onDrawn` — the style, the lit sections written, the fit's camera at rest, then a
 * full render). The pane is shown when none is still waiting. On a phone the Conditions and Map views are held too
 * (`holdViews`); the web builds a map only once it is seen (`firstSeen`), so a held map there could never draw: the web
 * holds the Regulations view only.
 */
import { createContext, useContext, useEffect, useState } from "react";

/** How long the map is held over a pane still building: past it, the pane shows its own waiting state. */
export const PANE_HOLD_MS = 1200;

/** A held pane's waits: `wait()` is one part of the pane still coming; calling what it returns says that part is in. */
export interface PaneHold { wait(): () => void }

/** The hold over the pane being built (null: no pane is being held, nothing waits). */
export const PaneReadyContext = createContext<PaneHold | null>(null);

/**
 * A part of a water view says it is drawn whole: `ready` once nothing it shows is still coming (an error or an
 * unavailable answer is drawn whole too — the reader should see it, not the map). Until then, the held pane waits.
 */
export function usePaneReady(ready: boolean): void {
  const hold = useContext(PaneReadyContext);
  useEffect(() => (hold && !ready ? hold.wait() : undefined), [hold, ready]);
}

/**
 * (T508) A LIVE MAP IN A HELD PANE: the pane waits from the map's mount until it has drawn. Returns the map's
 * `onDrawn` once `armed` (the map has been handed what it is to draw: its lit sections and its fit), else undefined —
 * so a map's first draw of nothing (still loading its part) never counts. Undefined, too, when no pane is held.
 */
export function usePaneMap(armed = true): (() => void) | undefined {
  const hold = useContext(PaneReadyContext);
  const [done, setDone] = useState<{ f: () => void } | null>(null);
  useEffect(() => {
    if (!hold) { setDone(null); return; }
    const d = hold.wait();
    setDone({ f: d });
    return d;
  }, [hold]);
  return armed && done ? done.f : undefined;
}

/**
 * The hold, keyed on whether a water is open (`open`) and whether a map is on screen to hold over it (`overMap`): a
 * browse → water change made while the map was showing is held until no part of the pane is still waiting, or
 * `PANE_HOLD_MS` runs out. Returns whether the pane is held now, and the hold for `PaneReadyContext` (null: not held).
 */
export function usePaneHold(open: boolean, overMap: boolean, holdMs = PANE_HOLD_MS): { holding: boolean; hold: PaneHold | null } {
  // derived in render (React's "state from a prop change"): the render that opens the pane already holds it, so no
  // frame of a half-built pane is ever committed
  const [s, setS] = useState({ open, holding: false });
  let cur = s;
  if (s.open !== open) {
    cur = { open, holding: open && overMap };
    setS(cur);
  }
  const [h] = useState(() => {
    // `gen`: the hold a wait belongs to — a part of the last pane that says it is in late counts for nothing now
    let waiting = 0, seq = 0, gen = 0;
    // (test bed only, the open probe's build) how the hold ended: every part in, or the time ran out with some waiting
    const said = (how: string) => {
      if ((globalThis as { __openProbe?: boolean }).__openProbe) console.log(`[hold] ${JSON.stringify({ how, waiting })}`);
    };
    const release = () => setS((x) => (x.holding ? { ...x, holding: false } : x));
    const timeout = () => setS((x) => { if (x.holding) said("timeout"); return x.holding ? { ...x, holding: false } : x; });
    // checked after the effects of the commit that changed it have all run: one part's "in" (an effect's clean-up) is
    // never read before a part mounted in the same commit has said it waits
    const check = () => { const at = seq; queueMicrotask(() => { if (at === seq && waiting === 0) { said("whole"); release(); } }); };
    const hold: PaneHold = {
      wait() {
        waiting += 1; seq += 1;
        const mine = gen;
        let open = true;
        return () => { if (!open || mine !== gen) return; open = false; waiting -= 1; seq += 1; check(); };
      },
    };
    return { hold, check, timeout, reset: () => { waiting = 0; seq += 1; gen += 1; } };
  });
  if (cur !== s && cur.holding) h.reset();             // a new hold: nothing of the last pane waits in it
  useEffect(() => {
    if (!cur.holding) return;
    h.check();                                   // nothing waits (every part already whole): shown at once
    const t = setTimeout(h.timeout, holdMs);
    return () => clearTimeout(t);
  }, [cur.holding, holdMs, h]);
  return { holding: cur.holding, hold: cur.holding ? h.hold : null };
}
