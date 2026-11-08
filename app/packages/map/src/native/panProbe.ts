import { now as clockNow } from "@app/core";
/**
 * (T273) THE PAN PROBE — test-bed measurement only, beside T74's grey-frame probe (`probe.ts`). On while
 * `globalThis.__panProbe` is set (apps/mobile sets it from `EXPO_PUBLIC_PAN_PROBE=1`, the test bed's build); otherwise
 * every call here is a no-op and no timer runs.
 *
 * What it counts, per second with anything in it, one line to the log (`[pan] {…}`, `adb logcat -s ReactNativeJS`):
 *   lag      the JS thread's worst delay that second (a 50 ms timer's overshoot, ms) and how many ticks were >100 ms late
 *   tiles    atlas tile events (`MLRNTileIds`) the colouring took, and the section labels they produced
 *   repeats  (T420) repeat-request events (no ids: the tile's ids came once already)
 *   sends    `setFeatureStates` bridge calls, and their worst / mean round trip (ms) — label → acknowledged on the map
 *   renders  the native Map component's renders
 *   frames   (T499) frames the map drew (`full`: of them, fully) — a map at rest with no input draws none
 *   src      (T499) writes the map made with no camera move: a GeoJSON source handed new data, the highlight re-sent,
 *            pushed data re-sent (`src.copy`, `src.lit` …) — what redraws a map left idle
 */
interface Win { lagMax: number; late: number; tiles: number; repeats: number; labels: number; sends: number; ackMax: number; ackSum: number;
  acks: number; renders: number; frames: number; full: number; src: Record<string, number> }
const fresh = (): Win => ({ lagMax: 0, late: 0, tiles: 0, repeats: 0, labels: 0, sends: 0, ackMax: 0, ackSum: 0, acks: 0, renders: 0,
  frames: 0, full: 0, src: {} });

const on = () => !!(globalThis as { __panProbe?: boolean }).__panProbe;
let w = fresh();
let started = false;

function start(): void {
  if (started || !on()) return;
  started = true;
  let due = clockNow() + 50;
  setInterval(() => {
    const now = clockNow(), lag = Math.max(0, now - due);
    due = now + 50;
    if (lag > w.lagMax) w.lagMax = lag;
    if (lag > 100) w.late += 1;
  }, 50);
  setInterval(() => {
    const busy = w.tiles || w.repeats || w.sends || w.renders || w.late || w.frames || Object.keys(w.src).length;
    if (busy) {
      const ackMean = w.acks ? Math.round(w.ackSum / w.acks) : 0;
      console.log(`[pan] ${JSON.stringify({ t: clockNow(), lagMax: w.lagMax, late: w.late, tiles: w.tiles, repeats: w.repeats, labels: w.labels,
                                            sends: w.sends, ackMax: w.ackMax, ackMean, renders: w.renders,
                                            frames: w.frames, full: w.full, src: w.src })}`);
    }
    w = fresh();
  }, 1000);
}

export const panProbe = {
  tile(labels: number): void { if (!on()) return; start(); w.tiles += 1; w.labels += labels; },
  /** (T420) a repeat-request event (no ids) */
  repeat(): void { if (!on()) return; start(); w.repeats += 1; },
  /** A `setFeatureStates` call sent: call the returned function when it is acknowledged. */
  send(): () => void {
    if (!on()) return () => {};
    start();
    w.sends += 1;
    const t0 = clockNow();
    return () => { const ms = clockNow() - t0; w.acks += 1; w.ackSum += ms; if (ms > w.ackMax) w.ackMax = ms; };
  },
  render(): void { if (!on()) return; start(); w.renders += 1; },
  /** (T420/T401) is the probe on: the map registers its frame listeners only then */
  on,
  /** (T420/T401) the camera began to move (`onRegionWillChange`) */
  moving(): void { if (!on()) return; drawn.moving(clockNow()); },
  /** (T420/T401) the camera came to rest (`onRegionDidChange`) */
  settled(): void { if (!on()) return; drawn.settled(clockNow()); },
  /** (T420/T401) a frame drawn; `full`: every tile the view needs was in it (MapLibre's "fully rendered") */
  frame(full: boolean): void {
    if (!on()) return;
    start();
    // (T499) `frames`: the binding's every-frame event; `full`: its fully-drawn one (counted apart: they are two events)
    if (full) w.full += 1; else w.frames += 1;
    const t = clockNow(); drawn.frame(full, t); swaps.frame(t);
  },
  /** (T499) a write the map made: a source's new data, the highlight or pushed data sent again */
  src(what: string): void { if (!on()) return; start(); w.src[what] = (w.src[what] ?? 0) + 1; },
  /** (T451) a new style handed to the binding (`why`: what changed since the last one) */
  styleHanded(why: string): void { if (!on()) return; swaps.handed(why, clockNow()); },
  /** (T451) a style finished loading */
  styleLoaded(): void { if (!on()) return; swaps.loaded(clockNow()); },
  /** (T451) the first labels after a style load acknowledged on the map */
  acked(): void { if (!on()) return; swaps.acked(clockNow()); },
  /** (T451) what a section labelled before the swap holds right after it */
  kept(held: boolean | null): void { if (!on()) return; swaps.kept(held); },
};

/**
 * (T451) STYLE SWAPS, measured: one `[swap] {…}` line per style load —
 *   handed    styles handed to the binding since the last load (2+: a switch that swapped twice), and why each was
 *   loadMs    first hand-over → loaded
 *   bare      frames drawn between the load and the first labels' acknowledgement (drawn without the engine's colours)
 *   bareMs    load → acknowledgement
 *   kept      a section labelled before the load still wore its state after it (feature-state survived the swap)
 */
export function swapTracker(log: (line: string) => void = (l) => console.log(l)) {
  let why: string[] = [], firstAt = -1, loadAt = -1, bare = 0, kept: boolean | null = null, open = false;
  let timer: ReturnType<typeof setTimeout> | null = null;
  const done = (ackAt: number | null) => {
    if (timer) { clearTimeout(timer); timer = null; }
    log(`[swap] ${JSON.stringify({ t: loadAt, handed: why.length, why, loadMs: firstAt >= 0 ? loadAt - firstAt : null, bare,
                                   bareMs: ackAt === null ? null : ackAt - loadAt, kept })}`);
    open = false; why = []; firstAt = -1; kept = null;
  };
  return {
    handed(w: string, t: number): void { if (open) done(null); if (firstAt < 0) firstAt = t; why.push(w); },
    loaded(t: number): void {
      if (open) done(null);
      loadAt = t; bare = 0; open = true;
      timer = setTimeout(() => { if (open) done(null); }, 4000);
    },
    frame(_t: number): void { if (open) bare += 1; },
    acked(t: number): void { if (open) done(t); },
    kept(h: boolean | null): void { kept = h; },
  };
}
const swaps = swapTracker();

/**
 * (T420/T401) STREAMS DRAWN, from MapLibre's own word: each frame is drawn either FULLY (every tile the view needs is
 * loaded) or partially (some are still a parent placeholder or missing: the pop-in). One line per camera move to the log,
 * `[drawn] {t, ms, frames, partial, firstPartialMs, never}`:
 *   frames / partial   frames drawn while the camera moved, and how many of them were missing tiles (pop-in DURING the move)
 *   ms                 from the camera at rest to the first full frame (0: the last frame of the move was already full)
 *   never              no full frame within 8 s of rest
 */
export function drawnTracker(log: (line: string) => void = (l) => console.log(l)) {
  let moveAt = -1, restAt = -1, frames = 0, partial = 0, lastFull = true, waiting = false, timer: ReturnType<typeof setTimeout> | null = null;
  const done = (ms: number | null, t: number) => {
    waiting = false;
    if (timer) { clearTimeout(timer); timer = null; }
    log(`[drawn] ${JSON.stringify({ t, ms, frames, partial, moveMs: restAt - moveAt, never: ms === null })}`);
  };
  return {
    moving(t: number): void {
      if (waiting) done(null, t);                   // moved again before the last rest was ever full
      if (moveAt > restAt) return;                  // still the same move
      moveAt = t; frames = 0; partial = 0;
    },
    settled(t: number): void {
      if (moveAt <= restAt) return;                 // a settle with no move (a style swap)
      restAt = t;
      if (lastFull) { done(0, t); return; }
      waiting = true;
      timer = setTimeout(() => { if (waiting) done(null, restAt + 8000); }, 8000);
    },
    frame(full: boolean, t: number): void {
      lastFull = full;
      if (moveAt > restAt) { frames += 1; if (!full) partial += 1; return; }
      if (waiting && full) done(t - restAt, t);
    },
  };
}
const drawn = drawnTracker();
