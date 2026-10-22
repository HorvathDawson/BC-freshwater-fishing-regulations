/**
 * THE WEB MAP'S LONG PRESS (owner 2026-10-21: press and hold the browse map to drop a spot).
 *
 * MapLibre Native reports a long press of its own (`Map.native.tsx`, the SDK's `onLongPress`, decided by its gesture
 * detector beside its pan and pinch). maplibre-gl has none: a mouse's right click and Chrome-on-Android's long touch
 * reach it as `contextmenu`, but Safari on a phone sends nothing. So the web map times a held finger itself, by the one
 * policy (`@app/core` `LONG_PRESS`, `longPressHolds`), and takes `contextmenu` too — whichever comes first fires, once
 * per press:
 *
 *   down     a press began (touchstart; mousedown with `touches` 0 — a mouse is never timed: holding the button is a pan).
 *            The mousedown a phone browser makes up after a touch (within `EMULATED_MS` of it) is that touch, not a
 *            new press, so it does not clear `fired` and the click after a long press is still swallowed
 *   move     the finger moved, or a second one came down: past the slop, or a pinch, it is no long press
 *   up       lifted, cancelled (`touch`), or the map began to move
 *   context  the platform's own long press or a right click: fires unless this press already did
 *   fired    the press that just ended fired, so the click that may follow it is not also a tap
 *
 * Pure apart from the clock it is handed, so a test drives it with a fake one.
 */
import { LONG_PRESS, longPressHolds } from "@app/core";

export interface LongPressClock {
  set(fn: () => void, ms: number): unknown;
  clear(handle: unknown): void;
  /** Milliseconds, for telling a phone's made-up mousedown from a real one. */
  now(): number;
}

/** A mousedown this soon (ms) after a touch is the browser's emulation of that touch, not a new press. */
export const EMULATED_MS = 1000;

export interface LongPressWatch {
  down(x: number, y: number, touches: number): void;
  move(x: number, y: number, touches: number): void;
  /** `touch`: a finger lifted (touchend, touchcancel) — the mousedown a phone makes up next is that touch's. */
  up(touch?: boolean): void;
  context(x: number, y: number): void;
  fired(): boolean;
  dispose(): void;
}

const REAL_CLOCK: LongPressClock = {
  set: (fn, ms) => setTimeout(fn, ms),
  clear: (h) => clearTimeout(h as ReturnType<typeof setTimeout>),
  now: () => Date.now(),
};

export function longPressWatch(fire: (x: number, y: number) => void,
                               clock: LongPressClock = REAL_CLOCK): LongPressWatch {
  let start: { x: number; y: number } | null = null;
  let timer: unknown = null;
  let done = false;
  let touched = -Infinity;
  const stop = () => {
    if (timer !== null) clock.clear(timer);
    timer = null;
    start = null;
  };
  const go = (x: number, y: number) => {
    stop();
    done = true;
    fire(x, y);
  };
  return {
    down: (x, y, touches) => {
      if (touches === 0 && clock.now() - touched < EMULATED_MS) return;     // the touch's own made-up mouse
      if (touches > 0) touched = clock.now();
      stop();
      done = false;
      if (!longPressHolds(0, 0, touches)) return;
      start = { x, y };
      timer = clock.set(() => { timer = null; if (start) go(start.x, start.y); }, LONG_PRESS.ms);
    },
    move: (x, y, touches) => {
      touched = clock.now();
      if (start && !longPressHolds(x - start.x, y - start.y, touches)) stop();
    },
    up: (touch = false) => { if (touch) touched = clock.now(); stop(); },
    context: (x, y) => { if (!done) go(x, y); },
    fired: () => done,
    dispose: stop,
  };
}
