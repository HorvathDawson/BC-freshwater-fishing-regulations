import { now } from "./clock";
/**
 * (T287) A DECODE IN SLICES: the release's binary decoders (paint, status index, method, salmon slices) are written once
 * as generators that `yield` every few thousand rows; `runNow` runs one to the end (the plain `decodeX`), `runSliced`
 * runs it a time budget at a time with the screen's turn between, so a 150 ms decode never holds the JS thread in one
 * piece (owner 2026-10-29: "it should not freeze. it should update in background cleanly"). Same answer either way.
 */
export type Steps<T> = Generator<void, T, void>;

/** How often a decoder loop offers to yield (rows): cheap enough to be free in one go, fine enough for a 8 ms slice. */
export const YIELD_EVERY = 2048;

/** Run a decoder to the end, synchronously. */
export function runNow<T>(g: Steps<T>): T {
  for (;;) { const r = g.next(); if (r.done) return r.value; }
}

/** One turn of the event loop: a MessageChannel where there is one (a browser: no 4 ms timer clamp), else a 0 ms timer. */
export function nextTurn(): Promise<void> {
  const MC = (globalThis as { MessageChannel?: typeof MessageChannel }).MessageChannel;
  if (MC) {
    return new Promise((resolve) => {
      const ch = new MC();
      ch.port1.onmessage = () => { ch.port1.close(); resolve(); };
      ch.port2.postMessage(0);
    });
  }
  return new Promise((resolve) => setTimeout(resolve, 0));
}

/** The JS thread time one slice may take (ms): well under the 50 ms a long task is, leaving a frame its turn. */
export const SLICE_MS = 8;

/**
 * (T741) The slice a LAUNCH's paint decode may take: the boot screen is all that is on screen (its fish is a native-driver
 * animation, so a busy JS thread does not stutter it) and nothing on it answers a finger. Each turn between slices costs
 * a timer tick on React Native (no MessageChannel there: `nextTurn` is a 0 ms timer), so 8 ms slices spent ~25 turns on
 * the paint. 40 ms is still under the 50 ms a long task is and far under the 300 ms stall limit. ONLY for a launch:
 * whenever a map is on screen (an update applied in the session) the decode keeps `SLICE_MS`.
 */
export const LAUNCH_SLICE_MS = 40;

/** Run a decoder `budgetMs` at a time, the event loop's turn between slices. */
export async function runSliced<T>(g: Steps<T>, budgetMs = SLICE_MS, turn: () => Promise<void> = nextTurn): Promise<T> {
  let start = now();
  for (;;) {
    const r = g.next();
    if (r.done) return r.value;
    if (now() - start >= budgetMs) { await turn(); start = now(); }
  }
}
