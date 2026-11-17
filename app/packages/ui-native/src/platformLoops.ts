/**
 * (T814, iPhone build 36: the fish stood still after a reopen mid-download, and on a second phone never moved on the first
 * download) KEEP A LOADER'S LOOPS RUNNING, with no per-frame JS. Used by the native halves of both loaders (the circling
 * `FishSpinner` and the `CohoLoader`); their web halves are CSS and resume by themselves.
 *
 * A platform-driven loop can be dropped on iOS — the app going inactive or to the background, a view mounted before it is
 * attached (a launch iOS makes in the background, the first launch) — and nothing starts it again. So, without guessing
 * which:
 *   - the loops are started again whenever the app comes to the front (`AppState` "active") and on the view's first
 *     layout (the returned `onLayout`);
 *   - each loop is ONE `Animated.timing` with `iterations: -1`: the very native animation `Animated.loop` sends for a
 *     native-driven loop (`_startNativeLoop`), but with the end callback `Animated.loop` drops on that path (RN 0.88
 *     `loopImpl`). An infinite loop never ends by itself, so an end this hook did not ask for is a dropped loop: it is
 *     started again (`RESTART_MS` later, in front; else at the next "active"). The callback runs once per drop, never per
 *     frame.
 * Each start resets the values to 0 (the loader jumps to its first frame once, on a restart).
 *
 * (Owner 2026-11-17: "we will ignore reduced motion for this") Reduce Motion does not stop a loader: one that stood still
 * read as a hung app (the second iPhone had it on).
 */
import { useCallback, useEffect, useRef } from "react";
import { Animated, AppState, Easing } from "react-native";

/** A loop that ended though nothing here stopped it is started again after this long: never a tight retry. */
const RESTART_MS = 1000;

/** One loop: the value driven 0 -> 1, and how long one pass takes. */
export type PlatformLoop = readonly [value: Animated.Value, durationMs: number];

/** Runs every loop on the platform driver and keeps it running; returns the loader view's `onLayout`. */
export function usePlatformLoops(loops: readonly PlatformLoop[]): () => void {
  const start = useRef<() => void>(() => {});
  const laidOut = useRef(false);
  // the loops are the caller's refs and constants: a new array each render is the same loops
  const held = useRef(loops);
  useEffect(() => {
    let live = true;
    let gen = 0;
    let retry: ReturnType<typeof setTimeout> | null = null;
    let running: Animated.CompositeAnimation[] = [];
    const stop = () => {
      gen += 1;                       // first, so the ends the stops cause are known as ours
      for (const l of running) l.stop();
      running = [];
      if (retry !== null) { clearTimeout(retry); retry = null; }
    };
    const go = () => {
      stop();
      const mine = gen;
      running = held.current.map(([value, duration]) => {
        value.setValue(0);
        const l = Animated.timing(value, {
          toValue: 1, duration, easing: Easing.linear, iterations: -1, useNativeDriver: true,
        });
        l.start(() => {
          // stopped here (a restart, the unmount), or one restart already waits for every loop
          if (!live || gen !== mine || retry !== null) return;
          if (AppState.currentState === "active") retry = setTimeout(go, RESTART_MS);
        });
        return l;
      });
    };
    start.current = go;
    go();
    const sub = AppState.addEventListener("change", (s) => { if (s === "active") go(); });
    return () => { live = false; start.current = () => {}; sub.remove(); stop(); };
  }, []);
  return useCallback(() => {
    if (laidOut.current) return;
    laidOut.current = true;
    start.current();
  }, []);
}
