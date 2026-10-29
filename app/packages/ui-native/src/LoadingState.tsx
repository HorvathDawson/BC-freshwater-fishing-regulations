/**
 * (T257, owner 2026-10-29) THE ONE WAITING STATE: every loading or waiting state in the app is a `FishSpinner`, and every
 * one of them is drawn through here, so none of them flashes on a quick load.
 *
 *   - NOT FOR A QUICK LOAD: the spinner shows only once the wait has lasted `SPINNER_TIMING.showAfterMs` (300 ms). Until
 *     then its place is held empty at its own size, so nothing jumps when it does appear and nothing flashes when it
 *     never does.
 *   - NO FLICKER: once shown it stays at least `SPINNER_TIMING.holdMs` (400 ms), even when the wait ends sooner — a caller
 *     that branches on `useDelayedShow(busy).pending` keeps its waiting branch up for that long.
 *
 * `tools/one-spinner.test.ts` fails on a `FishSpinner` used anywhere but here, an `ActivityIndicator`, or a hand-made
 * turning loop.
 */
import { useEffect, useRef, useState } from "react";
import { View, type ViewStyle } from "react-native";
import { FishSpinner } from "./FishSpinner";
import type { Palette } from "./theme";

export const SPINNER_TIMING = { showAfterMs: 300, holdMs: 400 } as const;

export interface DelayedShow {
  /** draw the waiting state: busy, or still inside the hold after a spinner that was shown */
  readonly pending: boolean;
  /** draw the spinner itself: the wait has lasted `showAfterMs` (and the hold has not run out) */
  readonly shown: boolean;
}

/**
 * The timing every waiting state shares. `busy` the wait is on; `immediate` shows the spinner at once (a surface that
 * already waited before it appeared, like the synopsis bar).
 */
export function useDelayedShow(busy: boolean, { immediate = false }: { immediate?: boolean } = {}): DelayedShow {
  const [shown, setShown] = useState(busy && immediate);
  const shownAt = useRef<number | null>(busy && immediate ? Date.now() : null);
  useEffect(() => {
    let t: ReturnType<typeof setTimeout> | null = null;
    if (busy) {
      if (shownAt.current === null) {
        const show = () => { shownAt.current = Date.now(); setShown(true); };
        if (immediate) show(); else t = setTimeout(show, SPINNER_TIMING.showAfterMs);
      }
    } else if (shownAt.current !== null) {
      const left = SPINNER_TIMING.holdMs - (Date.now() - shownAt.current);
      const hide = () => { shownAt.current = null; setShown(false); };
      if (left <= 0) hide(); else t = setTimeout(hide, left);
    }
    return () => { if (t !== null) clearTimeout(t); };
  }, [busy, immediate]);
  return { pending: busy || shown, shown };
}

/**
 * The spinner (and what goes beside it: a caption, a progress line) in a box of the caller's style. `shown` from the
 * caller's `useDelayedShow` (its hold); absent, it times its own delay from when it mounted.
 */
export function LoadingState({ palette, size = 96, label, colour, style, shown, immediate = false, children }: {
  palette: Palette;
  size?: number;
  /** said aloud */
  label?: string;
  colour?: string;
  /** the box round the spinner and its caption */
  style?: ViewStyle;
  shown?: boolean;
  immediate?: boolean;
  /** drawn after the spinner, with it: a caption */
  children?: React.ReactNode;
}) {
  const own = useDelayedShow(true, { immediate });
  const on = shown ?? own.shown;
  const spinner = on
    ? <FishSpinner palette={palette} size={size} {...(label !== undefined ? { label } : {})} {...(colour ? { colour } : {})} />
    : <View testID="loading-held" style={{ width: size, height: size }} />;
  if (!style && children === undefined) return spinner;
  return <View testID={on ? "loading-state" : undefined} style={style}>{spinner}{on ? children : null}</View>;
}
