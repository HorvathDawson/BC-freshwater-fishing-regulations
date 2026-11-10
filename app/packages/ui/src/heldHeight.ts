/**
 * (T581 follow-up, T568) A BLOCK THAT DOES NOT SHRINK WHILE ITS QUESTION IS THE SAME. The estimate block above the route
 * map is a different height under Flow and under Level on a forecast day (one headline wraps to two lines, the other
 * answers "no estimate"), so the map under it moved on the toggle. The block keeps the tallest height it has been drawn
 * at for one (water, day), the quantity not part of the key: after the first look at each, the map's top is the same in
 * both. A different water or day starts again. Words are never cut to fit.
 */
import { useCallback, useState } from "react";

export interface Held { readonly key: string; readonly h: number }

/** The held height after a block of height `h` was drawn under `key`: the tallest for this key, or `h` for a new one. */
export function holdHeight(prev: Held, key: string, h: number): Held {
  if (prev.key !== key) return { key, h };
  return h > prev.h ? { key, h } : prev;
}

export function useHeldHeight(key: string): { min: number; hold: (h: number) => void } {
  const [held, setHeld] = useState<Held>({ key, h: 0 });
  const hold = useCallback((h: number) => setHeld((p) => holdHeight(p, key, Math.ceil(h))), [key]);
  return { min: held.key === key ? held.h : 0, hold };
}
