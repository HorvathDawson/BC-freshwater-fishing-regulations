/**
 * (T896) DOES THE WHOLE KEY FIT THE BAR? Decided by layout, never by a window-width breakpoint: the bar's own width
 * against the key's width laid on one row (a hidden copy of its entries that cannot wrap, so a longer label, a dated
 * "Closed Sep 30" or the gear marks are counted as they come). Unknown until both are measured (null).
 */
import { useCallback, useState } from "react";
import type { LayoutChangeEvent } from "react-native";

/** the key fits when its one-row width plus the bar's side padding is within the bar (half a pixel of rounding slack) */
export function keyFits(natural: number | null, bar: number | null, padX: number): boolean | null {
  if (natural == null || bar == null) return null;
  return natural + 2 * padX <= bar + 0.5;
}

export function useKeyFit(padX: number) {
  const [bar, setBar] = useState<number | null>(null);
  const [natural, setNatural] = useState<number | null>(null);
  const onBar = useCallback((e: LayoutChangeEvent) => setBar(Math.round(e.nativeEvent.layout.width * 2) / 2), []);
  const onKey = useCallback((e: LayoutChangeEvent) => setNatural(Math.round(e.nativeEvent.layout.width * 2) / 2), []);
  return { fits: keyFits(natural, bar, padX), onBar, onKey };
}
