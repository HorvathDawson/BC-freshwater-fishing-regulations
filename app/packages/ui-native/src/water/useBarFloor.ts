import { useRef } from "react";
import type { LayoutChangeEvent } from "react-native";

/**
 * (T731, owner 2026-11-16) THE MAP'S BAR KEEPS ITS HEIGHT ACROSS A STEP INTO ANOTHER WATER. The bar's height follows what
 * the part has: a one-part water has no "Show all N parts" row and may hold no exit line, so a step from the Chilliwack
 * into the Vedder made the bar 122 px shorter and the map above it grew by that much. `key` is the water drawn (null
 * while the next is read); when it changes the bar is floored at the height it last measured, so nothing under the map
 * moves while stepping. A water opened fresh has no floor (no empty row on a one-part water that was not stepped to).
 */
export function useBarFloor(key: string | null): { minHeight: number | undefined; onLayout: (e: LayoutChangeEvent) => void } {
  const s = useRef<{ key: string | null; last: number; floor: number | undefined }>({ key: null, last: 0, floor: undefined });
  if (key !== null && key !== s.current.key) {
    if (s.current.key !== null && s.current.last > 0) s.current.floor = s.current.last;
    s.current.key = key;
  }
  return { minHeight: s.current.floor, onLayout: (e) => { s.current.last = e.nativeEvent.layout.height; } };
}
