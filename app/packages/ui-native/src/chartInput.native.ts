/**
 * The flow chart's browser-only input, on a phone: there is no wheel, and the pointer is a finger. The pinch, drag
 * and taps are react-native-gesture-handler's (`Hydrograph.tsx`), the same on both targets. See `chartInput.web.ts`.
 */
import type { RefObject } from "react";

/** No wheel on a phone. */
export function useChartWheel(_ref: RefObject<unknown>, _onWheel: (x: number, deltaY: number) => void,
                              _enabled: boolean): void {}

/** No hovering pointer on a phone: a tap reads the value instead. */
export function useChartHover(_ref: RefObject<unknown>, _onHover: (x: number | null) => void,
                              _enabled: boolean): void {}

/** A phone's pointer is a finger. */
export function chartPointer(): "touch" | "pointer" {
  return "touch";
}
