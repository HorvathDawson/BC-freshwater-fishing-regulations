/**
 * (T40) The OS text size as a capped Text sees it: `PixelRatio.getFontScale()` (1 = the default; iOS and Android reach
 * past 3), held to [1, CAPPED_SCALE]. For the layout around the water's header — the rows whose height or line count is
 * set in one-scale pixels (the name's two lines, the place label's two-line slot), so they grow with the text they hold.
 *
 * Read on every render; `useWindowDimensions` re-renders the caller when the OS text size changes (react-native emits a
 * Dimensions change carrying the new fontScale; the web, a resize).
 */
import { PixelRatio, useWindowDimensions } from "react-native";
import { CAPPED_SCALE } from "./type";

export function useCappedScale(): number {
  useWindowDimensions();
  return Math.min(Math.max(PixelRatio.getFontScale() || 1, 1), CAPPED_SCALE);
}
