/**
 * (T723 G5) A ROUGH LOCATION, SAID. With approximate-only location (iOS "Precise: Off", Android "Approximate") the phone
 * hands back a fix that can be kilometres off, and "At my location" pinned a spot there without a word. The bar is the
 * one the phone's own module holds a recent fix to before it will pin a spot by it (`locate.native.ts`:
 * `requiredAccuracy: 100`, metres); a fix reported worse than that gets one sentence on the capture and the reader goes on
 * (never blocked: the pin is theirs to judge, and the map under it is in front of them).
 */
import { SPOT_CAPTURE_AT } from "./mapRail";

/** Metres: a fix whose reported accuracy is worse than this is "rough". */
export const ROUGH_FIX_M = 100;

/** "about 250 m" under a kilometre (to 10 m), else "about 3.2 km" (one decimal under 10 km, whole above). */
function about(m: number): string {
  if (m < 1000) return `about ${Math.round(m / 10) * 10} m`;
  const km = m / 1000;
  return `about ${km < 10 ? (Math.round(km * 10) / 10).toString() : Math.round(km).toString()} km`;
}

/** The sentence for a rough fix, or null when the fix is good (or its accuracy is not known). */
export function roughFixLine(accuracyM: number | null | undefined): string | null {
  if (accuracyM == null || !(accuracyM > ROUGH_FIX_M)) return null;
  return `Your phone placed you only to ${about(accuracyM)}, so this pin may not be where you are.`;
}

/** The capture's "tap the water" instruction for a pin that came from the phone's location: plus the rough-fix sentence. */
export function captureAtDetail(accuracyM: number | null | undefined): string {
  const rough = roughFixLine(accuracyM);
  return rough ? `${SPOT_CAPTURE_AT.detail} ${rough}` : SPOT_CAPTURE_AT.detail;
}
