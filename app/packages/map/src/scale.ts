/**
 * THE SCALE BAR'S ARITHMETIC, shared so the phone draws the web's bar (N2 review).
 *
 * The web uses MapLibre GL JS's `ScaleControl` (`web/useMapInstance.ts`: metric, `maxWidth` 92, bottom-right). The RN binding's
 * scale bar is top-left only on Android and follows the device's units, so `Map.native.tsx` draws its own from this:
 * the ground distance `maxWidth` pixels span at the centre, rounded DOWN to 1, 2, 3 or 5 × 10ⁿ metres (GL JS's
 * `getRoundNum`), and the bar shortened in proportion. MapLibre's world is 512 px wide at z0.
 */
const EARTH_CIRCUMFERENCE_M = 40075016.686;

function roundNum(n: number): number {
  const pow10 = 10 ** (`${Math.floor(n)}`.length - 1);
  let d = n / pow10;
  d = d >= 10 ? 10 : d >= 5 ? 5 : d >= 3 ? 3 : d >= 2 ? 2 : d >= 1 ? 1 : Math.round(d * 10) / 10;
  return pow10 * d;
}

/** The bar for a camera at `lat`, `zoom`: its width in px (≤ `maxWidth`) and its label ("5 km", "200 m"). */
export function metricScale(lat: number, zoom: number, maxWidth: number): { width: number; label: string } {
  const perPx = (EARTH_CIRCUMFERENCE_M * Math.cos((lat * Math.PI) / 180)) / (512 * 2 ** zoom);
  const max = perPx * maxWidth;
  const metres = roundNum(max);
  return { width: (maxWidth * metres) / max, label: metres >= 1000 ? `${metres / 1000} km` : `${metres} m` };
}
