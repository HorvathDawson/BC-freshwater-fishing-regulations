/**
 * The zoom ladder — which water is drawn at which zoom, as a number rather than a picture.
 *
 * WHY THE APP NEEDS IT AT ALL. Every water on the map is a tile feature, and tippecanoe
 * already stamped each one with the first zoom it appears at, so the atlas thins itself out
 * as you zoom away with nothing in the app deciding anything. The gauge dots are the one
 * exception: they are a GeoJSON source refreshed every half hour, and a GeoJSON source has
 * no ladder. Left alone every station in the province drew at every zoom, so at z6 a dot for
 * a creek gauge floated over country where its creek had thinned out four zooms earlier — a
 * reading with no water under it, which is precisely the "nearest gauge" mistake this whole
 * subsystem exists to refuse.
 *
 * So a dot appears exactly when the reach it measures appears. Same input (FWA stream
 * magnitude), same stops, and the stops are NOT retyped here from memory: they are generated
 * into `pipeline/tiles/tile-contract.json` and `app/tools/tile-contract.test.ts` fails if
 * this copy and that one disagree. Drift is a red test rather than a floating dot.
 */

/** (minimum magnitude, first zoom drawn), descending — mirrors `pipeline/tiles/ladder.py`. */
export const MAGNITUDE_LADDER: readonly (readonly [number, number])[] = [
  [20000, 4], [5000, 5], [1000, 6], [250, 7], [100, 8],
  [50, 9], [20, 10], [10, 11], [5, 12], [2, 13], [0, 14],
];

export const MIN_ZOOM = 4;
export const MAX_ZOOM = 14;

/**
 * First zoom a stream of this magnitude is drawn at.
 *
 * A missing magnitude is NOT a small one — it is a station whose node never got a headwater
 * count. It goes to the bottom of the ladder rather than being hidden, because a gauge that
 * exists and is reporting is a fact, and suppressing it entirely would be a stronger claim
 * than we can support.
 */
export function zoomForMagnitude(magnitude: number | null | undefined): number {
  const m = magnitude ?? 0;
  for (const [threshold, zoom] of MAGNITUDE_LADDER) if (m >= threshold) return zoom;
  return MAX_ZOOM;
}
