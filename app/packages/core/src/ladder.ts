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
 * into `pipeline/deliver/tiles/tile-contract.json` and `app/tools/tile-contract.test.ts` fails if
 * this copy and that one disagree. Drift is a red test rather than a floating dot.
 */

/** (minimum magnitude, first zoom drawn), descending — mirrors `pipeline/deliver/tiles/ladder.py`. */
export const MAGNITUDE_LADDER: readonly (readonly [number, number])[] = [
  [20000, 4], [5000, 5], [1000, 6], [250, 7], [100, 8],
  [50, 9], [20, 10], [10, 11], [5, 12], [2, 13], [0, 14],
];

export const MIN_ZOOM = 4;
export const MAX_ZOOM = 14;

/**
 * Where the camera may go. `[[west, south], [east, north]]`, MapLibre's `maxBounds` shape.
 *
 * BOTH OF THESE WERE DEAD until now: the constants existed and the map was constructed
 * without `minZoom`, `maxZoom` or `maxBounds`, so you could zoom out to the whole globe and
 * pan into the Pacific. Neither shows anything — the atlas has no features below z4, and
 * the basemap archive is clipped to British Columbia — so every zoom past the end is a
 * screen the app has no data for, drawn as ragged basemap and bare paper.
 *
 * British Columbia is lon -139.06..-114.05, lat 48.23..60.00. Eight degrees of slack lets
 * a reader see where the province sits without letting them leave it, and keeps the widest
 * possible viewport at MIN_ZOOM (about 19 x 24 degrees on a phone) well inside the mask
 * frame that `pipeline/deliver/tiles/boundary.py` builds at 30 degrees. If either number
 * moves, check the other.
 */
export const CAMERA_BOUNDS: readonly [readonly [number, number],
                                      readonly [number, number]] =
  [[-147.1, 40.2], [-106.1, 68.0]];

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
