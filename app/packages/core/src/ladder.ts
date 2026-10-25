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
  [50, 9], [20, 10], [10, 11], [5, 12], [0, 13],
];

export const MIN_ZOOM = 4;

/**
 * THE DEEPEST ZOOM THE ATLAS ARCHIVE HOLDS — the ladder's bottom rung (`MAGNITUDE_LADDER`), and the `maxzoom` the map
 * gives the atlas SOURCE. Read from the archive's own header and from `pipeline/deliver/tiles/tile-contract.json`;
 * `tools/overzoom.test.ts` holds this to both. z13 since T85 (decisions D32, 2026-10-24): z14 was 58% of the archive,
 * and the z13 tile is simplified at its top zoom to the old z14's ground error (`tippe.SIMPLIFICATION_AT_MAX`), so the
 * camera's overzoom draws it no coarser. AN ARCHIVE BUILT TO z14 STILL SERVES: the source asks for z13 and overzooms.
 *
 * There used to be one `MAX_ZOOM` here, used for the ladder, the camera and the fit alike. Those are three different
 * questions, and overzoom (owner 2026-10-21) is exactly where they come apart: the camera goes past the tiles, the
 * tiles do not. So there is no bare MAX_ZOOM any more — every caller names which ceiling it means.
 */
export const ATLAS_MAX_ZOOM = 13;

/** The basemap archive's deepest zoom (`basemap.pmtiles` header; `pipeline/deliver/tiles/basemap.py` MAX_ZOOM). */
export const BASEMAP_MAX_ZOOM = 14;

/**
 * HOW FAR THE CAMERA MAY GO — past the tiles (four zooms past the basemap's z14, five past the atlas's z13), which
 * MapLibre OVERZOOMS: the deepest tile is drawn magnified
 * (vector geometry re-tessellated, so lines and labels stay sharp; rasters scaled), no tile past a source's own
 * `maxzoom` is ever requested. "Keeps the resolution of the lower zoom", as the owner asked.
 *
 * WHY 18 AND NOT MapLibre's DEFAULT 22. A z14 tile is 4,096 units across and drawn 512 px wide, so one unit is 1/8 px
 * at z14 and 2 px at z18: z18 is the last zoom at which the archive's coordinate grid is still finer than a few
 * pixels (z19 is 4 px steps, z22 32 px). The ground agrees: z18 is about 0.2 m a pixel at 50°N and a phone is ~80 m
 * across — already finer than the Freshwater Atlas's own 1:20,000 source (~10 m), so a closer camera shows nothing a
 * reader can use, only the data's corners. The archive is also simplified at its top zoom (`tippe.py`, 10 units ≈
 * 4 m on the ground): at z18 that is up to 20 px of corner-cutting on a river bend, visible, and the price of going
 * this close without a tile rebuild.
 */
export const CAMERA_MAX_ZOOM = 18;

/**
 * THE CLOSEST A FIT GOES — a searched water, a highlighted section, a town. z14 (the atlas's deepest zoom until T85,
 * kept when the archive moved to z13 so no fit changed; z14 overzooms the z13 tile): a fit frames
 * the subject in its country (z14 is a phone ~2.5 km across), and anything closer is the reader's choice to make with a
 * pinch, not ours to make for a 40 m pond. The web passes it as `fitBounds`'s `maxZoom`; the native binding's
 * `fitBounds` takes no such option, so the box is widened first (`fitAtMost`).
 */
export const FIT_MAX_ZOOM = 14;

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
  return ATLAS_MAX_ZOOM;
}

/** Web Mercator, normalised to [0, 1] — the space tiles are cut in (and `panelCamera`'s in trace.ts). */
const LAT_LIMIT = 85.05112878;
const mercY = (lat: number) => {
  const p = (Math.min(LAT_LIMIT, Math.max(-LAT_LIMIT, lat)) * Math.PI) / 180;
  return (1 - Math.log(Math.tan(p) + 1 / Math.cos(p)) / Math.PI) / 2;
};
const mercLat = (y: number) => (Math.atan(Math.sinh(Math.PI * (1 - 2 * y))) * 180) / Math.PI;

/**
 * `b` widened about its centre, in Mercator, just enough that fitting it into a `width` x `height` map with `padding`
 * on every side lands at `maxZoom` and no closer. Unchanged when it already fits at or below it — MapLibre fits the
 * tighter axis, so one axis wide enough is enough.
 *
 * FOR THE NATIVE MAP, whose `fitBounds` has no `maxZoom`: with the camera allowed to z18 (`CAMERA_MAX_ZOOM`), a fit to
 * a pond would otherwise fill the screen with 40 m of water and nothing around it. An unmeasured map (0 x 0, before
 * layout) is taken as a 400 x 800 phone rather than as a point, which would fit at every zoom.
 */
export function fitAtMost(b: readonly [number, number, number, number], maxZoom: number,
                          width: number, height: number, padding: number): [number, number, number, number] {
  const w = Math.max(1, (width || 400) - 2 * padding), h = Math.max(1, (height || 800) - 2 * padding);
  const world = 512 * 2 ** maxZoom;
  const needX = w / world, needY = h / world;
  const x0 = (b[0] + 180) / 360, x1 = (b[2] + 180) / 360;
  const y0 = mercY(b[3]), y1 = mercY(b[1]);              // y grows southward
  if (x1 - x0 >= needX || y1 - y0 >= needY) return [b[0], b[1], b[2], b[3]];
  const cx = (x0 + x1) / 2, cy = (y0 + y1) / 2;
  return [(cx - needX / 2) * 360 - 180, mercLat(cy + needY / 2),
          (cx + needX / 2) * 360 - 180, mercLat(cy - needY / 2)];
}
