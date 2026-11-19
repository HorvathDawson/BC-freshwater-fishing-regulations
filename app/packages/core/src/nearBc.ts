/**
 * (T864, owner 2026-11-18: "make sure a tester outside of british columbia doesnt have it glitch out too with location")
 * IS THE DEVICE SOMEWHERE THIS APP CAN HELP? The map, its tiles and the regulations stop at the province (the camera is
 * held to `CAMERA_BOUNDS`), so a fix from Toronto, London or Sydney has nowhere to go: centring on it pinned the camera
 * against the bounds over a blank screen. Every reader of the device's position asks this first; a position that fails
 * it is not used, and the reader is told once (`MY_LOCATION.far`).
 *
 * "Near" is B.C.'s own outline grown by about 50 km (`nearBcOutline.ts`, generated from the same boundary the map's
 * outside mask is cut from), so a border town — Bellingham, Jasper, Watson Lake — works as it did. A position that is
 * not a number, off the globe, or exactly 0,0 ("null island", what a broken GPS can report) is never near.
 */
import type { LatLon } from "./searchGeometry";
import { NEAR_BC_RING } from "./nearBcOutline";

export { NEAR_BC_MARGIN_KM } from "./nearBcOutline";

/** The outline's box `[west, south, east, north]`, for a cheap first answer (most of the world is outside it). */
const BOX = NEAR_BC_RING.reduce<[number, number, number, number]>(
  (b, [x, y]) => [Math.min(b[0], x), Math.min(b[1], y), Math.max(b[2], x), Math.max(b[3], y)],
  [Infinity, Infinity, -Infinity, -Infinity]);

/** True when the position is in British Columbia or within about 50 km of it. */
export function nearBC(p: LatLon | null | undefined): boolean {
  if (!p) return false;
  const { lat, lon } = p;
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || Math.abs(lat) > 90 || Math.abs(lon) > 180) return false;
  if (lat === 0 && lon === 0) return false;
  if (lon < BOX[0] || lon > BOX[2] || lat < BOX[1] || lat > BOX[3]) return false;
  // even-odd ray cast along the latitude: the ring is closed (first point repeated)
  let inside = false;
  for (let i = 0, j = NEAR_BC_RING.length - 1; i < NEAR_BC_RING.length; j = i++) {
    const [xi, yi] = NEAR_BC_RING[i]!, [xj, yj] = NEAR_BC_RING[j]!;
    if ((yi > lat) !== (yj > lat) && lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi) inside = !inside;
  }
  return inside;
}
