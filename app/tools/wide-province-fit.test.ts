/**
 * T1208 (owner 2026-11-26: "Leave phones; fit all on wide screens (Recommended)"). ON A WIDE SCREEN, ZOOMED ALL THE WAY OUT,
 * THE WHOLE PROVINCE FITS: Haida Gwaii, the north-west corner at 60 N, the Rockies' south-east corner and Victoria, all in
 * the map's clear area (under the top bar, left of the map rail, above the key card). Phones are left as they are: at z4,
 * the atlas's first zoom, a phone shows 18 degrees of longitude at 412 wide and B.C. is 25 (the T1208 ticket).
 *
 * How far out a camera can go is MapLibre's rule: at least `MIN_ZOOM`, and at least the zoom at which the window is no
 * wider (or taller) than `CAMERA_BOUNDS` (maxBounds). At 1280 x 800 that is z4.456, the zoom the web app reported there.
 * The chrome's edges were measured in the web app at 1280 x 800 and 1440 x 900 (the top bar's bottom 103 px, the map
 * rail 56 px from the right, the key card 42 px tall at the bottom); the measured picture is the T1208 lane's
 * 12-wide-1280.png. If the bounds, the first zoom or that chrome move, this says whether the province still fits.
 */
import { describe, expect, it } from "vitest";
import { CAMERA_BOUNDS, MIN_ZOOM } from "@app/core";

const TILE = 512;
const merc = (lat: number) => Math.log(Math.tan(Math.PI / 4 + (lat * Math.PI) / 360));
/** the widest a w x h map can show: MapLibre keeps the whole window inside maxBounds */
const widestZoom = (w: number, h: number) => {
  const [[west, south], [east, north]] = CAMERA_BOUNDS;
  const byLon = Math.log2((w * 360) / (TILE * (east - west)));
  const byLat = Math.log2((h * 2 * Math.PI) / (TILE * (merc(north) - merc(south))));
  return Math.max(MIN_ZOOM, byLon, byLat);
};
/** B.C. whole: Haida Gwaii's west coast is inside the NW corner's longitude; Victoria's south tip; the 60th parallel */
const BC = { west: -139.06, east: -114.05, south: 48.3, north: 60.0 };
const CHROME = { top: 103, right: 56, bottom: 42 };

describe("(T1208) the whole province on a wide screen, zoomed all the way out", () => {
  for (const [w, h] of [[1280, 800], [1440, 900]] as const)
    it(`${w} x ${h}: it fits the map's clear area`, () => {
      const z = widestZoom(w, h), world = TILE * 2 ** z;
      const across = ((BC.east - BC.west) / 360) * world;
      const down = ((merc(BC.north) - merc(BC.south)) / (2 * Math.PI)) * world;
      expect(across, `B.C. ${across.toFixed(0)} px across at z${z.toFixed(2)}`).toBeLessThanOrEqual(w - CHROME.right);
      expect(down, `B.C. ${down.toFixed(0)} px down at z${z.toFixed(2)}`).toBeLessThanOrEqual(h - CHROME.top - CHROME.bottom);
    });

  it("phones: z4 cannot hold it (the owner left phones as they are), so nothing here claims it", () => {
    const world = TILE * 2 ** widestZoom(412, 915);
    expect(((BC.east - BC.west) / 360) * world).toBeGreaterThan(412);
  });
});
