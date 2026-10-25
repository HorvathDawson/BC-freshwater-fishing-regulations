/**
 * OVERZOOM (owner 2026-10-21): the camera goes past the tiles, the tiles do not.
 *
 * "We should allow over zooming (more than the pmtile) where it just keeps the resolution of the lower zoom." The
 * camera's ceiling and each source's deepest zoom are DIFFERENT numbers for different questions, and the one mistake
 * this pins is collapsing them: a source asked for its tiles at the camera's zoom fetches tiles that do not exist and
 * draws nothing, which looks exactly like water with no rules on it.
 */
import { describe, expect, it } from "vitest";
import {
  ATLAS_MAX_ZOOM, BASEMAP_MAX_ZOOM, CAMERA_MAX_ZOOM, FIT_MAX_ZOOM, MAGNITUDE_LADDER, MIN_ZOOM, fitAtMost,
  zoomForMagnitude,
} from "./ladder";
import * as core from "./index";

/** The zoom MapLibre would fit `b` at in a `w` x `h` map with `pad` on every side (512 px world tiles). */
function fitZoom(b: readonly [number, number, number, number], w: number, h: number, pad: number): number {
  const toY = (lat: number) => {
    const p = (lat * Math.PI) / 180;
    return (1 - Math.log(Math.tan(p) + 1 / Math.cos(p)) / Math.PI) / 2;
  };
  const sx = (b[2] - b[0]) / 360, sy = toY(b[1]) - toY(b[3]);
  return Math.min(Math.log2((w - 2 * pad) / (512 * sx)), Math.log2((h - 2 * pad) / (512 * sy)));
}

describe("the zoom ceilings", () => {
  it("the camera goes four zooms past the tiles; the sources stop where their archives stop", () => {
    expect({ min: MIN_ZOOM, atlas: ATLAS_MAX_ZOOM, basemap: BASEMAP_MAX_ZOOM, camera: CAMERA_MAX_ZOOM,
             fit: FIT_MAX_ZOOM })
      .toEqual({ min: 4, atlas: 13, basemap: 14, camera: 18, fit: 14 });
  });

  it("no source's deepest zoom is the camera's — a source raised to it fetches tiles that do not exist", () => {
    expect([ATLAS_MAX_ZOOM, BASEMAP_MAX_ZOOM].map((z) => z < CAMERA_MAX_ZOOM)).toEqual([true, true]);
  });

  it("the ladder's bottom rung is the atlas's deepest zoom, not the camera's", () => {
    // every reach exists at z13 (T85); a gauge on an unnumbered creek appears with it, not five zooms later
    expect([MAGNITUDE_LADDER[MAGNITUDE_LADDER.length - 1], zoomForMagnitude(0), zoomForMagnitude(null)])
      .toEqual([[0, 13], 13, 13]);
  });

  it("there is no bare MAX_ZOOM to reach for: every caller names which ceiling it means", () => {
    expect("MAX_ZOOM" in core).toBe(false);
  });
});

describe("fitAtMost: a fit frames the subject in its country, never past the tiles", () => {
  const W = 400, H = 800, PAD = 48;

  it("a pond 40 m across would fit near z19; widened, it fits at exactly the fit ceiling", () => {
    const pond: [number, number, number, number] = [-121.9502, 49.1999, -121.9498, 49.2001];
    expect(fitZoom(pond, W, H, PAD)).toBeGreaterThan(FIT_MAX_ZOOM + 3);
    const b = fitAtMost(pond, FIT_MAX_ZOOM, W, H, PAD);
    expect(Math.round(fitZoom(b, W, H, PAD) * 1e6) / 1e6).toBe(FIT_MAX_ZOOM);
    // about the same centre: widened symmetrically in Mercator, not slid
    expect([(b[0] + b[2]) / 2, (b[1] + b[3]) / 2].map((v) => Math.round(v * 1e4) / 1e4))
      .toEqual([-121.95, 49.2]);
  });

  it("a box that already fits at or below the ceiling is returned as it is", () => {
    const valley: [number, number, number, number] = [-122.3, 49.0, -121.6, 49.4];
    expect(fitAtMost(valley, FIT_MAX_ZOOM, W, H, PAD)).toEqual(valley);
  });

  it("a hair-wide creek that is tall enough is left alone — the fit is the larger axis's", () => {
    // MapLibre fits the tighter axis, so one axis at or below the ceiling already holds the camera there
    const creek: [number, number, number, number] = [-121.95, 49.1, -121.9499, 49.3];
    expect(fitAtMost(creek, FIT_MAX_ZOOM, W, H, PAD)).toEqual(creek);
  });

  it("an unmeasured map (0 x 0) is treated as a phone, never as a point that fits at every zoom", () => {
    const pond: [number, number, number, number] = [-121.9502, 49.1999, -121.9498, 49.2001];
    const b = fitAtMost(pond, FIT_MAX_ZOOM, 0, 0, 8);
    expect(fitZoom(b, 400, 800, 8)).toBeLessThanOrEqual(FIT_MAX_ZOOM + 1e-9);
  });
});
