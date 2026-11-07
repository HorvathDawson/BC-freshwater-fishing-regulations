import { describe, expect, it } from "vitest";
import { PHOTO_FIT, PHOTO_ZOOM, clampPhotoView, isPhotoZoomed, photoPanBy, photoSwipe, photoWheelFactor, photoZoomAt, togglePhotoZoom } from "./photoZoom";

const S = { w: 400, h: 600 };

describe("photo zoom", () => {
  it("zooms about the point: the picture point under it stays put", () => {
    const at = { x: 300, y: 450 };
    const v = photoZoomAt(PHOTO_FIT, 2, at, S);
    // picture point under `at` before: (at - centre); after: (at - centre - t) / scale
    const before = { x: at.x - 200, y: at.y - 300 };
    expect((at.x - 200 - v.x) / v.scale).toBeCloseTo(before.x);
    expect((at.y - 300 - v.y) / v.scale).toBeCloseTo(before.y);
  });
  it("zooming about the centre does not move the picture", () => {
    expect(photoZoomAt(PHOTO_FIT, 3, { x: 200, y: 300 }, S)).toEqual({ scale: 3, x: 0, y: 0 });
  });
  it("holds the scale to 1..5", () => {
    expect(photoZoomAt(PHOTO_FIT, 100, { x: 200, y: 300 }, S).scale).toBe(PHOTO_ZOOM.max);
    expect(photoZoomAt({ scale: 2, x: 10, y: 10 }, 0.01, { x: 5, y: 5 }, S)).toEqual(PHOTO_FIT);
  });
  it("stops the edges at the box's edges", () => {
    const v = photoZoomAt(PHOTO_FIT, 2, { x: 200, y: 300 }, S);
    expect(photoPanBy(v, 9999, -9999, S)).toEqual({ scale: 2, x: 200, y: -300 });
    expect(clampPhotoView({ scale: 3, x: 1e6, y: 0 }, S).x).toBe(400);
  });
  it("a corner tap cannot leave an empty margin", () => {
    const v = photoZoomAt(PHOTO_FIT, 5, { x: 0, y: 0 }, S);
    expect(v.x).toBe(800);
    expect(v.y).toBe(1200);
  });
  it("nothing pans at the fit", () => {
    expect(photoPanBy(PHOTO_FIT, 50, 50, S)).toEqual(PHOTO_FIT);
  });
  it("double-tap zooms in at the point, and back to fit", () => {
    const v = togglePhotoZoom(PHOTO_FIT, { x: 300, y: 300 }, S);
    expect(v.scale).toBe(PHOTO_ZOOM.tap);
    expect(isPhotoZoomed(v)).toBe(true);
    expect(togglePhotoZoom(v, { x: 1, y: 1 }, S)).toEqual(PHOTO_FIT);
  });
  it("wheel up zooms in, down zooms out", () => {
    expect(photoWheelFactor(-100)).toBeGreaterThan(1);
    expect(photoWheelFactor(100)).toBeLessThan(1);
  });
  it("a swipe changes photo at the fit but not while zoomed", () => {
    expect(photoSwipe(PHOTO_FIT, -80, 50)).toBe(1);
    expect(photoSwipe(PHOTO_FIT, 80, 50)).toBe(-1);
    expect(photoSwipe(PHOTO_FIT, 20, 50)).toBe(0);
    expect(photoSwipe({ scale: 2, x: 0, y: 0 }, -80, 50)).toBe(0);
  });
});
