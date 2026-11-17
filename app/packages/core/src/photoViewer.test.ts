/**
 * (T836) The photo viewer's gestures as numbers: the strip, the drag down to close, the opening from the thumbnail, and
 * the rubber and glide of zoom / pan. Each guard below fails its test when its line is removed (mutation-checked by
 * hand on 2026-11-17: the zoom guard in photoStripSettle, the off-the-ends guard, the flick-up guard in
 * photoDismissEnds, the past-the-end test in photoStripFollow, the empty-box guard in photoFromThumb, the half-shown
 * test in photoThumbShown, the scale and edge rubber, the fling carry).
 */
import { describe, expect, it } from "vitest";
import { PHOTO_VIEWER, photoDismissEnds, photoDismissFollow, photoDismissLook, photoDragKind,
         photoFromThumb, photoStripFollow, photoStripSettle, photoThumbShown } from "./photoViewer";
import { PHOTO_FIT, PHOTO_GLIDE, PHOTO_ZOOM, clampPhotoView, photoGlide, photoRubber, photoZoomLoose,
         rubberPhotoView } from "./photoZoom";

const W = 400, H = 800;
const S = { w: W, h: H };
const ZOOMED = { scale: 2, x: 0, y: 0 };

describe("what a one-finger drag is", () => {
  it("zoomed it pans, whichever way it goes", () => {
    expect(photoDragKind(true, 50, 0)).toBe("pan");
    expect(photoDragKind(true, 0, 50)).toBe("pan");
  });
  it("at the fit: sideways is the strip, down (or up) is the dismiss", () => {
    expect(photoDragKind(false, -30, 10)).toBe("strip");
    expect(photoDragKind(false, 5, 30)).toBe("dismiss");
    expect(photoDragKind(false, 5, -30)).toBe("dismiss");
  });
  it("at the fit, undecided until it has moved: a first update with no movement is not sideways", () => {
    expect(photoDragKind(false, 0, 0)).toBeNull();
    expect(photoDragKind(false, 2, 4)).toBeNull();
    expect(photoDragKind(false, 2, PHOTO_VIEWER.decideAfter)).toBe("dismiss");
  });
});

describe("the strip follows the finger", () => {
  it("with the finger where there is a photo that way", () => {
    expect(photoStripFollow(-120, 1, 3, W)).toBe(-120);
    expect(photoStripFollow(120, 1, 3, W)).toBe(120);
  });
  it("with resistance past the first and the last", () => {
    expect(photoStripFollow(120, 0, 3, W)).toBeGreaterThan(0);
    expect(photoStripFollow(120, 0, 3, W)).toBeLessThan(120);
    expect(photoStripFollow(-120, 2, 3, W)).toBeLessThan(0);
    expect(photoStripFollow(-120, 2, 3, W)).toBeGreaterThan(-120);
    expect(photoStripFollow(-120, 0, 1, W)).toBeGreaterThan(-120);
  });
});

describe("letting go of the strip", () => {
  it("far enough moves on, short of it settles back", () => {
    const far = W * PHOTO_VIEWER.swipeShare;
    expect(photoStripSettle(PHOTO_FIT, 1, 3, -(far + 1), 0, W)).toBe(1);
    expect(photoStripSettle(PHOTO_FIT, 1, 3, far + 1, 0, W)).toBe(-1);
    expect(photoStripSettle(PHOTO_FIT, 1, 3, -(far - 1), 0, W)).toBe(0);
  });
  it("a flick moves on however short, but only the way it was dragged", () => {
    const flick = PHOTO_VIEWER.swipeFlick + 1;
    expect(photoStripSettle(PHOTO_FIT, 1, 3, -20, -flick, W)).toBe(1);
    expect(photoStripSettle(PHOTO_FIT, 1, 3, 20, flick, W)).toBe(-1);
    expect(photoStripSettle(PHOTO_FIT, 1, 3, 20, -flick, W)).toBe(0);
  });
  it("never off the ends", () => {
    expect(photoStripSettle(PHOTO_FIT, 0, 3, W, 0, W)).toBe(0);
    expect(photoStripSettle(PHOTO_FIT, 2, 3, -W, 0, W)).toBe(0);
  });
  it("never while zoomed (the drag was a pan)", () => {
    expect(photoStripSettle(ZOOMED, 1, 3, -W, -2000, W)).toBe(0);
  });
});

describe("drag down to close", () => {
  it("follows the finger down, gives with resistance up", () => {
    expect(photoDismissFollow(200, H)).toBe(200);
    expect(photoDismissFollow(-200, H)).toBeLessThan(0);
    expect(photoDismissFollow(-200, H)).toBeGreaterThan(-200);
  });
  it("far enough down closes; short of it springs back", () => {
    const far = H * PHOTO_VIEWER.dismissShare;
    expect(photoDismissEnds(far + 1, 0, H)).toBe(true);
    expect(photoDismissEnds(far - 1, 0, H)).toBe(false);
  });
  it("a flick down closes however short", () => {
    expect(photoDismissEnds(20, PHOTO_VIEWER.dismissFlick + 1, H)).toBe(true);
  });
  it("a flick back up keeps it open even past the distance; dragged up never closes", () => {
    expect(photoDismissEnds(H * 0.4, -(PHOTO_VIEWER.dismissFlick + 1), H)).toBe(false);
    expect(photoDismissEnds(-H * 0.4, 0, H)).toBe(false);
    expect(photoDismissEnds(-10, PHOTO_VIEWER.dismissFlick + 1, H)).toBe(false);
  });
  it("the ground fades out by half the height and the photo shrinks with the drag", () => {
    const look = photoDismissLook(H);
    expect(look.fade).toEqual({ inputRange: [0, H * PHOTO_VIEWER.dismissFadeShare], outputRange: [1, 0] });
    expect(look.shrink).toEqual({ inputRange: [0, H], outputRange: [1, PHOTO_VIEWER.dismissShrink] });
  });
});

describe("the photo grows from its thumbnail", () => {
  const stage = { x: 0, y: 0, w: 400, h: 800 };
  it("a landscape photo fitted to the width lands centred on the thumbnail, covering it", () => {
    // 800 x 600 fitted in 400 x 800: 400 x 300 shown. A 100 px square thumbnail at (50, 100).
    const v = photoFromThumb({ x: 50, y: 100, w: 100, h: 100 }, stage, { w: 800, h: 600 })!;
    expect(v.x).toBe(100 - 200);
    expect(v.y).toBe(150 - 400);
    expect(v.scale).toBeCloseTo(100 / 300); // covers: the taller ratio (100/300 > 100/400)
  });
  it("a portrait photo covers by its width", () => {
    const v = photoFromThumb({ x: 0, y: 0, w: 100, h: 100 }, stage, { w: 300, h: 900 })!;
    // 300 x 900 in 400 x 800: fit = min(4/3, 8/9) = 8/9 -> 266.7 x 800 shown
    expect(v.scale).toBeCloseTo(100 / (300 * 8 / 9));
  });
  it("the stage's own offset counts (a modal window not at the origin)", () => {
    const v = photoFromThumb({ x: 150, y: 350, w: 100, h: 100 }, { x: 0, y: 24, w: 400, h: 800 }, { w: 400, h: 400 })!;
    expect(v.x).toBe(0);
    expect(v.y).toBe(400 - 424);
  });
  it("no box, no growing: an empty thumbnail, stage or photo size fades instead", () => {
    expect(photoFromThumb({ x: 0, y: 0, w: 0, h: 100 }, stage, { w: 10, h: 10 })).toBeNull();
    expect(photoFromThumb({ x: 0, y: 0, w: 100, h: 100 }, { ...stage, w: 0 }, { w: 10, h: 10 })).toBeNull();
    expect(photoFromThumb({ x: 0, y: 0, w: 100, h: 100 }, stage, { w: 0, h: 10 })).toBeNull();
  });
  it("shrinks back only into a thumbnail at least half on screen", () => {
    expect(photoThumbShown({ x: 10, y: 10, w: 100, h: 100 }, stage)).toBe(true);
    expect(photoThumbShown({ x: 10, y: -40, w: 100, h: 100 }, stage)).toBe(true);
    expect(photoThumbShown({ x: 10, y: -60, w: 100, h: 100 }, stage)).toBe(false);
    expect(photoThumbShown({ x: 10, y: 900, w: 100, h: 100 }, stage)).toBe(false);
    expect(photoThumbShown({ x: 10, y: 10, w: 0, h: 0 }, stage)).toBe(false);
  });
});

describe("zoom and pan: rubber past the limits, glide on a fling", () => {
  it("the rubber gives less the further it goes and never reaches the box", () => {
    expect(photoRubber(0, 400)).toBe(0);
    expect(photoRubber(100, 400)).toBeLessThan(100);
    expect(photoRubber(10_000, 400)).toBeLessThan(400);
    expect(photoRubber(200, 400) - photoRubber(100, 400)).toBeLessThan(photoRubber(100, 400));
  });
  it("a pinch past the most zoom is drawn a little further, not all of it", () => {
    const v = rubberPhotoView({ scale: 8, x: 0, y: 0 }, S);
    expect(v.scale).toBeGreaterThan(PHOTO_ZOOM.max);
    expect(v.scale).toBeLessThan(8);
  });
  it("a pinch below the fit is drawn a little smaller", () => {
    const v = rubberPhotoView({ scale: 0.6, x: 0, y: 0 }, S);
    expect(v.scale).toBeLessThan(1);
    expect(v.scale).toBeGreaterThan(0.6);
  });
  it("a drag past the photo's edge is drawn past it with resistance; inside, as it is", () => {
    const m = ((2 - 1) * W) / 2; // 200
    expect(rubberPhotoView({ scale: 2, x: 150, y: 0 }, S).x).toBe(150);
    const past = rubberPhotoView({ scale: 2, x: m + 100, y: 0 }, S).x;
    expect(past).toBeGreaterThan(m);
    expect(past).toBeLessThan(m + 100);
    expect(rubberPhotoView({ scale: 2, x: -(m + 100), y: 0 }, S).x).toBeCloseTo(-past);
  });
  it("the loose pinch keeps the point under the fingers, past the limits too", () => {
    const at = { x: 300, y: 600 };
    const v = photoZoomLoose(PHOTO_FIT, 8, at, S);
    expect(v.scale).toBe(8);
    expect((at.x - 200 - v.x) / v.scale).toBeCloseTo(100);
    expect(photoZoomLoose(PHOTO_FIT, 100, at, S).scale).toBe(PHOTO_GLIDE.looseMax);
  });
  it("let go: a slow release settles inside the limits", () => {
    const r = photoGlide({ scale: 2, x: 300, y: 0 }, 0, 0, S);
    expect(r.how).toBe("settle");
    expect(r.view).toEqual(clampPhotoView({ scale: 2, x: 300, y: 0 }, S));
  });
  it("let go: a fling carries the picture on, still held by the edges", () => {
    const r = photoGlide({ scale: 3, x: 0, y: 0 }, 400, 0, S);
    expect(r.how).toBe("glide");
    expect(r.view.x).toBeCloseTo(400 * PHOTO_GLIDE.carryS);
    expect(photoGlide({ scale: 3, x: 0, y: 0 }, 100_000, 0, S).view.x).toBe(((3 - 1) * W) / 2);
  });
  it("let go past the most zoom: back to it about the centre", () => {
    const r = photoGlide({ scale: 10, x: 100, y: 0 }, 0, 0, S);
    expect(r.view.scale).toBe(PHOTO_ZOOM.max);
    expect(r.view.x).toBeCloseTo(50);
  });
  it("let go below the fit: back to the fit", () => {
    expect(photoGlide({ scale: 0.6, x: 30, y: 30 }, 0, 0, S).view).toEqual(PHOTO_FIT);
  });
});
