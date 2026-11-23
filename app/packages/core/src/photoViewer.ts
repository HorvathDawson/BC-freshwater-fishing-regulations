/**
 * (T836) THE PHOTO VIEWER'S GESTURES, DECIDED — the standard phone photo viewer (Apple / Google Photos), as pure numbers
 * so the recognisers only measure and every threshold is pinned by a test:
 *
 *   photoDragKind      what a one-finger drag is: a pan (zoomed), the strip (sideways at the fit), the dismiss (down)
 *   photoStripFollow   the strip of photos follows the finger; past the first / last it gives with resistance
 *   photoStripSettle   let go: on to the next / previous (far enough or a flick), else back
 *   photoDismissFollow / photoDismissEnds / PHOTO_DISMISS_LOOK   drag down to close: follows, closes or springs back
 *   photoFromThumb / photoThumbShown   the photo grows from its thumbnail and shrinks back into it
 *
 * Zoom and pan are `photoZoom.ts`. Box coordinates are px; velocities px/s (the gesture handler's).
 */
import { isPhotoZoomed, photoRubber, type PhotoBox, type PhotoView } from "./photoZoom";

export const PHOTO_VIEWER = {
  /** a drag at the fit is read as the strip or the dismiss once it has moved this far (px); a finger that has moved
   *  this far is no longer a tap (the viewer's tap and double tap `maxDistance`) */
  decideAfter: 6,
  /** the space between two photos on the strip (px) */
  gap: 16,
  /** a drag this share of the width moves on to the next photo… */
  swipeShare: 0.3,
  /** …or a flick faster than this (px/s) in that direction */
  swipeFlick: 400,
  /** a drag down this share of the height closes the viewer… */
  dismissShare: 0.15,
  /** …or a flick down faster than this (px/s); a flick back up this fast keeps it open */
  dismissFlick: 600,
  /** the ground has faded out when the photo is dragged down this share of the height */
  dismissFadeShare: 0.5,
  /** and the photo has shrunk to this when dragged down the whole height */
  dismissShrink: 0.6,
  /** the strip settling on a photo / springing back, the dismiss springing back (ms) */
  settleMs: 260,
  /** the photo growing from its thumbnail / shrinking back into it (ms) */
  openMs: 300,
  closeMs: 250,
  /** a fade in or out: Reduce Motion, or no thumbnail to go to (ms) */
  fadeMs: 200,
  /** the buttons hiding / coming back on a tap (ms) */
  chromeMs: 160,
  /** how long the opening waits to learn where the thumbnail is and the photo's size before it fades instead (ms) */
  measureMs: 150,
} as const;

/** A box in window coordinates (px). */
export interface PhotoRect { readonly x: number; readonly y: number; readonly w: number; readonly h: number }

export type PhotoDragKind = "pan" | "strip" | "dismiss";

/**
 * What a one-finger drag is, from its first movement: zoomed it pans the photo; at the fit sideways is the strip, else
 * the dismiss. Null until it has moved `PHOTO_VIEWER.decideAfter` px either way: the recogniser's first update can carry
 * no movement at all (a browser's did), and 0 vs 0 read as sideways sent a drag down along the strip.
 */
export function photoDragKind(zoomed: boolean, dx: number, dy: number): PhotoDragKind | null {
  if (zoomed) return "pan";
  if (Math.max(Math.abs(dx), Math.abs(dy)) < PHOTO_VIEWER.decideAfter) return null;
  return Math.abs(dx) >= Math.abs(dy) ? "strip" : "dismiss";
}

/** Where the strip is drawn while a drag of `dx` is on it: with the finger, but with resistance past the first / last photo. */
export function photoStripFollow(dx: number, index: number, n: number, width: number): number {
  const pastEnd = (dx > 0 && index <= 0) || (dx < 0 && index >= n - 1);
  return pastEnd ? Math.sign(dx) * photoRubber(Math.abs(dx), width) : dx;
}

/** Let go of the strip: 1 on to the next, -1 back to the previous, 0 settle where it was (never while zoomed, never off the ends). */
export function photoStripSettle(v: PhotoView, index: number, n: number, dx: number, vx: number, width: number): -1 | 0 | 1 {
  if (isPhotoZoomed(v)) return 0;
  const far = width * PHOTO_VIEWER.swipeShare, flick = PHOTO_VIEWER.swipeFlick;
  const d = dx < -far || (dx < 0 && vx < -flick) ? 1 : dx > far || (dx > 0 && vx > flick) ? -1 : 0;
  return index + d < 0 || index + d >= n ? 0 : d;
}

/** Where the photo is drawn while dragged down by `dy`: with the finger down, with resistance up. */
export function photoDismissFollow(dy: number, height: number): number {
  return dy >= 0 ? dy : -photoRubber(-dy, height);
}

/** Let go of a drag down: true closes the viewer (far enough, or a flick down), false springs it back. */
export function photoDismissEnds(dy: number, vy: number, height: number): boolean {
  if (dy <= 0 || vy < -PHOTO_VIEWER.dismissFlick) return false;
  return dy > height * PHOTO_VIEWER.dismissShare || vy > PHOTO_VIEWER.dismissFlick;
}

/** The dismiss as drawn, for the platform's interpolation: the ground fades (the buttons faster, T970) and the photo shrinks with the drag down. */
export function photoDismissLook(height: number): { fade: { inputRange: number[]; outputRange: number[] };
                                                     chrome: { inputRange: number[]; outputRange: number[] };
                                                     shrink: { inputRange: number[]; outputRange: number[] } } {
  const h = Math.max(1, height);
  return {
    fade: { inputRange: [0, h * PHOTO_VIEWER.dismissFadeShare], outputRange: [1, 0] },
    // (T970) the buttons and the counter are gone by the time the ground is half gone (they sit over the page showing through)
    chrome: { inputRange: [0, h * PHOTO_VIEWER.dismissFadeShare / 2], outputRange: [1, 0] },
    shrink: { inputRange: [0, h], outputRange: [1, PHOTO_VIEWER.dismissShrink] },
  };
}

/**
 * The photo laid over its thumbnail: the view (offset of the centre from the stage's centre, and scale) that draws the
 * fitted photo (`image` px, contained in `stage`) over `thumb`, covering it as the thumbnail does. Null when any box is
 * empty: the viewer fades instead.
 */
export function photoFromThumb(thumb: PhotoRect, stage: PhotoRect, image: PhotoBox): PhotoView | null {
  if (thumb.w <= 0 || thumb.h <= 0 || stage.w <= 0 || stage.h <= 0 || image.w <= 0 || image.h <= 0) return null;
  const fit = Math.min(stage.w / image.w, stage.h / image.h);
  const shownW = image.w * fit, shownH = image.h * fit;
  return {
    scale: Math.max(thumb.w / shownW, thumb.h / shownH),
    x: thumb.x + thumb.w / 2 - (stage.x + stage.w / 2),
    y: thumb.y + thumb.h / 2 - (stage.y + stage.h / 2),
  };
}

/** Is the thumbnail on screen to shrink back into — at least half of it inside the stage? Else the viewer fades out. */
export function photoThumbShown(thumb: PhotoRect, stage: PhotoRect): boolean {
  if (thumb.w <= 0 || thumb.h <= 0) return false;
  const w = Math.min(thumb.x + thumb.w, stage.x + stage.w) - Math.max(thumb.x, stage.x);
  const h = Math.min(thumb.y + thumb.h, stage.y + stage.h) - Math.max(thumb.y, stage.y);
  return w > 0 && h > 0 && w * h >= 0.5 * thumb.w * thumb.h;
}
