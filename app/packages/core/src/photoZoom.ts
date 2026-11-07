/**
 * (T454) A PHOTOGRAPH'S ZOOM AND PAN, DECIDED. Pure arithmetic on a view of one picture drawn to fill a box of `size`
 * (the picture is fitted, so scale 1 is the whole picture), so the pinch, the wheel, the double-tap and the drag cannot
 * disagree about what a zoom is.
 *
 *   view     scale (1 = fitted) and the offset (px) of the picture's centre from the box's centre
 *   photoZoomAt scale by a factor about a point in the box: the picture point under that point stays put
 *   photoPanBy slide the picture, its edges stopping at the box's edges
 *   togglePhotoZoom a double-tap: back to fit when zoomed, else in to `PHOTO_ZOOM.tap` at the point
 *   swipes   a sideways swipe means previous / next only at the fit
 */

export interface PhotoView { readonly scale: number; readonly x: number; readonly y: number }
export interface PhotoBox { readonly w: number; readonly h: number }
export interface PhotoPoint { readonly x: number; readonly y: number }

export const PHOTO_ZOOM = {
  min: 1,
  max: 5,
  /** a double-tap zooms in to this */
  tap: 2.5,
  /** one notch of a mouse wheel (deltaY of 100 px) */
  wheelPer100: 1.25,
  /** a scale this close to 1 IS the fit (a rounding error is not "zoomed") */
  fitSlack: 0.01,
} as const;

export const PHOTO_FIT: PhotoView = { scale: 1, x: 0, y: 0 };

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));

/** The view held inside the box: scale in range, no empty margin showing beyond the picture's edges. */
export function clampPhotoView(v: PhotoView, size: PhotoBox): PhotoView {
  const scale = clamp(v.scale, PHOTO_ZOOM.min, PHOTO_ZOOM.max);
  if (scale - 1 < PHOTO_ZOOM.fitSlack) return PHOTO_FIT;
  const mx = ((scale - 1) * size.w) / 2, my = ((scale - 1) * size.h) / 2;
  return { scale, x: clamp(v.x, -mx, mx), y: clamp(v.y, -my, my) };
}

/** Scale by `factor` about `at` (box coordinates, origin top-left): the picture point under `at` stays under it. */
export function photoZoomAt(v: PhotoView, factor: number, at: PhotoPoint, size: PhotoBox): PhotoView {
  const scale = clamp(v.scale * factor, PHOTO_ZOOM.min, PHOTO_ZOOM.max);
  const k = scale / v.scale;
  const fx = at.x - size.w / 2, fy = at.y - size.h / 2;
  return clampPhotoView({ scale, x: fx - (fx - v.x) * k, y: fy - (fy - v.y) * k }, size);
}

/** Slide by (dx, dy) px, stopping at the edges. Nothing moves at the fit. */
export function photoPanBy(v: PhotoView, dx: number, dy: number, size: PhotoBox): PhotoView {
  return clampPhotoView({ scale: v.scale, x: v.x + dx, y: v.y + dy }, size);
}

/** Zoomed in at all? */
export function isPhotoZoomed(v: PhotoView): boolean {
  return v.scale - 1 >= PHOTO_ZOOM.fitSlack;
}

/** A double-tap: the fit when zoomed, else in to `PHOTO_ZOOM.tap` about the tapped point. */
export function togglePhotoZoom(v: PhotoView, at: PhotoPoint, size: PhotoBox): PhotoView {
  return isPhotoZoomed(v) ? PHOTO_FIT : photoZoomAt(v, PHOTO_ZOOM.tap, at, size);
}

/** The factor one wheel event zooms by (up scrolls in); trackpad pinch arrives as small ctrl+wheel deltas. */
export function photoWheelFactor(deltaY: number): number {
  return Math.pow(PHOTO_ZOOM.wheelPer100, -deltaY / 100);
}

/** Whether a drag of `dx` px is a swipe to the previous (-1) / next (1) photo: only at the fit. */
export function photoSwipe(v: PhotoView, dx: number, threshold: number): -1 | 0 | 1 {
  if (isPhotoZoomed(v)) return 0;
  return dx > threshold ? -1 : dx < -threshold ? 1 : 0;
}
