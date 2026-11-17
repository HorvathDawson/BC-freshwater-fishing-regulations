/**
 * (T454) A PHOTOGRAPH'S ZOOM AND PAN, DECIDED. Pure arithmetic on a view of one picture drawn to fill a box of `size`
 * (the picture is fitted, so scale 1 is the whole picture), so the pinch, the wheel, the double-tap and the drag cannot
 * disagree about what a zoom is.
 *
 *   view     scale (1 = fitted) and the offset (px) of the picture's centre from the box's centre
 *   photoZoomAt scale by a factor about a point in the box: the picture point under that point stays put
 *   photoPanBy slide the picture, its edges stopping at the box's edges
 *   togglePhotoZoom a double-tap: back to fit when zoomed, else in to `PHOTO_ZOOM.tap` at the point
 *   (T836) photoZoomLoose / rubberPhotoView / photoGlide: during a gesture the view runs free and is DRAWN with
 *   resistance past the limits (`rubberPhotoView`); let go, it settles inside them, carried on by the fling
 *   (`photoGlide`). The swipe between photos is `photoViewer.ts`.
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

/**
 * (T836) HOW THE PHOTO MOVES WHEN IT IS LET GO. Pure numbers for the viewer's animations (the platform's native driver
 * runs them; nothing here is a frame).
 */
export const PHOTO_GLIDE = {
  /** a settle back inside the limits (a double-tap, a let-go past an edge) */
  settleMs: 220,
  /** a fling's glide to rest */
  glideMs: 420,
  /** how far a fling carries: its speed (px/s) times this many seconds */
  carryS: 0.25,
  /** a let-go faster than this (px/s) glides; slower settles */
  flick: 250,
  /** how stiff the rubber past a limit is (0.55 is the platform's own feel) */
  rubber: 0.55,
  /** the scale range a pinch may run in while the fingers are down (it is drawn rubbered, and settles to PHOTO_ZOOM) */
  looseMin: 0.5,
  looseMax: 10,
} as const;

/**
 * Past a limit by `over` px in a box `dim` px long: how far it is DRAWN past it. Gives less the further it goes and never
 * reaches `dim` (the platforms' rubber band).
 */
export function photoRubber(over: number, dim: number): number {
  if (over <= 0 || dim <= 0) return 0;
  const c = PHOTO_GLIDE.rubber;
  return (over * dim * c) / (dim + c * over);
}

/** (T836) A pinch while the fingers are down: `photoZoomAt` with no limits but the loose scale range. */
export function photoZoomLoose(v: PhotoView, factor: number, at: PhotoPoint, size: PhotoBox): PhotoView {
  const scale = clamp(v.scale * factor, PHOTO_GLIDE.looseMin, PHOTO_GLIDE.looseMax);
  const k = scale / v.scale;
  const fx = at.x - size.w / 2, fy = at.y - size.h / 2;
  return { scale, x: fx - (fx - v.x) * k, y: fy - (fy - v.y) * k };
}

/** (T836) A free view as it is drawn: inside the limits as it is, past them with resistance (`photoRubber`). */
export function rubberPhotoView(v: PhotoView, size: PhotoBox): PhotoView {
  const { min, max } = PHOTO_ZOOM;
  const scale = v.scale > max ? max + photoRubber(v.scale - max, max)
    : v.scale < min ? min - photoRubber(min - v.scale, min) : v.scale;
  const edge = (p: number, dim: number) => {
    const m = Math.max(0, ((scale - 1) * dim) / 2), a = Math.abs(p);
    return a <= m ? p : Math.sign(p) * (m + photoRubber(a - m, dim));
  };
  return { scale, x: edge(v.x, size.w), y: edge(v.y, size.h) };
}

/**
 * (T836) LET GO: where a free view comes to rest, and how. A fling (velocity px/s) carries the picture on by
 * `PHOTO_GLIDE.carryS` of its speed; a scale past the limits comes back about the box's centre; the rest is held inside
 * the edges (`clampPhotoView`). "glide" when a fling carried it, else "settle".
 */
export function photoGlide(v: PhotoView, vx: number, vy: number, size: PhotoBox): { view: PhotoView; how: "glide" | "settle" } {
  const scale = clamp(v.scale, PHOTO_ZOOM.min, PHOTO_ZOOM.max), k = scale / v.scale;
  const fling = Math.hypot(vx, vy) > PHOTO_GLIDE.flick;
  const carry = fling ? PHOTO_GLIDE.carryS : 0;
  const view = clampPhotoView({ scale, x: v.x * k + vx * carry, y: v.y * k + vy * carry }, size);
  return { view, how: fling ? "glide" : "settle" };
}
