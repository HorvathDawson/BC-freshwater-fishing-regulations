/**
 * (T454) The photo viewer's zoom state: the view (`@app/core` photoZoom) for one photo in one box. It resets by itself
 * when the photo or the box changes (a step to the next photo, a rotation). The gestures call the actions below; none
 * does arithmetic of its own.
 *
 * (T836) THE VIEW IS NOT REACT STATE: every change is handed to `show` with how to get there — "follow" (put it there
 * now: a finger is on it), "settle" (a short ease back inside the limits) or "glide" (a fling's longer run to rest) —
 * and the viewer hands that to the platform's animation driver. Nothing re-renders per finger movement. While fingers
 * are down the view runs free (`photoZoomLoose`) and is shown rubbered past the limits (`rubberPhotoView`); let go, it
 * settles or glides inside them (`photoGlide`).
 */
import { useCallback, useEffect, useMemo, useRef } from "react";
import { PHOTO_FIT, isPhotoZoomed, photoGlide, photoZoomAt, photoZoomLoose, rubberPhotoView, togglePhotoZoom,
         type PhotoBox, type PhotoPoint, type PhotoView } from "@app/core";

export type PhotoMotion = "follow" | "settle" | "glide";

export interface PhotoZoom {
  /** the settled view (not the one drawn under a finger), read at event time */
  readonly view: PhotoView;
  /** zoomed in at all (the settled view), read at event time */
  readonly zoomed: boolean;
  /** a pinch or a pan begins (several may overlap: the view is freed once and settled when the last ends) */
  begin(): void;
  /** fingers down: scale by `factor` about `at` (box coordinates), drawn rubbered past the limits */
  pinch(factor: number, at: PhotoPoint): void;
  /** fingers down: slide by (dx, dy), drawn rubbered past the edges */
  drag(dx: number, dy: number): void;
  /** a pinch or pan ended, with the fling's velocity (px/s): the last to end settles or glides */
  release(vx: number, vy: number): void;
  /** a wheel notch: scale by `factor` about `at`, held inside the limits at once */
  zoomAt(factor: number, at: PhotoPoint): void;
  /** a double-tap */
  toggle(at: PhotoPoint): void;
}

export function usePhotoZoom(photoKey: string | number, size: PhotoBox,
                             show: (v: PhotoView, how: PhotoMotion) => void): PhotoZoom {
  const key = `${photoKey}:${size.w}x${size.h}`;
  // the latest, read at event time: a gesture's callbacks are built once, not per render
  const live = useRef({ size, show, settled: PHOTO_FIT as PhotoView, loose: PHOTO_FIT as PhotoView, held: 0 });
  live.current.size = size;
  live.current.show = show;
  const settle = useCallback((v: PhotoView, how: PhotoMotion) => {
    const l = live.current;
    l.settled = v; l.loose = v;
    l.show(v, how);
  }, []);
  // a new photo or box: back to the fit, at once
  useEffect(() => { live.current.held = 0; settle(PHOTO_FIT, "follow"); }, [key, settle]);
  return useMemo((): PhotoZoom => ({
    get view() { return live.current.settled; },
    get zoomed() { return isPhotoZoomed(live.current.settled); },
    begin: () => { const l = live.current; if (l.held++ === 0) l.loose = l.settled; },
    pinch: (factor, at) => {
      const l = live.current;
      l.loose = photoZoomLoose(l.loose, factor, at, l.size);
      l.show(rubberPhotoView(l.loose, l.size), "follow");
    },
    drag: (dx, dy) => {
      const l = live.current;
      l.loose = { ...l.loose, x: l.loose.x + dx, y: l.loose.y + dy };
      l.show(rubberPhotoView(l.loose, l.size), "follow");
    },
    release: (vx, vy) => {
      const l = live.current;
      l.held = Math.max(0, l.held - 1);
      if (l.held > 0) return;
      const g = photoGlide(l.loose, vx, vy, l.size);
      settle(g.view, g.how);
    },
    zoomAt: (factor, at) => settle(photoZoomAt(live.current.settled, factor, at, live.current.size), "follow"),
    toggle: (at) => settle(togglePhotoZoom(live.current.settled, at, live.current.size), "settle"),
  }), [settle]);
}
