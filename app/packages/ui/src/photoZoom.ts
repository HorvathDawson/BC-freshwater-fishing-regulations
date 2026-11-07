/**
 * (T454) The photo viewer's zoom state: the view (`@app/core` photoZoom) for one photo in one box. It resets by itself
 * when the photo or the box changes (a step to the next photo, a rotation) — the view is stored with the key it was
 * made for and read as the fit under any other. The gestures call the actions below; none does arithmetic of its own.
 */
import { useCallback, useMemo, useRef, useState } from "react";
import { PHOTO_FIT, isPhotoZoomed, photoPanBy, photoSwipe, photoZoomAt, togglePhotoZoom, type PhotoBox, type PhotoView, type PhotoPoint } from "@app/core";

export interface PhotoZoom {
  readonly view: PhotoView;
  readonly zoomed: boolean;
  /** scale by `factor` about `at` (box coordinates) */
  zoomAt(factor: number, at: PhotoPoint): void;
  panBy(dx: number, dy: number): void;
  /** a double-tap */
  toggle(at: PhotoPoint): void;
  reset(): void;
  /** a drag of `dx` px ended: -1 previous, 1 next, 0 none (never while zoomed) */
  swipe(dx: number, threshold: number): -1 | 0 | 1;
}

export function usePhotoZoom(photoKey: string | number, size: PhotoBox): PhotoZoom {
  const key = `${photoKey}:${size.w}x${size.h}`;
  const [held, setHeld] = useState<{ key: string; view: PhotoView }>({ key, view: PHOTO_FIT });
  const view = held.key === key ? held.view : PHOTO_FIT;
  // the latest, read at event time: a gesture's callbacks are built once, not per render
  const live = useRef({ key, view, size });
  live.current = { key, view, size };
  const set = useCallback((f: (v: PhotoView, s: PhotoBox) => PhotoView) => {
    const l = live.current;
    const next = f(l.view, l.size);
    live.current = { ...l, view: next };
    setHeld({ key: l.key, view: next });
  }, []);
  return useMemo(() => ({
    view, zoomed: isPhotoZoomed(view),
    zoomAt: (factor, at) => set((v, s) => photoZoomAt(v, factor, at, s)),
    panBy: (dx, dy) => set((v, s) => photoPanBy(v, dx, dy, s)),
    toggle: (at) => set((v, s) => togglePhotoZoom(v, at, s)),
    reset: () => set(() => PHOTO_FIT),
    swipe: (dx, threshold) => photoSwipe(live.current.view, dx, threshold),
  }), [view, set]);
}
