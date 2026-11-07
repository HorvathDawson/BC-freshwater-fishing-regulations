/**
 * The photo viewer's browser-only input (T454): a mouse wheel or a trackpad pinch (ctrl+wheel) over the photo zooms it
 * about the pointer. The page never scrolls under the viewer, so every wheel is taken. Measures only: the offset in the
 * element and deltaY go to the caller, which hands them to core's `photoWheelFactor`.
 */
import { useEffect, type RefObject } from "react";

export function usePhotoWheel(ref: RefObject<unknown>, onWheel: (x: number, y: number, deltaY: number) => void,
                              enabled: boolean): void {
  useEffect(() => {
    const el = ref.current as HTMLElement | null;
    if (!enabled || !el || typeof el.addEventListener !== "function") return;
    const h = (e: WheelEvent) => {
      const r = el.getBoundingClientRect();
      onWheel(e.clientX - r.left, e.clientY - r.top, e.deltaMode === 1 ? e.deltaY * 33 : e.deltaY);
      e.preventDefault();
    };
    el.addEventListener("wheel", h, { passive: false });
    return () => el.removeEventListener("wheel", h);
  }, [ref, onWheel, enabled]);
}
