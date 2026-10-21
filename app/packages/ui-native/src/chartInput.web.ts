/**
 * The flow chart's browser-only input (owner 2026-10-21: zoom the chart). The touch gestures are
 * react-native-gesture-handler's and run on both targets; a MOUSE WHEEL and "is this a mouse or a finger" are the
 * browser's own, so they live here with a no-op native twin (`chartInput.native.ts`).
 *
 * Measures only: the wheel's offset across the element and its deltaY go to the caller, which hands them to core's
 * `wheelFactor` through @app/ui's `zoomAt`.
 */
import { useEffect, type RefObject } from "react";

/** Calls `onWheel(x, deltaY)` for a wheel over the element; the page does not scroll while the chart takes it. */
export function useChartWheel(ref: RefObject<unknown>, onWheel: (x: number, deltaY: number) => void,
                              enabled: boolean): void {
  useEffect(() => {
    const el = ref.current as HTMLElement | null;
    if (!enabled || !el || typeof el.addEventListener !== "function") return;
    const h = (e: WheelEvent) => {
      // Over the chart the wheel ZOOMS, as the hint beside it says (a map does the same); a trackpad pinch arrives
      // as a ctrl+wheel and is the same zoom. The page scrolls everywhere else.
      e.preventDefault();
      const r = el.getBoundingClientRect();
      onWheel(e.clientX - r.left, e.deltaMode === 1 ? e.deltaY * 33 : e.deltaY);
    };
    el.addEventListener("wheel", h, { passive: false });
    return () => el.removeEventListener("wheel", h);
  }, [ref, onWheel, enabled]);
}

/**
 * Calls `onHover(x)` as a MOUSE moves over the element and `onHover(null)` when it leaves — the read-out follows the
 * pointer, as the hint says. A finger or a pen is left to the tap (a touch "hover" would fight the drag).
 */
export function useChartHover(ref: RefObject<unknown>, onHover: (x: number | null) => void,
                              enabled: boolean): void {
  useEffect(() => {
    const el = ref.current as HTMLElement | null;
    if (!enabled || !el || typeof el.addEventListener !== "function") return;
    const move = (e: PointerEvent) => {
      if (e.pointerType !== "mouse") return;
      onHover(e.clientX - el.getBoundingClientRect().left);
    };
    const leave = (e: PointerEvent) => { if (e.pointerType === "mouse") onHover(null); };
    el.addEventListener("pointermove", move);
    el.addEventListener("pointerleave", leave);
    return () => { el.removeEventListener("pointermove", move); el.removeEventListener("pointerleave", leave); };
  }, [ref, onHover, enabled]);
}

/** Which hint the chart shows: a coarse pointer (a finger) gets the touch words. */
export function chartPointer(): "touch" | "pointer" {
  try {
    return globalThis.matchMedia?.("(pointer: coarse)").matches ? "touch" : "pointer";
  } catch {
    return "pointer";
  }
}
