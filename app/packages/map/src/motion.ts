/**
 * (T243) THE CAMERA'S OWN MOVES under the OS "reduce motion" setting: framing a water, returning from search, the water
 * copy's zoom probe and a spot's centring glide 300–450 ms; with reduce motion on they jump (0 ms). The reader's own
 * pans and pinches are theirs and untouched. Each renderer keeps the flag current from its platform (`Map.web.tsx`: the
 * `prefers-reduced-motion` media query; `Map.native.tsx`: AccessibilityInfo), so this file stays platform-free.
 */
let reduced = false;

export function setReducedMotion(on: boolean): void { reduced = on; }

/** a camera move's duration: as asked, or 0 while reduce motion is on */
export function cameraMs(ms: number): number { return reduced ? 0 : ms; }
