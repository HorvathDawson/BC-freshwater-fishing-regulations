/**
 * T214 item 2: on the web a drag between a water's views is for touch only. The primary pointer is what decides: a
 * phone or tablet (`pointer: coarse`) swipes; a desktop, and a laptop whose touch screen is the second pointer, does
 * not (its buttons move between views). No `matchMedia` (a test, an old browser) falls back to touch points.
 */
export function isTouchDevice(): boolean {
  if (typeof window === "undefined") return true;
  if (typeof window.matchMedia === "function") return window.matchMedia("(pointer: coarse)").matches;
  return typeof navigator !== "undefined" && navigator.maxTouchPoints > 0;
}
