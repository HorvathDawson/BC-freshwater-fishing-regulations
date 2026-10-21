/**
 * GESTURES, DECIDED (G1). Every threshold a finger is measured against, and every decision made from a measurement,
 * as pure functions — so the web, Android and (later) iOS bindings cannot disagree about what a swipe is.
 *
 * Units are the ones react-native-gesture-handler reports: distances in px (translation since the press began),
 * velocities in px per SECOND. A binding measures and calls these; it never compares a number of its own.
 *
 *   back       the left-edge swipe, only where the platform has no system back (`edgeBackEnabled`)
 *   part       a sideways swipe on a water's view steps to the neighbouring part (`classifySwipe`, `stepPart`)
 *   sheet      a bottom sheet dragged down closes past a third of its height or on a flick (`sheetDismiss`)
 *   map        one policy for both map bindings (`MAP_GESTURES`): pan, pinch, double-tap and wheel zoom; no rotate,
 *              no tilt; a tap is a press that moved less than `TAP_SLOP`
 */

/** A press that moved less than this (px) is a tap — the prototype's rule. A small pan never selects a section. */
export const TAP_SLOP = 8;

/** The left-edge back swipe: starts in the first `zone` px, commits past `commit` px or on a rightward `flick`. */
export const EDGE_BACK = { zone: 24, commit: 80, flick: 500 } as const;

/**
 * The sideways part swipe. `lock`: horizontal must beat vertical by this factor, or it is a scroll and nothing
 * steps (direction lock). Commits past `distance` px, or on a `flick` that has moved at least `flickMin` px the
 * same way (so a tap that happens to be fast steps nothing).
 */
export const SWIPE = { lock: 2, distance: 60, flick: 500, flickMin: 20 } as const;

/** A sheet closes when dragged down past this fraction of its height, or on a downward `flick`. */
export const SHEET_DRAG = { fraction: 1 / 3, flick: 800 } as const;

/** How far a view may be pulled when there is nowhere to go (the end of a water): a short resistance, no wrap. */
export const RESIST = { factor: 0.3, max: 32 } as const;

/** Did a press that moved (dx, dy) stay a tap? Strictly under `TAP_SLOP`. */
export function isTap(dx: number, dy: number): boolean {
  return Math.hypot(dx, dy) < TAP_SLOP;
}

/**
 * Is the left-edge back swipe on, on this platform? Only where there is no system back: iOS. Android's own edge
 * gesture owns that strip (and the system back reaches `useBack` through `BackHandler`); a browser owns it on the web.
 */
export function edgeBackEnabled(os: string): boolean {
  return os === "ios";
}

/** Does a press starting `x` px from the left edge begin an edge-back swipe? */
export function edgeBackStarts(x: number): boolean {
  return x >= 0 && x < EDGE_BACK.zone;
}

/** Released after dragging `dx` px right at `vx` px/s: does the edge swipe go back? */
export function edgeBackCommits(dx: number, vx: number): boolean {
  return dx >= EDGE_BACK.commit || (dx > 0 && vx >= EDGE_BACK.flick);
}

/**
 * May a part swipe begin at `x` in a view `width` px wide? Never inside either edge zone: the left one is the back
 * swipe's (iOS) and both are the system's edge gesture on Android and in a phone browser.
 */
export function partSwipeStarts(x: number, width: number): boolean {
  return x >= EDGE_BACK.zone && x <= width - EDGE_BACK.zone;
}

export type SwipeStep = "next" | "prev";

/**
 * A finished drag of (dx, dy) leaving at `vx` px/s: a step, or nothing. Finger moving LEFT is "next" (the next part
 * slides in from the right, like turning a page); finger moving RIGHT is "prev".
 */
export function classifySwipe(dx: number, dy: number, vx: number): SwipeStep | null {
  const ax = Math.abs(dx);
  if (ax === 0 || ax < Math.abs(dy) * SWIPE.lock) return null;
  const far = ax >= SWIPE.distance;
  const flick = ax >= SWIPE.flickMin && Math.abs(vx) >= SWIPE.flick && Math.sign(vx) === Math.sign(dx);
  if (!far && !flick) return null;
  return dx < 0 ? "next" : "prev";
}

/**
 * The neighbour of position `at` among `count` parts, or null at an end (no wrap). Positions are in the water's step
 * order — mouth first, the order of the water map's ▼ Downstream / ▲ Upstream buttons — so "next" is upstream.
 */
export function stepPart(at: number, count: number, dir: SwipeStep): number | null {
  if (!Number.isInteger(at) || !Number.isInteger(count) || at < 0 || at >= count) return null;
  const to = dir === "next" ? at + 1 : at - 1;
  return to >= 0 && to < count ? to : null;
}

/** A sheet dragged down `dy` px, released at `vy` px/s, `height` px tall: does it close? */
export function sheetDismiss(dy: number, vy: number, height: number): boolean {
  if (dy <= 0) return false;
  return (height > 0 && dy >= height * SHEET_DRAG.fraction) || vy >= SHEET_DRAG.flick;
}

/** Where a dragged view sits for a finger `d` px along: with it when it can move, held back when it cannot. */
export function follow(d: number, free: boolean): number {
  if (free) return d;
  const r = Math.min(Math.abs(d) * RESIST.factor, RESIST.max);
  return d < 0 ? -r : r;
}

/**
 * THE MAP'S GESTURES, one policy for both bindings: `Map.web.tsx` turns it into maplibre-gl options
 * (`@app/map` `maplibreGestures`), the native map into MapLibre Native's props (`nativeMapGestures`).
 */
export const MAP_GESTURES = {
  pan: true,
  pinch: true,
  doubleTapZoom: true,
  /** The mouse wheel / trackpad — web only; a phone has none. */
  wheel: true,
  rotate: false,
  tilt: false,
  tapSlop: TAP_SLOP,
} as const;
export type MapGestures = { readonly [K in keyof typeof MAP_GESTURES]: (typeof MAP_GESTURES)[K] extends number
  ? number : boolean };
