/**
 * The native half of `tile-ids.web.ts`: NOTHING TO READ. The MapLibre Native binding exposes no per-tile load event
 * and no tile object, so there is no tile to take ids from (colour-latency study; how the native map colours is open
 * until N2 — a native tile event or bulk state, or a ring with a zoom floor). Same exports, so a caller compiles on
 * both and gets the honest answer here: null, take another route.
 */

/** The maplibre-gl minor the web half's internals were read from. */
export const GL_INTERNALS = "5.24";

export function tileIds(_tile: unknown, _layers: readonly string[]): Record<string, number[]> | null {
  return null;
}

export function sourceTiles(_map: unknown, _sourceId: string): unknown[] | null {
  return null;
}
