/**
 * THE PHONE'S TILES, AS THE COLOURING ENGINE SEES THEM (T74). The patched binding reports each atlas tile MapLibre
 * Native requests, with its section ids per source-layer (the device event `MLRNTileIds`, read from the same local
 * archive; deps.md): the native half of GL JS's `sourcedata`. The map labels the tile from it at once, and keeps it
 * here, bounded, for what the engine asks of a renderer after a new labelling or a style swap — every tile it holds
 * (`TileRenderer.loaded`). Platform-free, so it is tested in node.
 *
 * (T420) A TILE'S IDS COME ONCE PER WATCH. MapLibre Native re-requests tiles it already holds many times a second; the
 * binding sends a tile's ids the first time only, and a repeat as `{tag, z, x, y}` with no `layers` (`touch`), which
 * moves the tile to the end here exactly as the binding's own memory of what it sent (an LRU of `NATIVE_SENT_MAX`,
 * touched in the same order). The hold keeps at least twice that, so a repeat always finds its tile; one that does not
 * is a broken invariant, reported by the caller and answered with a new watch (which makes the binding send again).
 */
import { LABEL_SOURCE_LAYERS, type TileSections } from "./colouring";

/** The binding's memory of tiles sent (`SENT_MAX` in the patch's MLRNVectorSource.kt / .m). */
export const NATIVE_SENT_MAX = 256;

/** The device event the patched binding sends per requested tile. */
export const TILE_IDS_EVENT = "MLRNTileIds";

export interface TileIdsEvent {
  /** The `VectorSource` view's tag (`nativeTag()`): which map's source asked. */
  tag: number;
  z: number;
  x: number;
  y: number;
  /** The tile's ids per source-layer; absent on a repeat of a tile already sent in this watch. */
  layers?: Record<string, readonly number[]>;
}

/** The tiles last reported, most recent last; a tile reported again moves to the end. */
export class TileHold {
  private tiles = new Map<string, TileSections>();
  constructor(private readonly max = 2 * NATIVE_SENT_MAX) {
    if (max < 2 * NATIVE_SENT_MAX) throw new Error(`TileHold: ${max} tiles cannot cover the binding's ${NATIVE_SENT_MAX}`);
  }

  /** Keep one event's tile and return its sections — only the source-layers that wear labels. */
  put(e: TileIdsEvent & { layers: Record<string, readonly number[]> }): TileSections {
    const t: Record<string, readonly number[]> = {};
    for (const l of LABEL_SOURCE_LAYERS) {
      const ids = e.layers[l];
      if (ids && ids.length) t[l] = ids;
    }
    const key = `${e.z}/${e.x}/${e.y}`;
    this.tiles.delete(key);
    this.tiles.set(key, t as TileSections);
    if (this.tiles.size > this.max) this.tiles.delete(this.tiles.keys().next().value!);
    return t as TileSections;
  }

  /** A repeat request: the tile moves to the end. False when it is not held (the caller reports and re-watches). */
  touch(e: Pick<TileIdsEvent, "z" | "x" | "y">): boolean {
    const key = `${e.z}/${e.x}/${e.y}`;
    const t = this.tiles.get(key);
    if (!t) return false;
    this.tiles.delete(key);
    this.tiles.set(key, t);
    return true;
  }

  all(): TileSections[] { return [...this.tiles.values()]; }
  get size(): number { return this.tiles.size; }
  clear(): void { this.tiles.clear(); }
}
