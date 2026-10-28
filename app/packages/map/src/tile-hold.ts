/**
 * THE PHONE'S TILES, AS THE COLOURING ENGINE SEES THEM (T74). The patched binding reports each atlas tile MapLibre
 * Native requests, with its section ids per source-layer (the device event `MLRNTileIds`, read from the same local
 * archive; deps.md): the native half of GL JS's `sourcedata`. The map labels the tile from it at once, and keeps it
 * here, bounded, for what the engine asks of a renderer after a new labelling or a style swap — every tile it holds
 * (`TileRenderer.loaded`). Platform-free, so it is tested in node.
 */
import { LABEL_SOURCE_LAYERS, type TileSections } from "./colouring";

/** The device event the patched binding sends per requested tile. */
export const TILE_IDS_EVENT = "MLRNTileIds";

export interface TileIdsEvent {
  /** The `VectorSource` view's tag (`nativeTag()`): which map's source asked. */
  tag: number;
  z: number;
  x: number;
  y: number;
  layers: Record<string, readonly number[]>;
}

/** The tiles last reported, most recent last; a tile reported again moves to the end. */
export class TileHold {
  private tiles = new Map<string, TileSections>();
  constructor(private readonly max = 512) {}

  /** Keep one event's tile and return its sections — only the source-layers that wear labels. */
  put(e: TileIdsEvent): TileSections {
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

  all(): TileSections[] { return [...this.tiles.values()]; }
  get size(): number { return this.tiles.size; }
  clear(): void { this.tiles.clear(); }
}
