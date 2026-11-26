/**
 * (T1154) MAPLIBRE NATIVE'S PER-TILE IDS: the patched binding's device event, moved here unchanged from
 * `../useColouring.ts` and `../../tile-hold.ts`. The binding reports each atlas tile MapLibre Native requests, with its
 * section ids per source-layer read from the same local archive (deps.md: the patch), as `MLRNTileIds` on React Native's
 * device event bus; the colouring engine (`../useColouring.ts`) labels the tile from it and keeps it (`TileHold`).
 */
import { DeviceEventEmitter } from "react-native";
import type { EngineTiles } from "../engine";

/** The binding's memory of tiles sent (`SENT_MAX` in the patch's MLRNVectorSource.kt / .m). */
export const NATIVE_SENT_MAX = 256;

/** The device event the patched binding sends per requested tile. */
const TILE_IDS_EVENT = "MLRNTileIds";

export const maplibreTiles: EngineTiles = {
  sentMax: NATIVE_SENT_MAX,
  onTileIds: (listener) => {
    const sub = DeviceEventEmitter.addListener(TILE_IDS_EVENT, listener);
    return () => sub.remove();
  },
};
