/**
 * (T1154) THE MAP ENGINE, AS THE PHONE'S MAP SEES IT. `../Map.native.tsx` is the SHELL: everything that DECIDES (the
 * fit and its refine, the water copy, the label hold, the look tracker and "drawn whole", the settle ask, the tap order
 * and its lift, spots, marks, pins, the callout, the scale bar, the zoom buttons, the credits, the screen-reader buttons,
 * the colouring's queue). The ENGINE draws: its component tree, its element refs, its per-tile event, its log, its held
 * label layers and its style dialect, each in `native/<engine>/` (today one: `native/maplibre/`).
 *
 * THIS FILE IS THE ONE WAY IN. Outside an engine folder no file of `packages/map/src` names a map SDK, a binding's event
 * or the device event bus, and none reaches into an engine folder but this one (`app/tools/check-boundaries.mjs`, the
 * engine rule; `app/layers.json`, the `packages/map/src/native/maplibre` layer). The types here are engine-neutral; the
 * one line that picks the engine is the re-export at the end.
 */
import type { TileIdsEvent } from "../tile-hold";

/** An engine's per-tile ids (the colouring's tile source, `useColouring.ts`). */
export interface EngineTiles {
  /** How many tiles the engine remembers sending (a repeat of one of them comes without its ids): `TileHold` keeps more. */
  sentMax: number;
  /** Listen for each atlas tile the engine requests, with its ids (or a repeat without them); returns the unsubscribe. */
  onTileIds(listener: (e: TileIdsEvent) => void): () => void;
}

export { HeldLabels, maplibreTiles as engineTiles } from "./maplibre";
