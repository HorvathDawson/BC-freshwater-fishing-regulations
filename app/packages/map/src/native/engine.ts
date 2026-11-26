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
import type { RefObject } from "react";
import type { NativeRefs } from "../adapters/native";
import type { TileIdsEvent } from "../tile-hold";
import type { AtlasSource } from "./useColouring";

/** A feature as the engine's queries hand it back. */
export interface EngineFeature { id?: string | number; geometry?: unknown; properties?: Record<string, unknown> | null }
type Box = [[number, number], [number, number]];
/** The map view's handle: what is drawn under a box or on screen, the bounds, a place's point on screen. */
export interface EngineMap {
  queryRenderedFeatures(box: Box, options?: { layers?: string[] }): Promise<EngineFeature[]>;
  queryRenderedFeatures(options?: { layers?: string[] }): Promise<EngineFeature[]>;
  getBounds(): Promise<readonly number[]>;
  project(lngLat: [number, number]): Promise<readonly number[]>;
}
/** The camera's handle (`adapters/native.ts` moves it). */
export type EngineCamera = NonNullable<ReturnType<NativeRefs["camera"]>>;
/**
 * The atlas vector source's handle: feature-state one by one (`adapters/native.ts`) and per tile in batches
 * (`useColouring.ts`), and read back for the water copy.
 */
export type EngineAtlas = NonNullable<ReturnType<NativeRefs["atlas"]>> & AtlasSource & {
  getFeatureState(f: { id: string | number; sourceLayer: string }): Promise<Record<string, unknown> | null>;
};
/** The water copy's GeoJSON source handle (T175). */
export interface EngineCopySource {
  setFeatureState(f: { id: string | number }, s: Record<string, unknown>): Promise<void>;
  removeFeatureState(f: { id: string | number }, key?: string): Promise<void>;
  removeFeatureState(): Promise<void>;
}
/** The element handles the shell reads (`useEngineRefs`), each null until the engine has mounted it. */
export interface EngineRefs {
  map: RefObject<EngineMap | null>;
  camera: RefObject<EngineCamera | null>;
  copy: RefObject<EngineCopySource | null>;
}

/** An engine's per-tile ids (the colouring's tile source, `useColouring.ts`). */
export interface EngineTiles {
  /** How many tiles the engine remembers sending (a repeat of one of them comes without its ids): `TileHold` keeps more. */
  sentMax: number;
  /** Listen for each atlas tile the engine requests, with its ids (or a repeat without them); returns the unsubscribe. */
  onTileIds(listener: (e: TileIdsEvent) => void): () => void;
}

export { HeldLabels, bindingRefs, engineLog, maplibreTiles as engineTiles, useEngineRefs } from "./maplibre";
