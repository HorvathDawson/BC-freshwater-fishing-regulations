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
import type { LayoutChangeEvent } from "react-native";
import type { NativeRefs } from "../adapters/native";
import type { HeldLayer, HoldSource } from "../label-hold";
import type { MapProps } from "../map-props";
import type { meData, mePaint } from "../me";
import type { HoldStore } from "./labelHold";
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
  /** The tile event's own name, as a report about it says it (MapLibre Native's: `MLRNTileIds`). */
  event: string;
  /** Listen for each atlas tile the engine requests, with its ids (or a repeat without them); returns the unsubscribe. */
  onTileIds(listener: (e: TileIdsEvent) => void): () => void;
}

/**
 * A press, a still tap's lift or a long press, as the engine reports it: its `nativeEvent` is read FIRST, before any
 * await — React Native recycles the event, and it is null afterwards (N1 §3).
 */
export type EnginePress = { nativeEvent: { point: [number, number] | { x: number; y: number };
                                           lngLat: [number, number] } | null };
/** The camera about to move, moving, or come to rest (its `nativeEvent` read first, as a press's). */
export type EngineRegion = { nativeEvent: { center?: [number, number]; zoom?: number; userInteraction?: boolean;
                                            bounds?: [number, number, number, number] } | null };
type Layers = readonly { id: string }[];

/**
 * THE ENGINE'S MAP ELEMENT (`EngineView`): every value and every handler is the shell's; the engine draws them. A handler
 * left undefined is not listened for (a frame event costs a bridge call per frame).
 */
export interface EngineViewProps {
  refs: EngineRefs;
  /** The atlas source's handle, each time the engine attaches (or re-creates) it: the shell's callback ref. */
  atlasAttached(r: EngineAtlas | null): void;
  style: MapProps["style"];
  onLayout(e: LayoutChangeEvent): void;
  /** The style, handed whole: a change is a style swap. */
  mapStyle: unknown;
  /** The most device pixels per dp drawn; 0 is the device's own. */
  pixelRatio: number;
  /** How many zooms below the shown one are also loaded, as placeholders; -1 is the engine's own. */
  prefetchDelta: number;
  initial: { center: [number, number]; zoom: number };
  onPress(e: EnginePress): void;
  onTapUp?: (e: EnginePress) => void;
  onLongPress?: (e: EnginePress) => void;
  onRegionWillChange(e: EngineRegion): void;
  onRegionIsChanging?: (e: EngineRegion) => void;
  onRegionDidChange(e: EngineRegion): void;
  /** A frame drawn (tiles may be missing). */
  onFrame?: () => void;
  /** A frame drawn with every tile in. */
  onFrameFull?: () => void;
  onStyleLoaded(): void;
  /** The map drawn whole (MapLibre Native's "map fully rendered", its idle): the first opens the query gate. */
  onRenderedFully(): void;
  onFail(): void;
  /** (T876) the style's label layers adopted by id under their sources, and whether the hold hides them */
  held: { store: HoldStore; ids: Record<HoldSource, readonly string[]>; now: Record<HoldSource, readonly HeldLayer[]> };
  /** The generated images by name (`nativeImages`, the spots', the mark box). */
  images: Record<string, unknown>;
  gauges: GeoJSON.FeatureCollection;
  trace: unknown;
  tracePins: unknown;
  copy: unknown;
  circles: { data: unknown; layers: Layers };
  me: { data: ReturnType<typeof meData>; look: ReturnType<typeof mePaint> };
  pins: { data: unknown; layer: unknown };
  spots: { data: unknown; layers: Layers };
  marks: { data: unknown; layers: Layers };
}

export { EngineView, engineDialect, engineLog, engineTiles, useEngineRefs } from "./maplibre";
