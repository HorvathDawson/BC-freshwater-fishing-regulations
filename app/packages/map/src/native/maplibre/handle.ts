/**
 * (T1154) THE BINDING'S ELEMENT REFS — the map, the camera, the water copy's source — made here, typed as the binding types
 * them, and handed to the shell as the engine-neutral handles it reads (`EngineRefs`, `../engine.ts`): tsc checks that
 * each binding ref satisfies its handle. The atlas source's handle is the shell's own callback ref (`atlasAttached`).
 */
import { useRef, type RefObject } from "react";
import type { CameraRef, GeoJSONSourceRef, MapRef } from "@maplibre/maplibre-react-native";
import type { EngineRefs } from "../engine";

interface BindingRefs {
  map: RefObject<MapRef | null>;
  camera: RefObject<CameraRef | null>;
  copy: RefObject<GeoJSONSourceRef | null>;
}

export function useEngineRefs(): EngineRefs {
  const refs: BindingRefs = { map: useRef<MapRef | null>(null), camera: useRef<CameraRef | null>(null),
                              copy: useRef<GeoJSONSourceRef | null>(null) };
  return refs;
}

/** The same refs, handed back to the binding's elements: they are the ones `useEngineRefs` made. */
export const bindingRefs = (refs: EngineRefs): BindingRefs => refs as unknown as BindingRefs;
