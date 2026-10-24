/**
 * The MapLibre Native renderer behind the contract (`MapRenderer`), through `@maplibre/maplibre-react-native` 11.5.0.
 * Rendering lives here; the style never does.
 *
 * Typed against the SLICE of the binding's refs it uses (the map, the atlas `VectorSource`, the camera), read through
 * getters because the binding mounts them only once the map is ready and re-creates them on a re-render. No SDK import:
 * `Map.native.tsx` is the one that does (layers.json), and `parity.test.ts` drives this with a recorder.
 *
 * WHAT A PHONE DOES DIFFERENTLY, all of it here:
 *   - every call is async across the bridge, and a failure is reported once per pass rather than thrown;
 *   - the ids come back as STRINGS on Android (`"311274"`, N1 §3); `sectionIdOf` makes them numbers;
 *   - only the section source-layers take feature-state (MVT ids; MapLibre Native ignores `promoteId`): an area
 *     layer's values are written into the style instead (`nativeStyle`'s `areaData`);
 *   - a feature-state already pushed is not pushed again (`onceSink`; `forget()` after a style swap, which drops them
 *     all): each is one TurboModule call per (feature, source-layer), and the status set is re-sent on every settle.
 */
import type { SectionKey } from "@app/core";
import { unionBbox, bboxOfGeometry, type Bbox } from "@app/core";
import { baseAdapter, type MapAdapter, type MapRenderer } from "./contract";
import {
  SECTION_SOURCE_LAYERS, TAP_GROUPS, VISIBLE_LAYERS, WATER_SOURCE_LAYERS, firstSection, onceSink, sectionIdOf, stateHandle,
  tapBox, visibleSections,
} from "../sections";
import { mirrorSink, type LitFeature } from "../water-copy";
import type { CopyMirror } from "./web";

/** The style adapter, as before: views, modes, groups, the ground, feature data — the platform-free half. */
export const nativeAdapter = (): MapAdapter => baseAdapter("native");

type Feature = { id?: unknown; geometry?: unknown; properties?: unknown };
type FeatureRef = { id: string | number; sourceLayer: string };
/** What `nativeRenderer` asks of the binding's refs (`MapRef`, `VectorSourceRef`, `CameraRef`). */
export interface NativeRefs {
  map(): {
    queryRenderedFeatures(...a: unknown[]): Promise<Feature[]>;
    getBounds(): Promise<readonly number[]>;
  } | null;
  atlas(): {
    setFeatureState(f: FeatureRef, s: Record<string, unknown>): Promise<void>;
    removeFeatureState(f: FeatureRef, key?: string): Promise<void>;
    querySourceFeatures(o: { sourceLayer: string; filter?: unknown }): Promise<Feature[]>;
  } | null;
  camera(): {
    fitBounds(b: [number, number, number, number], o: Record<string, unknown>): void;
    easeTo(o: { center: [number, number]; zoom: number; duration: number }): void;
  } | null;
}

export interface NativeRenderer extends MapRenderer {
  /** A style swap dropped every feature-state: push everything again. */
  forget(): void;
}

export function nativeRenderer(refs: NativeRefs, report: (e: unknown) => void, mirror?: CopyMirror): NativeRenderer {
  let failed = false;
  const fail = (e: unknown) => { if (!failed) { failed = true; report(e); } };
  const ours = (t: { source: string; sourceLayer: string }) =>
    t.source === "atlas" && SECTION_SOURCE_LAYERS.includes(t.sourceLayer);
  const raw = {
    set: (t: { source: string; sourceLayer: string }, id: string | number, s: Record<string, unknown>) => {
      const src = refs.atlas();
      if (src && ours(t)) void src.setFeatureState({ id, sourceLayer: t.sourceLayer }, s).catch(fail);
    },
    remove: (t: { source: string; sourceLayer: string }, id: string | number, key: string) => {
      const src = refs.atlas();
      if (src && ours(t)) void src.removeFeatureState({ id, sourceLayer: t.sourceLayer }, key).catch(fail);
    },
  };
  // (T175) every state for one of the water copy's sections goes to the copy as well — the one pathway, twice written
  const sink = onceSink(mirror ? mirrorSink(raw, mirror.copy, mirror.target) : raw);
  const litFeatures = async (ids: readonly number[]): Promise<LitFeature[]> => {
    const src = refs.atlas();
    const out: LitFeature[] = [];
    if (!src || !ids.length) return out;
    for (const sourceLayer of WATER_SOURCE_LAYERS)
      for (const f of await src.querySourceFeatures({ sourceLayer, filter: ["in", ["id"], ["literal", [...ids]]] })) {
        const id = sectionIdOf(f.id);     // Android hands ids back as strings (N1 §3)
        if (id !== null && ids.includes(id))
          out.push({ id, layer: sourceLayer, geometry: (f.geometry ?? null) as LitFeature["geometry"],
                     properties: (f.properties ?? null) as LitFeature["properties"] });
      }
    return out;
  };
  return {
    platform: "native",
    forget: () => { sink.forget(); failed = false; },
    states: (lit: () => readonly SectionKey[]) => stateHandle(sink, lit),
    fitBounds: (b, { padding, duration }) => refs.camera()?.fitBounds([b[0], b[1], b[2], b[3]], {
      padding: { top: padding, right: padding, bottom: padding, left: padding }, duration,
    }),
    easeTo: (c, { duration }) => refs.camera()?.easeTo({ center: [c.lon, c.lat], zoom: c.zoom, duration }),
    press: async (x, y) => {
      const box = tapBox(x, y);
      for (const g of TAP_GROUPS) {
        const m = refs.map();
        if (!m) return null;
        const id = firstSection(await m.queryRenderedFeatures(box, { layers: [...g.layers] }));
        if (id !== null) return { layer: g.as, id };
      }
      return null;
    },
    visible: async () => {
      const m = refs.map();
      return m ? visibleSections(await m.queryRenderedFeatures({ layers: [...VISIBLE_LAYERS] })) : [];
    },
    // the same pick as the copy's (T175), measured
    waterBox: async (ids) => {
      let found: Bbox | null = null;
      for (const f of await litFeatures(ids)) found = unionBbox(found, bboxOfGeometry(f.geometry as never));
      return found;
    },
    litFeatures,
    bounds: async () => {
      const b = await refs.map()?.getBounds();
      if (!b || b.length !== 4) throw new Error("the map has no bounds yet");
      return [b[0]!, b[1]!, b[2]!, b[3]!];
    },
  };
}
