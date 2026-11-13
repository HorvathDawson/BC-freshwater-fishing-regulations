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
 *     all): each is one TurboModule call per (feature, source-layer), and the status set is re-sent on every settle;
 *   - (T345) a key is unset by a null write, never by a per-feature `removeFeatureState` (see `raw.remove`).
 */
import type { SectionKey } from "@app/core";
import { unionBbox, bboxOfGeometry, type Bbox } from "@app/core";
import { baseAdapter, type MapAdapter, type MapRenderer } from "./contract";
import {
  NOT_FISHING_LAYERS, OBSTACLE_LAYERS, SECTION_SOURCE_LAYERS, VISIBLE_LAYERS, WATER_SOURCE_LAYERS, firstNotFishing, firstObstacle, pickSection,
  onceSink, sectionIdOf, stateHandle,
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
    easeTo(o: { center: [number, number]; zoom: number; duration: number; easing?: "ease" }): void;
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
    // (T345) a key is UNSET by writing it null, never by `removeFeatureState`: MapLibre Native queues a frame's
    // removals apart from its sets and applies them AFTER the sets, and keeps only the FIRST per-feature removal per
    // source-layer in a frame (`SourceFeatureState::removeFeatureState`) — so moving the selection (remove `part`
    // and `water` from the old ids, set them on the new) left 316504 at `water` and unlit, and 316505 lit AND
    // `water` (the Coquitlam's "sliver", measured on the emulator by `getFeatureState`). A null write is a set: it
    // goes in order with the others, and every paint reads a null state as unset (`["boolean", …, false]`).
    remove: (t: { source: string; sourceLayer: string }, id: string | number, key: string) => {
      const src = refs.atlas();
      if (src && ours(t)) void src.setFeatureState({ id, sourceLayer: t.sourceLayer }, { [key]: null }).catch(fail);
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
    // (T400) EASED, both: the binding's fitBounds defaults to `easing: "fly"` — an arc out and back in, squeezed into
    // 450 ms, through zooms whose tiles are not loaded — which read as a choppy jump on a phone. The web eases.
    fitBounds: (b, { padding, duration }) => refs.camera()?.fitBounds([b[0], b[1], b[2], b[3]], {
      padding: { top: padding, right: padding, bottom: padding, left: padding }, duration, easing: "ease",
    }),
    easeTo: (c, { duration }) => refs.camera()?.easeTo({ center: [c.lon, c.lat], zoom: c.zoom, duration, easing: "ease" }),
    press: (x, y) => pickSection(async (box, g) => {
      const m = refs.map();
      return m ? m.queryRenderedFeatures(box, { layers: [...g.layers] }) : null;
    }, x, y),
    pressObstacle: async (x, y) => {
      const m = refs.map();
      return m ? firstObstacle(await m.queryRenderedFeatures(tapBox(x, y), { layers: [...OBSTACLE_LAYERS] })) : null;
    },
    pressNotFishing: async (x, y) => {
      const m = refs.map();
      return m ? firstNotFishing(await m.queryRenderedFeatures(tapBox(x, y), { layers: [...NOT_FISHING_LAYERS] })) : null;
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
