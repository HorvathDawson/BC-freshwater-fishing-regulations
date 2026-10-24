/**
 * The maplibre-gl renderer behind the contract (`MapRenderer`). Rendering lives here; the style never does.
 *
 * Typed against the SLICE of a maplibre-gl `Map` it uses, not the SDK, so this file imports no map SDK (`Map.web.tsx`
 * is the one that does, layers.json) and `parity.test.ts` can drive it with a recorder.
 */
import { bboxOfGeometry, unionBbox, type Bbox, type SectionKey } from "@app/core";
import { baseAdapter, type MapAdapter, type MapRenderer } from "./contract";
import {
  TAP_GROUPS, VISIBLE_LAYERS, WATER_SOURCE_LAYERS, firstSection, onceSink, sectionIdOf, stateHandle, tapBox, visibleSections,
} from "../sections";
import { mirrorSink, type CopyTarget, type LitFeature, type WaterCopy } from "../water-copy";

/** The style adapter, as before: views, modes, groups, the ground, feature data — the platform-free half. */
export const webAdapter = (): MapAdapter => baseAdapter("web");

type Target = { source: string; sourceLayer: string; id: string | number };
type LayerTarget = { source: string; sourceLayer: string; id?: string | number };
type Feature = { id?: unknown; geometry?: unknown; properties?: unknown };
/** What `webRenderer` asks of a maplibre-gl `Map`. */
export interface GlMapSlice {
  getLayer(id: string): unknown;
  setFeatureState(t: Target, s: Record<string, unknown>): void;
  removeFeatureState(t: LayerTarget, key?: string): void;
  queryRenderedFeatures(...a: unknown[]): Feature[];
  querySourceFeatures(source: string, o: { sourceLayer: string }): Feature[];
  fitBounds(b: [[number, number], [number, number]], o: Record<string, unknown>): unknown;
  easeTo(o: { center: [number, number]; zoom: number; duration: number }): unknown;
  getBounds(): { getWest(): number; getSouth(): number; getEast(): number; getNorth(): number };
}

/** The web renderer: the contract, plus the one call only the web's profile colouring makes. */
export interface WebRenderer extends MapRenderer {
  /**
   * Drop EVERY feature-state on one source-layer — the profile colouring's release swap (colour-latency AP). Through
   * the renderer, not past it: the sink must forget what it held there, or the highlight the swap puts back is taken
   * for "already set" and never sent (colour-ap review).
   */
  clearSourceLayer(source: string, sourceLayer: string): void;
}

/**
 * (T175) The water copy this map keeps, and where its states go: every per-section state written for one of its
 * sections is written there too (`mirrorSink`). Absent, nothing is mirrored.
 */
export interface CopyMirror { copy(): WaterCopy | null; target(): CopyTarget | null }

export function webRenderer(m: GlMapSlice, maxZoom: number, mirror?: CopyMirror): WebRenderer {
  const present = (ids: readonly string[]) => ids.filter((id) => m.getLayer(id) !== undefined);
  // GL JS keeps feature-state for the life of the map (the style is never replaced), so nothing is ever forgotten
  const raw = {
    set: (t: { source: string; sourceLayer: string }, id: string | number, s: Record<string, unknown>) =>
      m.setFeatureState({ ...t, id }, s),
    remove: (t: { source: string; sourceLayer: string }, id: string | number, key: string) =>
      m.removeFeatureState({ ...t, id }, key),
  };
  const sink = onceSink(mirror ? mirrorSink(raw, mirror.copy, mirror.target) : raw);
  const litFeatures = async (ids: readonly number[]): Promise<LitFeature[]> => {
    const want = new Set(ids);
    const out: LitFeature[] = [];
    if (!want.size) return out;
    for (const sourceLayer of WATER_SOURCE_LAYERS)
      for (const f of m.querySourceFeatures("atlas", { sourceLayer })) {
        const id = sectionIdOf(f.id);
        if (id !== null && want.has(id))
          out.push({ id, layer: sourceLayer, geometry: (f.geometry ?? null) as LitFeature["geometry"],
                     properties: (f.properties ?? null) as LitFeature["properties"] });
      }
    return out;
  };
  return {
    platform: "web",
    clearSourceLayer: (source, sourceLayer) => {
      m.removeFeatureState({ source, sourceLayer });
      sink.forget({ source, sourceLayer });
    },
    states: (lit: () => readonly SectionKey[]) => stateHandle(sink, lit),
    fitBounds: (b, { padding, duration }) =>
      void m.fitBounds([[b[0], b[1]], [b[2], b[3]]], { padding, maxZoom, duration }),
    easeTo: (c, { duration }) => void m.easeTo({ center: [c.lon, c.lat], zoom: c.zoom, duration }),
    press: async (x, y) => {
      const box = tapBox(x, y);
      for (const g of TAP_GROUPS) {
        const layers = present(g.layers);
        if (!layers.length) continue;
        const id = firstSection(m.queryRenderedFeatures(box, { layers }));
        if (id !== null) return { layer: g.as, id };
      }
      return null;
    },
    visible: async () => visibleSections(m.queryRenderedFeatures({ layers: present(VISIBLE_LAYERS) })),
    // the same pick as the copy's (T175), measured
    waterBox: async (ids) => {
      let found: Bbox | null = null;
      for (const f of await litFeatures(ids)) found = unionBbox(found, bboxOfGeometry(f.geometry as never));
      return found;
    },
    litFeatures,
    bounds: async () => {
      const b = m.getBounds();
      return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
    },
  };
}
