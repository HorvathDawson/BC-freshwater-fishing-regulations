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

/** The style adapter, as before: views, modes, groups, the ground, feature data — the platform-free half. */
export const webAdapter = (): MapAdapter => baseAdapter("web");

type Target = { source: string; sourceLayer: string; id: string | number };
type Feature = { id?: unknown; geometry?: unknown };
/** What `webRenderer` asks of a maplibre-gl `Map`. */
export interface GlMapSlice {
  getLayer(id: string): unknown;
  setFeatureState(t: Target, s: Record<string, unknown>): void;
  removeFeatureState(t: Target, key?: string): void;
  queryRenderedFeatures(...a: unknown[]): Feature[];
  querySourceFeatures(source: string, o: { sourceLayer: string }): Feature[];
  fitBounds(b: [[number, number], [number, number]], o: Record<string, unknown>): unknown;
  getBounds(): { getWest(): number; getSouth(): number; getEast(): number; getNorth(): number };
}

export function webRenderer(m: GlMapSlice, maxZoom: number): MapRenderer {
  const present = (ids: readonly string[]) => ids.filter((id) => m.getLayer(id) !== undefined);
  // GL JS keeps feature-state for the life of the map (the style is never replaced), so nothing is ever forgotten
  const sink = onceSink({
    set: (t, id, s) => m.setFeatureState({ ...t, id }, s),
    remove: (t, id, key) => m.removeFeatureState({ ...t, id }, key),
  });
  return {
    platform: "web",
    states: (lit: () => readonly SectionKey[]) => stateHandle(sink, lit),
    fitBounds: (b, { padding, duration }) =>
      void m.fitBounds([[b[0], b[1]], [b[2], b[3]]], { padding, maxZoom, duration }),
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
    waterBox: async (ids) => {
      const want = new Set(ids);
      let found: Bbox | null = null;
      for (const sourceLayer of WATER_SOURCE_LAYERS)
        for (const f of m.querySourceFeatures("atlas", { sourceLayer })) {
          const id = sectionIdOf(f.id);
          if (id !== null && want.has(id)) found = unionBbox(found, bboxOfGeometry(f.geometry as never));
        }
      return found;
    },
    bounds: async () => {
      const b = m.getBounds();
      return [b.getWest(), b.getSouth(), b.getEast(), b.getNorth()];
    },
  };
}
