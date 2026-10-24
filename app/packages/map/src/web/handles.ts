/**
 * THE WEB MAP'S HANDLES: what the adapter, the colouring engine, the water copy and the route overlay write through —
 * each a thin view of one maplibre-gl map.
 */
import type * as maplibregl from "maplibre-gl";
import type { MapHandle, MapRenderer } from "../adapters/contract";
import type { WebRenderer } from "../adapters/web";
import { LABEL_SOURCE_LAYERS, type LabelTarget, type TileSections } from "../colouring";
import { COPY_SOURCE, type CopyTarget, type WaterCopy } from "../water-copy";

/**
 * Feature-state on the atlas, by source-layer: the target the profile colouring writes to. The labels go straight to
 * the map (a section's label is set once per index, so the renderer's de-dupe has nothing to add); the release swap's
 * clear goes THROUGH the renderer, so its sink forgets what it held there and the highlight put back is really sent.
 */
export const atlasTarget = (m: maplibregl.Map, r: WebRenderer, copy: WaterCopy, to: CopyTarget): LabelTarget => ({
  setState: (sourceLayer, id, s) => {
    m.setFeatureState({ source: "atlas", sourceLayer, id }, s);
    // (T175) a label for one of the water copy's sections is the copy's label too — the one pathway
    const fresh = copy.fresh(id, s);
    if (fresh) to.set(id, fresh);
  },
  clearAll: (sourceLayer) => {
    r.clearSourceLayer("atlas", sourceLayer);
    // and a clear of its layer clears the copy's (what the engine then puts back, the mirror puts back there too)
    for (const id of copy.ids)
      if ((copy.layerOf(id) ?? sourceLayer) === sourceLayer && copy.drop(id)) to.remove(id);
  },
});

/** (T175) The water copy's feature-state, on its own source — while the source exists (a style always declares it). */
export const copyTarget = (m: maplibregl.Map): CopyTarget => ({
  set: (id, s) => { if (m.getSource(COPY_SOURCE)) m.setFeatureState({ source: COPY_SOURCE, id }, s); },
  remove: (id, key) => { if (m.getSource(COPY_SOURCE)) m.removeFeatureState({ source: COPY_SOURCE, id }, key); },
});

/**
 * THE PUBLIC ROUTE to the sections the map holds: `querySourceFeatures` per source-layer, over every tile it has
 * rendered. Heavier than reading one tile's ids (`tile-ids.web.ts`), and blind to tiles in the out-of-view cache, but it
 * uses nothing private — it is what runs when 5.24's internals are not there.
 */
export function publicSections(m: maplibregl.Map): TileSections {
  const out: Record<string, number[]> = {};
  for (const layer of LABEL_SOURCE_LAYERS) {
    const ids: number[] = [];
    for (const f of m.querySourceFeatures("atlas", { sourceLayer: layer }))
      if (typeof f.id === "number") ids.push(f.id);
    out[layer] = ids;
  }
  return out;
}

/**
 * The adapter's handle on this map's layers: paint, visibility, zoom range, and feature-state through the renderer
 * (`r.states`, N2) — clearing there removes only `selected`, per feature, and here nothing is lit, so a mode change
 * never takes the profile labels with it.
 */
export function layerHandle(m: maplibregl.Map, r: MapRenderer): MapHandle {
  return {
    ...r.states(() => []),
    /*
     * THE ZOOM A LAYER TAKES IN THIS VIEW — see `minzoomByView`. The Conditions view
     * stands the rivers down below z9 so the basin field can answer instead; every other
     * view resets them to the style's own floor, which is why this is called on every
     * view change and not once at load.
     */
    setZoomRange: (id: string, minzoom: number, maxzoom?: number) => {
      if (!m.getLayer(id)) return;
      const spec = m.getLayer(id) as { maxzoom?: number };
      m.setLayerZoomRange(id, minzoom, maxzoom ?? spec.maxzoom ?? 24);
    },
    setVisibility: (id: string, v: boolean) => {
      if (m.getLayer(id)) m.setLayoutProperty(id, "visibility", v ? "visible" : "none");
    },
    setPaint: (id: string, prop: string, value: unknown) => {
      if (m.getLayer(id)) m.setPaintProperty(id, prop as never, value as never);
    },
  };
}

/** The route overlay's handle: its sources' data and nothing else (`setTrace`). */
export const traceHandle = (m: maplibregl.Map): MapHandle => ({
  setVisibility: () => {}, setPaint: () => {}, setFeatureState: () => {},
  clearFeatureStates: () => {},
  setSourceData: (id, d) =>
    (m.getSource(id) as { setData?: (x: unknown) => void } | undefined)?.setData?.(d),
});
