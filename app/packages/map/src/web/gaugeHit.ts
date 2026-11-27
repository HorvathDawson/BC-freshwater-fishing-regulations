/** (T1194) The web's half of the gauge tap: the shared boxes and pick (`../gauge-hit`) over MapLibre GL JS (synchronous queries). */
import type * as maplibregl from "maplibre-gl";
import { GAUGE_DOT_LAYER, GAUGE_PILL_LAYER, dotBox, gaugeHit, pillBox } from "../gauge-hit";

/** The station of the gauge pill at, or dot within 22 px of, a press at `(x, y)`, or null: none there, or the layers not on the map. */
export function hitGauge(m: maplibregl.Map, x: number, y: number, at: { lat: number; lng: number }): string | null {
  const ask = (box: ReturnType<typeof dotBox>, layer: string) => (m.getLayer(layer) ? m.queryRenderedFeatures(box, { layers: [layer] }) : []);
  return gaugeHit(ask(pillBox(x, y), GAUGE_PILL_LAYER), ask(dotBox(x, y), GAUGE_DOT_LAYER), [at.lng, at.lat]);
}
