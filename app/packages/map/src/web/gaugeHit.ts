/** (T1194) The web's half of the gauge tap: the shared box and pick (`../gauge-hit`) over MapLibre GL JS. */
import type * as maplibregl from "maplibre-gl";
import { GAUGE_TAP_LAYERS, gaugeBox, gaugeHit } from "../gauge-hit";

/** The station of the gauge dot (or its label) under a press at `(x, y)`, or null: none drawn there, or the layers not on the map. */
export function hitGauge(m: maplibregl.Map, x: number, y: number, at: { lat: number; lng: number }): string | null {
  const layers = GAUGE_TAP_LAYERS.filter((l) => m.getLayer(l));
  return layers.length ? gaugeHit(m.queryRenderedFeatures(gaugeBox(x, y), { layers: [...layers] }), [at.lng, at.lat]) : null;
}
