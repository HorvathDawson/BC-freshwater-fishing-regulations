/**
 * (T812) THE CLOSED-AREA CIRCLES on the web map (`../salmon-circles.ts`): one GeoJSON source and its three layers, under
 * the streams. The data follows the salmon facts, the day, the legend's "only" and the stream's mode; the layers follow
 * the theme. Re-added on `styledata` if a style change dropped them (a theme switch rebuilds the style).
 */
import { useEffect, useRef } from "react";
import type * as maplibregl from "maplibre-gl";
import type { MapProps } from "../map-props";
import { SALMON_CIRCLE_BEFORE, SALMON_CIRCLE_SOURCE, salmonCircleLayers, salmonCirclesData } from "../salmon-circles";
import { afterLoad } from "./ready";
import type { WebMapRefs } from "./refs";

type Layer = Parameters<maplibregl.Map["addLayer"]>[0];

export function useSalmonCircles({ map }: WebMapRefs, salmon: MapProps["salmon"], modes: MapProps["modes"], theme: string) {
  const data = salmonCirclesData(salmon, modes?.stream);
  const dataNow = useRef(data);
  dataNow.current = data;
  const key = JSON.stringify(data.features.map((f) => (f as { properties: unknown }).properties));
  // the layers, in this theme: added once the style has loaded, and again whenever a style change has dropped them
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    let alive = true;
    const ensure = () => {
      if (!alive || !m.isStyleLoaded()) return;
      if (!m.getSource(SALMON_CIRCLE_SOURCE)) m.addSource(SALMON_CIRCLE_SOURCE, { type: "geojson", data: dataNow.current as never });
      const before = m.getLayer(SALMON_CIRCLE_BEFORE) ? SALMON_CIRCLE_BEFORE : undefined;
      for (const l of salmonCircleLayers(theme) as Layer[]) {
        if (m.getLayer(l.id)) continue;
        m.addLayer(l, before);
      }
    };
    const undo = afterLoad(m, ensure);
    m.on("styledata", ensure);
    return () => {
      alive = false;
      undo();
      m.off("styledata", ensure);
      for (const l of salmonCircleLayers(theme)) if (m.getLayer(l.id)) m.removeLayer(l.id);
    };
  }, [theme, map.current]);  // eslint-disable-line react-hooks/exhaustive-deps
  // the data: the day's circles, or none outside the Salmon view
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const apply = () => {
      const src = m.getSource(SALMON_CIRCLE_SOURCE) as { setData?: (d: unknown) => void } | undefined;
      if (!src?.setData) return false;
      src.setData(dataNow.current);
      return true;
    };
    if (apply()) return;
    const h = () => { if (apply()) m.off("styledata", h); };
    m.on("styledata", h);
    return () => { m.off("styledata", h); };
  }, [key, map.current]);  // eslint-disable-line react-hooks/exhaustive-deps
}
