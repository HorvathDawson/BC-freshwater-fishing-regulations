/**
 * THE WEB MAP'S POINT MARKS (T317): the one marker (`marker`), the named points a panel describes (`pins`) and a water's
 * boundary marks (`marks`) — GeoJSON sources with layers over the map (`../point-marks.ts`), drawn by maplibre-gl with
 * the map. They were DOM markers, moved by GL JS on every frame, and the marks' labels were measured on every `move`.
 * Taps are the layers': a click hit-tests the marks first (`hitMark`), and the map's other click handlers skip a tap a
 * mark took (it opens no water under it).
 */
import { useEffect, useRef } from "react";
import * as maplibregl from "maplibre-gl";
import type { mapChrome } from "../chrome";
import type { MapProps } from "../map-props";
import { hereImage } from "../here-mark";
import {
  MARK_BOX_IMAGE, MARK_LABEL, MARK_RING, MARK_SOURCE, PIN_ICON, PIN_SOURCE, markBox, markBoxImage, markHit, marksData,
  marksLayers, pinIconLayer, pinsData, shiftedLabels,
  titledPin,
} from "../point-marks";
import { afterLoad } from "./ready";
import type { WebMapRefs } from "./refs";

type Furniture = ReturnType<typeof mapChrome>;
type Layer = Parameters<maplibregl.Map["addLayer"]>[0];
const EMPTY = { type: "FeatureCollection" as const, features: [] };

/** The source with `data`, its layers (re-added: a theme change re-colours them) on top of the map. */
function put(m: maplibregl.Map, source: string, data: unknown, layers: readonly unknown[]) {
  const src = m.getSource(source) as { setData?: (d: unknown) => void } | undefined;
  if (src?.setData) src.setData(data);
  else m.addSource(source, { type: "geojson", data: data as never });
  for (const l of layers as Layer[]) {
    if (m.getLayer(l.id)) m.removeLayer(l.id);
    m.addLayer(l);
  }
}

/** The index of the mark under a click at `(x, y)` (its 44 px target), or null — and null on a map with no marks. */
export function hitMark(m: maplibregl.Map, x: number, y: number): number | null {
  const layers = [MARK_RING, MARK_LABEL].filter((id) => m.getLayer(id));
  return layers.length ? markHit(m.queryRenderedFeatures(markBox(x, y), { layers })) : null;
}

/**
 * The marker and the named points (both ends of a route, typically): one source, the teardrop pin image per tone. A pin
 * with a title says it on a click (GL JS's popup, as the DOM marker's was). Keyed on the coordinates and tones, not on
 * the array: every render makes a new one.
 */
export function usePins({ map }: WebMapRefs, marker: MapProps["marker"], pins: MapProps["pins"], furniture: Furniture) {
  const pinsNow = useRef(pins);
  pinsNow.current = pins;
  const key = `${marker ? `${marker.lon},${marker.lat}` : ""}|${(pins ?? []).map((p) => `${p.lon},${p.lat},${p.tone ?? ""},${p.title ?? ""}`).join("|")}`;
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    return afterLoad(m, () => {
      const data = pinsData(marker, pinsNow.current, furniture.accent);
      if (!data.features.length && !m.getLayer(PIN_ICON)) return;     // none yet: nothing to add
      for (const f of data.features as { properties: { tone: string; img: string } }[]) {
        if (m.hasImage(f.properties.img)) continue;
        const g = hereImage(f.properties.tone, furniture.card);
        m.addImage(f.properties.img, g as unknown as ImageData, { pixelRatio: g.pixelRatio });
      }
      put(m, PIN_SOURCE, data, [pinIconLayer()]);
    });
  }, [key, furniture.accent, furniture.card, map.current]);
  // a titled pin's words on a click
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const onClick = (e: maplibregl.MapMouseEvent) => {
      if (!m.getLayer(PIN_ICON)) return;
      const { x, y } = e.point;
      const p = titledPin(m.queryRenderedFeatures([[x - 12, y - 12], [x + 12, y + 12]], { layers: [PIN_ICON] }), pinsNow.current);
      if (p) new maplibregl.Popup({ closeButton: false }).setLngLat([p.lon, p.lat]).setText(p.title).addTo(m);
    };
    m.on("click", onClick);
    return () => { m.off("click", onClick); };
  }, [map.current]);
  useEffect(() => () => {
    const m = map.current;
    (m?.getSource(PIN_SOURCE) as { setData?: (d: unknown) => void } | undefined)?.setData?.(EMPTY);
  }, []);
}

/**
 * THE BOUNDARY MARKS (P15): a 16 px ring (filled when open) and its label above it in a box, the prototype's `.mk`, now
 * map layers. A tap within 22 px of a mark is the mark's. EACH LABEL KEPT INSIDE THE MAP (the prototype's `--dx`): the
 * label's point is moved sideways by what would hang over an edge, measured where the camera SETTLES (`moveend`), never
 * per frame.
 */
export function useMarks({ map }: WebMapRefs, marks: MapProps["marks"], onPressMark: MapProps["onPressMark"],
                         furniture: Furniture) {
  const pressMark = useRef(onPressMark);
  pressMark.current = onPressMark;
  const key = JSON.stringify(marks ?? []);
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const ms = marks ?? [];
    let alive = true;
    let last = "";
    const place = () => {
      if (!alive) return;
      void shiftedLabels(ms, m.getContainer().clientWidth,
                         (lon, lat) => { const p = m.project({ lng: lon, lat }); return [p.x, p.y]; },
                         (x, y) => { const l = m.unproject([x, y]); return [l.lng, l.lat]; })
        .then((at) => {
          const data = marksData(ms, at);
          const sig = JSON.stringify(at);
          if (!alive || sig === last) return;
          last = sig;
          (m.getSource(MARK_SOURCE) as { setData?: (d: unknown) => void } | undefined)?.setData?.(data);
        });
    };
    const undo = afterLoad(m, () => {
      if (!alive || (!ms.length && !m.getLayer(MARK_RING))) return;
      if (m.hasImage(MARK_BOX_IMAGE)) m.removeImage(MARK_BOX_IMAGE);
      const g = markBoxImage(furniture.card, furniture.ink);
      m.addImage(MARK_BOX_IMAGE, g as unknown as ImageData,
                 { pixelRatio: g.pixelRatio, stretchX: g.stretchX, stretchY: g.stretchY, content: g.content });
      put(m, MARK_SOURCE, marksData(ms), marksLayers(furniture, MARK_BOX_IMAGE));
      place();
    });
    if (ms.some((k) => k.label)) m.on("moveend", place);
    const onClick = (e: maplibregl.MapMouseEvent) => {
      const i = hitMark(m, e.point.x, e.point.y);
      if (i !== null) pressMark.current?.(i);
    };
    m.on("click", onClick);
    return () => {
      alive = false;
      undo();
      m.off("moveend", place);
      m.off("click", onClick);
      (m.getSource(MARK_SOURCE) as { setData?: (d: unknown) => void } | undefined)?.setData?.(EMPTY);
    };
  }, [key, furniture.ink, furniture.card, map.current]);
}
