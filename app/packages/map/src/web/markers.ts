/**
 * THE WEB MAP'S DOM MARKERS: the one marker (`marker`), the named points a panel describes (`pins`) and a water's
 * boundary marks (`marks`) — MapLibre markers, in this map's chrome.
 */
import { useEffect, useRef } from "react";
import * as maplibregl from "maplibre-gl";
import type { mapChrome } from "../chrome";
import type { MapMark, MapProps } from "../map-props";
import { labelShift } from "../marks";
import type { WebMapRefs } from "./refs";

type Furniture = ReturnType<typeof mapChrome>;

export function useMarker({ map }: WebMapRefs, marker: MapProps["marker"], furniture: Furniture) {
  const pin = useRef<maplibregl.Marker | null>(null);
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    if (!marker) { pin.current?.remove(); pin.current = null; return; }
    if (!pin.current)
      pin.current = new maplibregl.Marker({ color: furniture.accent });
    pin.current.setLngLat([marker.lon, marker.lat]).addTo(m);
    return () => { pin.current?.remove(); pin.current = null; };
  }, [marker?.lat, marker?.lon]);
}

/**
 * The named points a panel is describing — both ends of a route, typically.
 *
 * Rebuilt wholesale rather than diffed: there are two or three of them and they all move
 * together when the subject changes. Keyed on the COORDINATES rather than on the array,
 * because every render produces a new array and a new marker would blink.
 */
export function usePins({ map }: WebMapRefs, pins: MapProps["pins"], furniture: Furniture) {
  const pinned = useRef<maplibregl.Marker[]>([]);
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    for (const mk of pinned.current) mk.remove();
    pinned.current = (pins ?? []).map((p) => {
      const mk = new maplibregl.Marker({ color: p.tone ?? furniture.accent,
                                        scale: 0.72 });
      if (p.title) mk.setPopup(new maplibregl.Popup({ closeButton: false }).setText(p.title));
      return mk.setLngLat([p.lon, p.lat]).addTo(m);
    });
    return () => {
      for (const mk of pinned.current) mk.remove();
      pinned.current = [];
    };
  }, [(pins ?? []).map((p) => `${p.lon},${p.lat},${p.tone ?? ""}`).join("|")]);
}

/**
 * THE BOUNDARY MARKS (P15): DOM markers, so a ring stays finger-sized at any zoom and a label sits over it — the
 * prototype's `.mk` (a 44 px button, a 16 px ring, the label above). Rebuilt when any mark changes: a water has a few.
 * Each mark's element is made by `markElement` (`Map.web.tsx`: building DOM is the web variant's alone).
 */
export function useMarks({ map }: WebMapRefs, marks: MapProps["marks"], onPressMark: MapProps["onPressMark"],
                         furniture: Furniture,
                         markElement: (k: MapMark, c: { ink: string; card: string }, press: () => void) => HTMLElement) {
  const marked = useRef<maplibregl.Marker[]>([]);
  const pressMark = useRef(onPressMark);
  pressMark.current = onPressMark;
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    for (const mk of marked.current) mk.remove();
    marked.current = (marks ?? []).map((k, i) =>
      new maplibregl.Marker({ element: markElement(k, furniture, () => pressMark.current?.(i)) })
        .setLngLat([k.lon, k.lat]).addTo(m));
    // KEEP EACH LABEL INSIDE THE MAP (the prototype's `--dx`): shifted sideways by what would hang over an edge
    const inside = () => {
      const w = m.getContainer().clientWidth;
      for (const mk of marked.current) {
        const span = mk.getElement().querySelector("span");
        if (span) span.style.transform = `translateX(calc(-50% + ${labelShift(m.project(mk.getLngLat()).x, w)}px))`;
      }
    };
    inside();
    m.on("move", inside);
    return () => {
      m.off("move", inside);
      for (const mk of marked.current) mk.remove();
      marked.current = [];
    };
  }, [JSON.stringify(marks ?? []), furniture.ink, furniture.card]);
}
