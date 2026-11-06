/**
 * (T303) THE READER'S OWN LOCATION on the web map: the blue dot and its accuracy circle, drawn from the browser's reading
 * (`MapProps.me.fix`), centred when asked and kept in view while following. (T317) The phone's GeoJSON point and two
 * circle layers (`../me.ts`), added over the map — not a DOM marker moved every frame, and the circle's size is a
 * zoom expression in the layer, not a style write on every `zoom` event.
 */
import { useEffect, useRef } from "react";
import type * as maplibregl from "maplibre-gl";
import type { MapProps } from "../map-props";
import { cameraMs } from "../motion";
import { ME_SOURCE, meData, mePaint } from "../me";
import { afterLoad } from "./ready";
import type { WebMapRefs } from "./refs";

/** the zoom a centre goes in to when the map is further out than this: street scale */
const CENTRE_ZOOM = 15;
const ME_ACCURACY = "me-accuracy", ME_DOT = "me-dot";

export function useMe({ map }: WebMapRefs, me: MapProps["me"], theme: string) {
  const lastCentre = useRef(0);
  const release = useRef(me?.onRelease);
  release.current = me?.onRelease;
  const fix = me?.on ? me.fix : null;

  // the dot and its circle
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    return afterLoad(m, () => {
      if (!fix && !m.getLayer(ME_DOT)) return;     // never on: nothing to add
      const data = JSON.parse(meData(fix)) as unknown;
      const src = m.getSource(ME_SOURCE) as { setData?: (d: unknown) => void } | undefined;
      if (src?.setData) src.setData(data);
      else m.addSource(ME_SOURCE, { type: "geojson", data: data as never });
      const look = mePaint(theme, fix);
      for (const [id, paint] of [[ME_ACCURACY, look.accuracy], [ME_DOT, look.dot]] as const) {
        if (m.getLayer(id)) for (const [k, v] of Object.entries(paint)) m.setPaintProperty(id, k, v);
        else m.addLayer({ id, type: "circle", source: ME_SOURCE, paint: paint as never } as maplibregl.LayerSpecification);
      }
    });
  }, [fix?.lat, fix?.lon, fix?.accuracy, theme, map.current]);

  // centre on each press; while following, on each new reading
  useEffect(() => {
    const m = map.current;
    if (!m || !fix || !me) return;
    const pressed = me.centre !== lastCentre.current;
    lastCentre.current = me.centre;
    if (!pressed && !me.following) return;
    m.easeTo({ center: [fix.lon, fix.lat], zoom: pressed && me.zoom != null ? me.zoom : Math.max(m.getZoom(), CENTRE_ZOOM),
               duration: pressed ? cameraMs(500) : cameraMs(300), essential: false });
  }, [me?.centre, me?.following, fix?.lat, fix?.lon]);

  // the reader's own drag stops following (a programmatic ease has no originalEvent)
  useEffect(() => {
    const m = map.current;
    if (!m || !me?.following) return;
    const stop = (e: { originalEvent?: unknown }) => { if (e.originalEvent) release.current?.(); };
    m.on("dragstart", stop);
    return () => { m.off("dragstart", stop); };
  }, [me?.following, map.current]);
}
