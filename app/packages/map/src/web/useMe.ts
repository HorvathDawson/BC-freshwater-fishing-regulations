/**
 * (T303) THE READER'S OWN LOCATION on the web map: the blue dot and its accuracy circle, drawn from the browser's reading
 * (`MapProps.me.fix`), centred when asked and kept in view while following. One DOM marker (as the pins are), so a style
 * change cannot drop it; the circle is sized in pixels from metres at the map's zoom, on every zoom.
 */
import { useEffect, useRef } from "react";
import * as maplibregl from "maplibre-gl";
import type { MapProps } from "../map-props";
import { colour, translucent } from "../chrome";
import { cameraMs } from "../motion";
import type { WebMapRefs } from "./refs";

/** the zoom a centre goes in to when the map is further out than this: street scale */
const CENTRE_ZOOM = 15;

/** Metres per CSS pixel at a latitude and zoom (512 px tiles). */
export const metresPerPixel = (lat: number, zoom: number): number => 78271.51696 * Math.cos((lat * Math.PI) / 180) / 2 ** zoom;

export function useMe({ map }: WebMapRefs, me: MapProps["me"], theme: string) {
  const mark = useRef<{ marker: maplibregl.Marker; circle: HTMLElement } | null>(null);
  const lastCentre = useRef(0);
  const release = useRef(me?.onRelease);
  release.current = me?.onRelease;
  const fix = me?.on ? me.fix : null;

  // the dot and its circle
  useEffect(() => {
    const m = map.current;
    if (!m || !fix) { mark.current?.marker.remove(); mark.current = null; return; }
    if (!mark.current) {
      // the map's own document (this folder is the web map's; MapLibre has no other)
      const doc = m.getContainer().ownerDocument;
      const blue = colour(theme, "color.ui.me"), ring = colour(theme, "color.ui.me-ring");
      const shade = translucent(theme, "color.ui.shadow", "opacity.ui.shadow");
      const el = doc.createElement("div");
      el.setAttribute("data-testid", "my-location");
      el.setAttribute("role", "img");
      el.setAttribute("aria-label", "Your location");
      el.style.cssText = "width:18px;height:18px;pointer-events:none;position:relative";
      const circle = doc.createElement("div");
      circle.setAttribute("data-testid", "my-location-accuracy");
      circle.style.cssText = `position:absolute;left:50%;top:50%;border-radius:50%;background:${blue}22;border:1px solid ${blue}66;transform:translate(-50%,-50%)`;
      const dot = doc.createElement("div");
      dot.style.cssText = `position:absolute;inset:0;border-radius:50%;background:${blue};border:3px solid ${ring};box-sizing:border-box;box-shadow:0 0 3px ${shade}`;
      el.append(circle, dot);
      mark.current = { marker: new maplibregl.Marker({ element: el, anchor: "center" }), circle };
    }
    const { marker, circle } = mark.current;
    marker.setLngLat([fix.lon, fix.lat]).addTo(m);
    const size = () => {
      const px = fix.accuracy ? Math.min(fix.accuracy / metresPerPixel(fix.lat, m.getZoom()), 4000) : 0;
      const d = Math.max(px * 2, 0);
      circle.style.width = circle.style.height = `${d}px`;
      circle.style.display = d > 18 ? "block" : "none";    // smaller than the dot: the dot says it
    };
    size();
    m.on("zoom", size);
    return () => { m.off("zoom", size); };
  }, [fix?.lat, fix?.lon, fix?.accuracy, map.current]);
  useEffect(() => () => { mark.current?.marker.remove(); mark.current = null; }, []);

  // centre on each press; while following, on each new reading
  useEffect(() => {
    const m = map.current;
    if (!m || !fix || !me) return;
    const pressed = me.centre !== lastCentre.current;
    lastCentre.current = me.centre;
    if (!pressed && !me.following) return;
    m.easeTo({ center: [fix.lon, fix.lat], zoom: Math.max(m.getZoom(), CENTRE_ZOOM),
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
