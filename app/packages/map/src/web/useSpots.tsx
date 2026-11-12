/**
 * (T131) YOUR SPOTS on the web map: the marks `spotMarkers` gives at the zoom the camera settled at — (T316) drawn by
 * maplibre-gl as a GeoJSON source with symbol layers (`../spot-marks.ts`), not DOM boxes moved on every `move`. Regrouped
 * when the points, the selection or the zoom change (`zoomend`), never on a pan. A click within a mark's 44 px target is
 * the mark's (`spotUnder` → `pressSpot`), and the map's other click handlers skip it (`hitSpot`): no water opens under
 * it. Returns the marks' buttons for the keyboard and a screen reader (1 px, out of the way; never moved with the map).
 */
import { spotMarkers, spotsInView, spotUnder, type SpotMarker } from "@app/core";
import { useEffect, useRef, useState } from "react";
import type * as maplibregl from "maplibre-gl";
import type { MapProps } from "../map-props";
import { pressSpot, spotsKey } from "../spots";
import { SPOT_SOURCE, spotBoxImage, spotBoxName, spotImages, spotsData, spotsLayers } from "../spot-marks";
import { afterLoad } from "./ready";
import type { WebMapRefs } from "./refs";

type Layer = Parameters<maplibregl.Map["addLayer"]>[0];
const EMPTY = { type: "FeatureCollection" as const, features: [] };

/** The marks each map drew, and the zoom they were placed at: what a click is checked against. */
const placed = new WeakMap<maplibregl.Map, { marks: readonly SpotMarker[]; zoom: number }>();

/** The spot mark under a click at this place, or null — for the map's other click handlers to leave it alone. */
export function hitSpot(m: maplibregl.Map, lat: number, lon: number): SpotMarker | null {
  const p = placed.get(m);
  return p && p.marks.length ? spotUnder(p.marks, lat, lon, p.zoom) : null;
}

export function useSpots({ map, renderer }: WebMapRefs, spots: MapProps["spots"], at: MapProps["at"]) {
  const spotsNow = useRef(spots);
  spotsNow.current = spots;
  const [spotMarks, setSpotMarks] = useState<readonly SpotMarker[]>([]);
  const colours = spots?.colours;
  const colourKey = colours ? `${colours.mark}|${colours.ground}` : "";
  const spotKey = spotsKey(spots?.points, spots?.selected ?? null);
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    let alive = true;
    // `sig`: the marks last handed to React — (T674) the ones on the map, for their buttons (the first regroup always
    // renders once, as it always did: the web map's later hooks first see the map on a render after it is made);
    // `drawn`: the marks last written to the source (an empty set is no change — a map with no spots writes nothing)
    let sig = "";
    let drawn = "[]";
    const regroup = () => {
      const s = spotsNow.current;
      if (!alive) return;
      const zoom = m.getZoom();
      const marks = s ? spotMarkers(s.points, zoom, s.selected) : [];
      placed.set(m, { marks, zoom });
      const next = JSON.stringify(marks);
      // (T674) the buttons a screen reader is given: the marks on the map now (`spotsInView`), never a spot off it
      const c = m.getCenter(), box = m.getContainer();
      const said = spotsInView(marks, { lat: c.lat, lon: c.lng, zoom, width: box.clientWidth, height: box.clientHeight });
      const saidSig = JSON.stringify(said);
      if (saidSig !== sig) { sig = saidSig; setSpotMarks(said); }
      if (next === drawn) return;
      drawn = next;
      const src = m.getSource(SPOT_SOURCE) as { setData?: (d: unknown) => void } | undefined;
      src?.setData?.(s && colours ? spotsData(marks, colours) : EMPTY);
    };
    regroup();
    const undo = afterLoad(m, () => {
      if (!alive) return;
      const s = spotsNow.current;
      if ((!s || !colours) && !m.getSource(SPOT_SOURCE)) return;            // none yet: nothing to add
      if (colours) {
        for (const f of spotImages(colours))
          if (!m.hasImage(f.name)) m.addImage(f.name, f as unknown as ImageData, { pixelRatio: f.pixelRatio });
        const b = spotBoxImage(colours);
        if (!m.hasImage(spotBoxName(colours))) m.addImage(spotBoxName(colours), b as unknown as ImageData, { pixelRatio: b.pixelRatio });
      }
      if (!m.getSource(SPOT_SOURCE)) m.addSource(SPOT_SOURCE, { type: "geojson", data: EMPTY as never });
      for (const l of (colours ? spotsLayers(colours) : []) as Layer[]) {
        if (m.getLayer(l.id)) m.removeLayer(l.id);
        m.addLayer(l);
      }
      drawn = "[]";
      regroup();
    });
    m.on("zoomend", regroup);
    // (T674) a pan or a resize changes which marks are on the map (never which marks there are: `drawn` is unchanged)
    m.on("moveend", regroup);
    m.on("resize", regroup);
    const onClick = (e: maplibregl.MapMouseEvent) => {
      const s = spotsNow.current;
      const hit = s ? hitSpot(m, e.lngLat.lat, e.lngLat.lng) : null;
      if (hit && s) pressSpot(hit, s, (c, o) => renderer.current?.easeTo(c, o));
    };
    m.on("click", onClick);
    return () => {
      alive = false;
      undo();
      m.off("zoomend", regroup);
      m.off("moveend", regroup);
      m.off("resize", regroup);
      m.off("click", onClick);
      placed.delete(m);
      if (drawn !== "[]") (m.getSource(SPOT_SOURCE) as { setData?: (d: unknown) => void } | undefined)?.setData?.(EMPTY);
    };
  }, [spotKey, colourKey, at.atlas, at.basemap, at.glyphs, at.sprite, at.assets, map.current]);  // eslint-disable-line react-hooks/exhaustive-deps
  return spots && spotMarks.length ? (
    <div data-spots="" style={{ position: "absolute", left: 0, top: 0, width: 1, height: 1, overflow: "hidden" }}>
      {spotMarks.map((mk) => (
        <button key={mk.key} type="button" data-spot={mk.key} aria-label={mk.a11y}
                onClick={() => pressSpot(mk, spotsNow.current ?? spots, (c, o) => renderer.current?.easeTo(c, o))}>
          {mk.label ?? ""}
        </button>))}
    </div>) : null;
}
