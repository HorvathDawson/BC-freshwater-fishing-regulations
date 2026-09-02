declare const __DEV__: boolean | undefined;
/**
 * The web map: maplibre-gl, reading PMTiles by byte range.
 *
 * This is the ONLY file in the workspace that imports a map SDK (layers.json), and it is
 * deliberately thin. It creates the map, hands it the composed style, and forwards the
 * adapter's calls. Everything about how water is coloured lives in the generated style and
 * the view definitions — not here — because a colour decided in a component is a colour
 * the native map will not have.
 *
 * Renders a plain <div>, which is correct even inside a react-native-web tree: maplibre
 * needs a real DOM node to attach a canvas to, and on this platform RNW is producing divs
 * anyway. The native renderer is a different file for exactly this reason.
 */
import { useEffect, useRef } from "react";
import * as maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import "maplibre-gl/dist/maplibre-gl.css";
import "./controls.css";
import { baseAdapter } from "./adapters/contract";
import { runtimeStyle } from "./runtime-style";

/** An empty source, so the gauge layers exist before the first feed tick arrives. */
const EMPTY_FC = '{"type":"FeatureCollection","features":[]}';
import type { MapProps } from "./map-props";

/**
 * `pmtiles://` has to be registered on maplibre ONCE per page, not per map. Registering it
 * again on every mount leaks a protocol handler and, worse, the second one wins while the
 * first map is still using it.
 */
let registered = false;
function registerPMTiles() {
  if (registered) return;
  maplibregl.addProtocol("pmtiles", new Protocol().tile);
  registered = true;
}

export function Map({ at, theme, view, modes, groups, initial, data, onPressFeature,
                      onError, onMoved, onMapPoint, highlight, marker, style,
                      gauges, onVisible }: MapProps) {
  const host = useRef<HTMLDivElement | null>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const adapter = useRef(baseAdapter("web"));
  const pin = useRef<maplibregl.Marker | null>(null);

  useEffect(() => {
    if (!host.current) return;
    registerPMTiles();
    const m = new maplibregl.Map({
      container: host.current,
      // `gauges` rides in on the endpoints so the source EXISTS from the first frame;
      // its contents are then replaced imperatively below. Declaring it later would mean
      // rebuilding the whole style every half hour to move a few hundred dots.
      style: runtimeStyle({ ...at, gauges: gauges ?? EMPTY_FC }, theme,
                          modes) as unknown as maplibregl.StyleSpecification,
      center: [initial.lon, initial.lat],
      zoom: initial.zoom,
      attributionControl: { compact: true },
    });

    /**
     * MapLibre's OWN controls, not ours.
     *
     * I had hand-written a zoom stack, a compass and a scale bar. They looked right and
     * none of them worked: the buttons had no handlers wired, and the scale bar was fed a
     * camera that only updated on `moveend` and lived in a ref, so it never re-rendered
     * and never changed. All three are solved problems that ship with the renderer —
     * NavigationControl already does zoom, bearing and pitch with the map's own easing,
     * and ScaleControl recomputes on every frame of a pinch.
     *
     * They are restyled to the design in `controls.css`, which is the part that was
     * actually worth writing.
     */
    m.addControl(new maplibregl.NavigationControl({
      showCompass: true, visualizePitch: false,
    }), "top-right");
    m.addControl(new maplibregl.ScaleControl({ maxWidth: 96, unit: "metric" }), "top-left");
    // A map that fails silently is the worst outcome this app can produce: an empty map
    // looks exactly like water with no regulations on it. Surface every renderer error.
    m.on("error", (e) => onError?.(
      e.error instanceof Error ? e.error : new Error(e.error?.message ?? "map error")));
    map.current = m;
    // Development only: a map is the one component you cannot inspect from the React tree,
    // and every question about it ("where is the camera", "did that source load") needs the
    // instance. Never present in a production bundle.
    if (typeof __DEV__ !== "undefined" && __DEV__)
      (globalThis as { __map?: unknown }).__map = m;
    return () => { m.remove(); map.current = null; };
    // Deliberately keyed on the ARCHIVES only. `theme` and `view` are recoloured by the
    // effect below rather than by rebuilding the map, because remounting would drop the
    // camera and refetch every tile — a theme toggle would look like a crash and a reload.
    // `initial` is a starting camera, not a controlled one; re-reading it here would yank
    // the map back to it on every render.
  }, [at.atlas, at.basemap, at.glyphs, at.sprite]);

  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const apply = () => {
      const handle = {
        setVisibility: (id: string, v: boolean) => {
          if (m.getLayer(id)) m.setLayoutProperty(id, "visibility", v ? "visible" : "none");
        },
        setPaint: (id: string, prop: string, value: unknown) =>
          m.getLayer(id) && m.setPaintProperty(id, prop as never, value as never),
        setFeatureState: (layerId: string, featureId: string, s: Record<string, unknown>) => {
          const src = (m.getLayer(layerId) as { source?: string } | undefined)?.source;
          if (src) m.setFeatureState({ source: src, sourceLayer: layerId, id: featureId }, s);
        },
        clearFeatureStates: (layerId: string) => {
          const src = (m.getLayer(layerId) as { source?: string } | undefined)?.source;
          if (src) m.removeFeatureState({ source: src, sourceLayer: layerId });
        },
      };
      adapter.current.applyView(handle, view, theme);
      // A bad mode or group name is a CALLER bug, and it used to throw from inside this
      // effect — which unmounts the tree and shows a blank screen. Report it and keep the
      // map alive: a map still showing the previous colouring is recoverable, a white
      // screen is not.
      for (const [id, mode] of Object.entries(modes ?? {})) {
        if (!m.getLayer(id)) continue;
        try { adapter.current.setLayerMode(handle, id, mode, theme); }
        catch (e) { onError?.(e instanceof Error ? e : new Error(String(e))); }
      }
      for (const [id, on] of Object.entries(groups ?? {})) {
        try { adapter.current.setGroupVisible(handle, id, on); }
        catch (e) { onError?.(e instanceof Error ? e : new Error(String(e))); }
      }
      for (const [layerId, values] of Object.entries(data ?? {}))
        adapter.current.setData(handle, layerId, values);
    };
    if (m.isStyleLoaded()) apply(); else m.once("load", apply);
  }, [view, modes, theme, data, groups, onError]);

  useEffect(() => {
    const m = map.current;
    if (!m || !onMoved) return;
    // `moveend`, not `move`: one report per gesture rather than one per frame.
    const report = () => {
      const c = m.getCenter();
      onMoved({ lon: c.lng, lat: c.lat, zoom: m.getZoom() });
    };
    m.on("moveend", report);
    return () => { m.off("moveend", report); };
  }, [onMoved]);

  useEffect(() => {
    const m = map.current;
    if (!m || !onMapPoint) return;
    const onTap = (e: maplibregl.MapMouseEvent) => onMapPoint(e.lngLat.lat, e.lngLat.lng);
    m.on("click", onTap);
    return () => { m.off("click", onTap); };
  }, [onMapPoint]);

  useEffect(() => {
    const m = map.current;
    if (!m) return;
    if (!marker) { pin.current?.remove(); pin.current = null; return; }
    if (!pin.current) pin.current = new maplibregl.Marker({ color: "#5F26E0" });
    pin.current.setLngLat([marker.lon, marker.lat]).addTo(m);
    return () => { pin.current?.remove(); pin.current = null; };
  }, [marker?.lat, marker?.lon]);

  useEffect(() => {
    const m = map.current;
    if (!m || !highlight) return;
    const apply = () => {
      const handle = {
        setVisibility: () => {},
        setPaint: () => {},
        setFeatureState: (layerId: string, featureId: string, st: Record<string, unknown>) => {
          const src = (m.getLayer(layerId) as { source?: string } | undefined)?.source;
          if (src) m.setFeatureState({ source: src, sourceLayer: layerId, id: featureId }, st);
        },
        clearFeatureStates: (layerId: string) => {
          const src = (m.getLayer(layerId) as { source?: string } | undefined)?.source;
          // Clear ONLY `selected`. Dropping every feature-state would take the status
          // values with it, so selecting a river would un-colour the map.
          if (src) m.removeFeatureState({ source: src, sourceLayer: layerId }, "selected");
        },
      };
      adapter.current.highlight(handle, [...highlight]);
    };
    if (m.isStyleLoaded()) apply(); else m.once("load", apply);
  }, [highlight]);

  // Which reaches are on screen. Reported after movement settles rather than per frame:
  // a pan fires `idle` once, where `move` fires sixty times a second and would issue a
  // query per frame.
  // The gauge points. A GeoJSON source is replaced wholesale rather than diffed: it is a
  // few hundred dots and they all change together when the feed ticks.
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const apply = () => {
      const src = m.getSource("gauges") as
        { setData?: (d: unknown) => void } | undefined;
      src?.setData?.(JSON.parse(gauges ?? EMPTY_FC));
    };
    if (m.isStyleLoaded()) apply(); else m.once("load", apply);
  }, [gauges]);

  // Held in a ref so a caller passing an inline arrow cannot make this effect thrash: the
  // listener registers once and reads the latest callback when it fires.
  const visibleCb = useRef(onVisible);
  visibleCb.current = onVisible;
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    const report = () => {
      const cb = visibleCb.current;
      if (!cb) return;
      const seen = new Set<string>();
      for (const f of m.queryRenderedFeatures())
        if (f.id !== undefined && f.sourceLayer === "stream") seen.add(String(f.id));
      if (seen.size) cb([...seen]);
    };
    m.on("idle", report);
    // `idle` may already have passed by the time this registers — on a tab switch the map
    // is mounted and settled before the effect runs. Ask once immediately.
    if (m.isStyleLoaded()) report();
    return () => { m.off("idle", report); };
    // ASKING AGAIN WHEN SOMEONE STARTS LISTENING is the whole reason this depends on
    // anything. `onVisible` is undefined on the Map tab and defined on Conditions, and the
    // map is NOT remounted between them — so switching tabs left a settled map that would
    // not fire `idle` again until you panned. Nobody had reported what was on screen, the
    // standings map stayed empty, and every river painted as unmeasured until you nudged
    // it. Keyed on whether a listener exists rather than on its identity, so a caller
    // passing an inline arrow still cannot make this thrash.
  }, [onVisible !== undefined]);

  useEffect(() => {
    const m = map.current;
    if (!m || !onPressFeature) return;
    const onClick = (e: maplibregl.MapMouseEvent) => {
      /**
       * A BOX, not a point.
       *
       * A river is a one-pixel line and a finger is about forty-four. Querying the exact
       * pixel means most taps land on nothing, and the ones that do land feel like luck —
       * which is what "hard to click things" is. 12 px each way is roughly a fingertip at
       * this density and still small enough that two rivers rarely both qualify.
       */
      const T = 12;
      const box: [maplibregl.PointLike, maplibregl.PointLike] = [
        [e.point.x - T, e.point.y - T], [e.point.x + T, e.point.y + T],
      ];
      const layers = adapter.current.layerIds().filter((id) => m.getLayer(id));
      const hits = m.queryRenderedFeatures(box, { layers });

      /**
       * Prefer the SMALLEST water under the finger.
       *
       * Tapping a creek inside a lake inside a management unit returns all three, and
       * taking the first would hand back whichever MapLibre happened to order first —
       * usually the biggest, because the big polygons draw underneath. A person aiming at
       * a creek means the creek.
       */
      const rank = (layerId: string) =>
        layerId === "stream" ? 0 : layerId === "under_lake" ? 1
        : layerId === "lake" || layerId === "wetland" ? 2 : 3;
      const best = hits
        .filter((f) => f.id !== undefined && f.layer?.id)
        .sort((a, b) => rank(a.layer!.id) - rank(b.layer!.id))[0];
      if (best) onPressFeature(best.layer!.id, String(best.id));
    };
    m.on("click", onClick);
    return () => { m.off("click", onClick); };
  }, [onPressFeature]);

  return <div ref={host} style={{ position: "absolute", inset: 0, ...(style as object) }} />;
}
