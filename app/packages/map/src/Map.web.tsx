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
import { useEffect, useMemo, useRef } from "react";
import * as maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import "maplibre-gl/dist/maplibre-gl.css";
import "./controls.css";
import { baseAdapter } from "./adapters/contract";
import { pillImage } from "./pill";
import { hatchImage } from "./hatch";
import { resolveTheme, STYLE_META } from "./style";
import { gaugeDotColour, runtimeStyle } from "./runtime-style";
import { CAMERA_BOUNDS, MAX_ZOOM, MIN_ZOOM } from "@app/core";

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

export function Map({ at, theme, view, modes, groups, initial, data, onPressFeature, chrome,
                      onError, onMoved, onMapPoint, highlight, marker, style,
                      gauges, onVisible, pins }: MapProps) {
  const host = useRef<HTMLDivElement | null>(null);
  const map = useRef<maplibregl.Map | null>(null);
  const adapter = useRef(baseAdapter("web"));
  const pin = useRef<maplibregl.Marker | null>(null);
  const pinned = useRef<maplibregl.Marker[]>([]);
  /** Re-adds the label pill in the current theme. Set once the map exists. */
  const pill = useRef<(() => void) | null>(null);

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
      // The atlas stops at z14 and has nothing below z4; the basemap is clipped to the
      // province. Past either end there is no data, so the map was drawing ragged tile
      // edges and bare paper and calling it a view. See CAMERA_BOUNDS.
      minZoom: MIN_ZOOM,
      maxZoom: MAX_ZOOM,
      maxBounds: CAMERA_BOUNDS as unknown as maplibregl.LngLatBoundsLike,
      attributionControl: { compact: true },
    });

    /*
     * COLLAPSED ON ARRIVAL.
     *
     * `compact: true` makes the attribution collapsible; it does not make it collapsed.
     * MapLibre adds `maplibregl-compact-show` on creation, so the panel opens itself — and
     * once the credits included Environment and Climate Change Canada and the Province
     * alongside OpenStreetMap and Protomaps, that was three lines and 64px of text lying
     * across the bottom of a phone screen, on top of the Layers button.
     *
     * The class is removed rather than the control restyled: the information stays exactly
     * one tap away on the (i), which is what the licences ask for, and the map is a map
     * again. Removed after a frame because MapLibre adds it during its own mount.
     */
    const collapse = () => host.current
      ?.querySelector(".maplibregl-ctrl-attrib")
      ?.classList.remove("maplibregl-compact-show");
    collapse();
    m.once("load", collapse);

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
    /*
     * BOTTOM-RIGHT, above the attribution.
     *
     * It was top-left, which is also where the date pill sits — so the two stacked in the
     * same corner, the scale clipped against the top edge of the screen, and the one thing
     * that qualifies every distance on the map was the least legible thing on it. Bottom
     * corners are where every map a reader has used puts a scale, and the top-left is now
     * the date's alone: WHEN is the app's own question, and it deserves the corner.
     */
    m.addControl(new maplibregl.ScaleControl({ maxWidth: 92, unit: "metric" }), "bottom-right");
    // A map that fails silently is the worst outcome this app can produce: an empty map
    // looks exactly like water with no regulations on it. Surface every renderer error.
    m.on("error", (e) => onError?.(
      e.error instanceof Error ? e.error : new Error(e.error?.message ?? "map error")));
    /**
     * THE GAUGE LABEL'S PILL, as pixels.
     *
     * Added imperatively because a style cannot declare a generated image, and the
     * alternative was forking the basemap's sprite sheet to add one rounded rectangle.
     *
     * REGISTERED HERE, INSIDE THE EFFECT THAT CREATES THE MAP. It used to be its own
     * effect declared above this one, which meant it ran first, found `map.current` still
     * null, returned — and never ran again, because its only dependency was the theme.
     * MapLibre's response to a missing `icon-image` is to draw the text and log a warning,
     * so every label rendered as bare text on the basemap and nothing looked broken.
     *
     * `styleimagemissing` is the belt to that brace: MapLibre fires it the moment a layer
     * asks for an image it does not have, so the pill arrives however the ordering falls.
     */
    const addPill = () => {
      const t = resolveTheme(theme) as Record<string, string>;
      const img = pillImage(t["color.outside"] ?? "#FFFFFF", t["color.line"] ?? "#D9D9D2");
      if (m.hasImage("gauge-pill")) m.removeImage("gauge-pill");
      m.addImage("gauge-pill", img as unknown as ImageData,
                 { pixelRatio: img.pixelRatio, stretchX: img.stretchX,
                   stretchY: img.stretchY, content: img.content });
      /*
       * AND THE HATCHES, on exactly the same rails and for the same reasons. A patterned
       * layer whose image is missing draws NOTHING — not a fallback colour, nothing — so
       * a wetland would silently disappear rather than look wrong, which is the harder
       * bug to notice. `styleimagemissing` below covers whichever ordering we get.
       */
      for (const [layerId, w] of Object.entries(STYLE_META.patterns ?? {})) {
        const id = `${layerId}-hatch`;
        const img2 = hatchImage(t[w.token] ?? "#808080", w.ground, w.stripe, w.darken,
                                w.spacing, w.weight, w.cross);
        if (m.hasImage(id)) m.removeImage(id);
        m.addImage(id, img2 as unknown as ImageData, { pixelRatio: img2.pixelRatio });
      }
    };
    pill.current = addPill;
    m.on("styleimagemissing", (e: { id: string }) => {
      if (e.id === "gauge-pill" || e.id.endsWith("-hatch")) pill.current?.();
    });
    m.on("load", addPill);
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
      // The pill is painted in the theme's paper, so a light one under dark text on a dark
      // map is the exact failure it replaced a halo to avoid. Repainted with everything else.
      pill.current?.();
      adapter.current.applyView(handle, view, theme);
      // A bad mode or group name is a CALLER bug, and it used to throw from inside this
      // effect — which unmounts the tree and shows a blank screen. Report it and keep the
      // map alive: a map still showing the previous colouring is recoverable, a white
      // screen is not.
      // THE WASH follows the stream layer's mode: the Conditions ramp needs a quiet
      // ground, the regulations view wants the map legible as a map. 0.42 rather than the
      // 0.58 it shipped at — enough that roads and landcover stop competing with a 1.4 px
      // coloured line, little enough that a reader can still find the town they launched
      // from. A wash heavy enough to guarantee the ramp is a wash that hides the map.
      if (m.getLayer("basemap-wash"))
        m.setPaintProperty("basemap-wash", "background-opacity",
                           (modes ?? {}).stream === "standing" ? 0.42 : 0);
      // The gauge dots are a runtime layer, so the loop below cannot find them by id —
      // see `gaugeDotColour`. Repainted explicitly, or a switch to temperature leaves the
      // right stations painted on the flow scale.
      if (m.getLayer("gauge-dot"))
        m.setPaintProperty("gauge-dot", "circle-color",
                           gaugeDotColour(theme, (modes ?? {}).gauges) as never);
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

  /**
   * The named points a panel is describing — both ends of a route, typically.
   *
   * Rebuilt wholesale rather than diffed: there are two or three of them and they all move
   * together when the subject changes. Keyed on the COORDINATES rather than on the array,
   * because every render produces a new array and a new marker would blink.
   */
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    for (const mk of pinned.current) mk.remove();
    pinned.current = (pins ?? []).map((p) => {
      const mk = new maplibregl.Marker({ color: p.tone ?? "#5F26E0", scale: 0.72 });
      if (p.title) mk.setPopup(new maplibregl.Popup({ closeButton: false }).setText(p.title));
      return mk.setLngLat([p.lon, p.lat]).addTo(m);
    });
    return () => {
      for (const mk of pinned.current) mk.remove();
      pinned.current = [];
    };
  }, [(pins ?? []).map((p) => `${p.lon},${p.lat},${p.tone ?? ""}`).join("|")]);

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
    /**
     * RETRY UNTIL THE SOURCE IS THERE, rather than once on `load`.
     *
     * `m.once("load", ...)` was the bug: on the first switch to Conditions the map had
     * loaded minutes earlier, so the callback was registered for an event that had already
     * fired and never ran. The dots appeared only after opening a reach and coming back —
     * because THAT remounted the map and baked the data into the style. `styledata` fires
     * on every style change and is the only event that reliably arrives afterwards.
     *
     * The empty collection is written too, and deliberately: leaving Conditions has to
     * clear the dots, and a stale layer of readings over the regulations map is a set of
     * numbers about a question nobody asked.
     */
    let done = false;
    const apply = () => {
      const src = m.getSource("gauges") as
        { setData?: (d: unknown) => void } | undefined;
      if (!src?.setData) return;
      src.setData(JSON.parse(gauges ?? EMPTY_FC));
      done = true;
      m.off("styledata", apply);
    };
    apply();
    if (!done) m.on("styledata", apply);
    return () => { m.off("styledata", apply); };
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
      /*
       * LAKES COUNT AS WATER HERE, and they did not.
       *
       * This reported stream sections only, so the Conditions view asked the bundle about
       * rivers and never about lakes — and every lake stayed grey, including the ones with
       * a gauge sitting in them. A lake section IS a section: `lake_gauge` reaches it by
       * item rather than by reach, but the id the map holds is the same kind of id.
       *
       * `wetland` is deliberately not here. It is context, nothing is gauged in a marsh,
       * and adding 333,526 features to a query bounded by the viewport would be work for
       * an answer that is always "no station".
       */
      const seen = new Set<string>();
      for (const f of m.queryRenderedFeatures())
        if (f.id !== undefined && (f.sourceLayer === "stream" || f.sourceLayer === "lake"))
          seen.add(String(f.id));
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
      /*
       * ONLY LAYERS THAT CAN NAME WHAT WAS HIT.
       *
       * `featureIds` says which tile property a layer promotes into `feature.id`; a layer
       * with no entry has no id to hand back, so a hit on it resolves to nothing. That is
       * every label, and it is the under-lake route — the dotted thread through a lake is
       * the topology's path, not water anybody fishes, and a person aiming at it means the
       * LAKE. The route used to rank SECOND in the order below, so a tap on a lake with a
       * river through it answered with the route.
       *
       * Derived rather than listed so the two cannot drift: dropping a layer's id in the
       * style source takes it out of the tap order in the same commit.
       */
      const layers = adapter.current.layerIds()
        .filter((id) => m.getLayer(id) && STYLE_META.featureIds[id] !== undefined);
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
        layerId === "stream" ? 0
        : layerId === "lake" || layerId === "wetland" ? 1 : 2;
      const best = hits
        .filter((f) => f.id !== undefined && f.layer?.id)
        .sort((a, b) => rank(a.layer!.id) - rank(b.layer!.id))[0];
      // WHERE ON THE FEATURE, not just which feature. A river is hundreds of kilometres
      // long and the answer to "how does this spot reach the gauge" starts at the point
      // under the finger — without it the route panel can mark the station and nothing
      // else. The coordinate is snapped to nothing: it is where the person tapped.
      if (best) onPressFeature(best.layer!.id, String(best.id), e.lngLat.lat, e.lngLat.lng);
    };
    m.on("click", onClick);
    return () => { m.off("click", onClick); };
  }, [onPressFeature]);

  /**
   * THE CONTROLS' COLOURS, from the map's own theme.
   *
   * `controls.css` reads `--map-ink`, `--map-card` and friends, and NOTHING HAD EVER SET
   * THEM — every rule fell through to its light-theme literal, so the dark map wore a white
   * control stack and a light-grey scale bar. Silent, because a fallback in `var()` is
   * indistinguishable from a value that was supplied.
   *
   * Set on the host rather than on `:root`: two maps in one tree (the screen map and a
   * `MiniMap` in a sheet) can be on different themes, and a document-level variable would
   * make the last one to mount win.
   */
  const vars = useMemo(() => (chrome ? {
    "--map-ink": chrome.ink,
    "--map-card": chrome.card,
    "--map-tint": chrome.tint,
    "--map-sub": chrome.sub,
    "--map-shadow": chrome.shadow,
    "--map-icon-filter": chrome.iconFilter,
    "--map-scale-bg": chrome.scaleBg,
  } as React.CSSProperties : {}), [chrome]);

  return <div ref={host}
              style={{ position: "absolute", inset: 0, ...vars, ...(style as object) }} />;
}
