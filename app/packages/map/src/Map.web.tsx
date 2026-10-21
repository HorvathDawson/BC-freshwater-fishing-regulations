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
import { dayOfYear, type SectionKey } from "@app/core";
import { useEffect, useMemo, useRef, useState } from "react";
import * as maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import "maplibre-gl/dist/maplibre-gl.css";
import "./controls.css";
import { baseAdapter } from "./adapters/contract";
import { pillImage } from "./pill";
import { hatchImage } from "./hatch";
import { STYLE_META } from "./style";
import { colour, mapChrome } from "./chrome";
import { runtimeStyle } from "./runtime-style";
import { applyLook } from "./look";
import { webRenderer, type WebRenderer } from "./adapters/web";
import type { MapRenderer } from "./adapters/contract";
import { followSize, syncSize } from "./size";
import { toExpression } from "./legacy-filter";
import { compassShown, keyboardRotation, maplibreGestures, touchRotation } from "./gestures";
import { CAMERA_BOUNDS, CAMERA_MAX_ZOOM, FIT_MAX_ZOOM, MIN_ZOOM, bboxOfGeometry, padBbox, refineFit,
         unionBbox, type Bbox } from "@app/core";
import type { MapHandle } from "./adapters/contract";
import { hereImage } from "./here-mark";
import { labelShift } from "./marks";
import { EMPTY_TRACE, HERE_IMAGE, tracePaint } from "./trace-overlay";
import { collectionBbox, traceOverlay } from "./trace-geometry";
import { Colouring, LABEL_SOURCE_LAYERS, standingInput, statusInput, withoutEngineKeys, type LabelTarget,
         type TileRenderer,
         type Channel, type ChannelInput, type TileSections } from "./colouring";
import { sourceTiles, tileIds } from "./tile-ids.web";

/** An empty source, so the gauge layers exist before the first feed tick arrives. */
const EMPTY_FC = '{"type":"FeatureCollection","features":[]}';
import type { MapMark, MapProps } from "./map-props";

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

/**
 * The outside-BC mask, fetched once for the whole session.
 *
 * The `outside` FILL layer fetches this itself, from the same URL, as a normal GeoJSON
 * source. This second copy exists because the basemap's labels are confined to the province
 * with a `within` filter, and a MapLibre expression cannot reference a source — it needs the
 * polygon as a literal. Module-level and keyed by URL, so a theme toggle or a remount does
 * not refetch 61 KB, and a failure just leaves the labels as they were.
 */
const outsideCache: Record<string, unknown> = {};
function useOutsideMask(url: string | undefined): unknown {
  const [got, setGot] = useState<unknown>(() => (url ? outsideCache[url] : undefined));
  useEffect(() => {
    if (!url || url in outsideCache) return;
    let live = true;
    fetch(url).then((r) => (r.ok ? r.json() : null)).then((j) => {
      if (!j || !live) return;
      outsideCache[url] = j;
      setGot(j);
    }).catch(() => {});
    return () => { live = false; };
  }, [url]);
  return got;
}

/**
 * The maps whose style has loaded once. `isStyleLoaded()` is ALSO false while tiles load (GL JS counts the sources),
 * but paint, layout and feature-state calls need only the style itself, which never unloads here (the style is set once
 * per map). Without this a zoom out to the basin field — a long tile burst, exactly when the basin standings arrive —
 * waited on a `styledata` that never came, and the field stayed blank (N2 review web walk).
 */
const styledOnce = new WeakSet<maplibregl.Map>();

/**
 * Run `fn` once the style can take layout and feature-state calls — now, or as soon as it
 * can. Returns the cleanup.
 *
 * NOT `isStyleLoaded() ? fn() : once("load", fn)`, the pattern this file uses elsewhere and
 * the one HANDOFF-ui.md records biting twice: `isStyleLoaded()` is briefly false on a map
 * that loaded long ago (every `setFilter` and tile burst), and `load` never fires again —
 * so the call was simply lost. `styledata` keeps arriving, so waiting on it cannot miss.
 */
function whenStyled(m: maplibregl.Map, fn: () => void): () => void {
  if (m.isStyleLoaded()) { fn(); return () => {}; }
  const h = () => {
    if (!m.isStyleLoaded()) return;
    m.off("styledata", h); m.off("load", h);
    fn();
  };
  m.on("styledata", h); m.on("load", h);
  return () => { m.off("styledata", h); m.off("load", h); };
}

/**
 * Run `fn` once the route overlay's sources exist — now, almost always.
 *
 * NOT `whenStyled`: a GeoJSON source takes `setData` whenever it exists, and
 * `isStyleLoaded()` is false for as long as ANY tile is loading — a route map whose lines
 * arrived mid-burst waited for a `styledata` that a settled map never sends again, and the
 * route was never drawn (measured: the pins landed, the lines did not).
 */
function whenTraced(m: maplibregl.Map, fn: () => void): () => void {
  if (m.getSource("trace")) { fn(); return () => {}; }
  const h = () => {
    if (!m.getSource("trace")) return;
    m.off("styledata", h);
    fn();
  };
  m.on("styledata", h);
  return () => { m.off("styledata", h); };
}

/**
 * Run `fn` now if this map's style has loaded ONCE, else on its `load` — for the look and the data. Not `whenStyled`:
 * its `isStyleLoaded()` also waits out every tile burst, and a `styledata` may never come; and not before `load`,
 * where the pill and hatches are added (adding them earlier raced the load handler: "gauge-pill already exists").
 */
function afterLoad(m: maplibregl.Map, fn: () => void): () => void {
  if (styledOnce.has(m)) { fn(); return () => {}; }
  m.once("load", fn);
  return () => { m.off("load", fn); };
}

/** Development only: where the colouring's cost is recorded, for the colour-latency measurements. */
type ColourCost = { t: number; ms: number; calls: number; kind: string };
const devCost = (c: ColourCost) => {
  if (typeof __DEV__ !== "undefined" && __DEV__)
    ((globalThis as { __colourCost?: ColourCost[] }).__colourCost ??= []).push(c);
};
/** Development only: take the public route even where 5.24's internals are there — to measure what it costs. */
const forcePublic = () => typeof __DEV__ !== "undefined" && __DEV__
  && (globalThis as { __colourPublic?: boolean }).__colourPublic === true;

/**
 * Feature-state on the atlas, by source-layer: the target the profile colouring writes to. The labels go straight to
 * the map (a section's label is set once per index, so the renderer's de-dupe has nothing to add); the release swap's
 * clear goes THROUGH the renderer, so its sink forgets what it held there and the highlight put back is really sent.
 */
const atlasTarget = (m: maplibregl.Map, r: WebRenderer): LabelTarget => ({
  setState: (sourceLayer, id, s) => m.setFeatureState({ source: "atlas", sourceLayer, id }, s),
  clearAll: (sourceLayer) => r.clearSourceLayer("atlas", sourceLayer),
});

/**
 * THE PUBLIC ROUTE to the sections the map holds: `querySourceFeatures` per source-layer, over every tile it has
 * rendered. Heavier than reading one tile's ids (`tile-ids.web.ts`), and blind to tiles in the out-of-view cache, but it
 * uses nothing private — it is what runs when 5.24's internals are not there.
 */
function publicSections(m: maplibregl.Map): TileSections {
  const out: Record<string, number[]> = {};
  for (const layer of LABEL_SOURCE_LAYERS) {
    const ids: number[] = [];
    for (const f of m.querySourceFeatures("atlas", { sourceLayer: layer }))
      if (typeof f.id === "number") ids.push(f.id);
    out[layer] = ids;
  }
  return out;
}

/**
 * The adapter's handle on this map's layers: paint, visibility, zoom range, and feature-state through the renderer
 * (`r.states`, N2) — clearing there removes only `selected`, per feature, and here nothing is lit, so a mode change
 * never takes the profile labels with it.
 */
function layerHandle(m: maplibregl.Map, r: MapRenderer): MapHandle {
  return {
    ...r.states(() => []),
    /*
     * THE ZOOM A LAYER TAKES IN THIS VIEW — see `minzoomByView`. The Conditions view
     * stands the rivers down below z9 so the basin field can answer instead; every other
     * view resets them to the style's own floor, which is why this is called on every
     * view change and not once at load.
     */
    setZoomRange: (id: string, minzoom: number, maxzoom?: number) => {
      if (!m.getLayer(id)) return;
      const spec = m.getLayer(id) as { maxzoom?: number };
      m.setLayerZoomRange(id, minzoom, maxzoom ?? spec.maxzoom ?? 24);
    },
    setVisibility: (id: string, v: boolean) => {
      if (m.getLayer(id)) m.setLayoutProperty(id, "visibility", v ? "visible" : "none");
    },
    setPaint: (id: string, prop: string, value: unknown) => {
      if (m.getLayer(id)) m.setPaintProperty(id, prop as never, value as never);
    },
  };
}

/** Run `fn` once the atlas source exists — the moment feature-state and paint calls can land on it. */
function whenAtlas(m: maplibregl.Map, fn: () => void): () => void {
  if (m.getSource("atlas")) { fn(); return () => {}; }
  const h = () => {
    if (!m.getSource("atlas")) return;
    m.off("styledata", h);
    fn();
  };
  m.on("styledata", h);
  return () => { m.off("styledata", h); };
}

export function Map({ at, theme, view, modes, groups, hide, show, initial, data, onPressFeature,
                     chrome,
                      onError, onMoved, onMapPoint, highlight, water, marker, style,
                      gauges, onVisible, pins, bare, fit, basemap, trace,
                      zoomControls, status, standing, marks, onPressMark }: MapProps) {
  const host = useRef<HTMLDivElement | null>(null);
  const map = useRef<maplibregl.Map | null>(null);
  /**
   * The contract's renderer over this map (`adapters/web.ts`): feature-state, taps, the visible set, the fit — the
   * same calls `Map.native.tsx` makes of its own, which `parity.test.ts` holds to the same command log.
   */
  const renderer = useRef<WebRenderer | null>(null);
  const adapter = useRef(baseAdapter("web"));
  /** The profile colouring of this map (colour-latency AP) — made with the map, fed by its tiles' load events. */
  const colouring = useRef<Colouring | null>(null);
  const pin = useRef<maplibregl.Marker | null>(null);
  const pinned = useRef<maplibregl.Marker[]>([]);
  /** Re-adds the label pill in the current theme. Set once the map exists. */
  const pill = useRef<(() => void) | null>(null);
  // Confines the BASEMAP's labels to British Columbia — see `useOutsideMask`. Absent on the
  // first frame and present a moment later, which is why it is in the restyle effect below
  // rather than only in the constructor.
  const outsideData = useOutsideMask(at.outside);
  /** MapLibre's own controls and markers, in this theme's chrome unless told otherwise. */
  const furniture = useMemo(() => chrome ?? mapChrome(theme), [chrome, theme]);

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
      // The atlas has nothing below z4 and the basemap is clipped to the province: past
      // either there is no data, only ragged tile edges and bare paper. See CAMERA_BOUNDS.
      // IN, the camera goes past the tiles (z18) and MapLibre overzooms each source from
      // its own deepest zoom, which the source declares (runtime-style) — CAMERA_MAX_ZOOM.
      minZoom: MIN_ZOOM,
      maxZoom: CAMERA_MAX_ZOOM,
      maxBounds: CAMERA_BOUNDS as unknown as maplibregl.LngLatBoundsLike,
      attributionControl: bare ? false : { compact: true },
      // G1: one gesture policy for both maps — pan, pinch, double-tap and wheel zoom; no rotate, no tilt; a press
      // that moves 8 px or more is a pan, never a tap (`@app/core` MAP_GESTURES)
      ...maplibreGestures(),
    });
    if (!touchRotation()) m.touchZoomRotate.disableRotation();
    if (!keyboardRotation()) m.keyboard.disableRotation();

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
    // See MapProps.bare — an illustration, not something you drive.
    // A bare map may still ask for the zoom buttons (`zoomControls`) — never the compass.
    if (!bare || zoomControls)
      m.addControl(new maplibregl.NavigationControl({
        // the compass turns the map when dragged; with rotation off north is always up (G1)
        showCompass: compassShown() && !bare, visualizePitch: false,
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
    if (!bare)
      m.addControl(new maplibregl.ScaleControl({ maxWidth: 92, unit: "metric" }),
                   "bottom-right");
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
      // The pill's border read `color.line`, which was never a token, so every theme drew
      // the light theme's hairline from a literal fallback. It is the app's hairline now.
      const img = pillImage(colour(theme, "color.outside"), colour(theme, "color.ui.line"));
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
        const img2 = hatchImage(colour(theme, w.token), w.ground, w.stripe, w.darken,
                                w.spacing, w.weight, w.cross);
        if (m.hasImage(id)) m.removeImage(id);
        m.addImage(id, img2 as unknown as ImageData, { pixelRatio: img2.pixelRatio });
      }
      // AND THE "YOU ARE HERE" PIN of the route overlay — the accent on this theme's paper.
      const here = hereImage(colour(theme, "color.ui.accent"), colour(theme, "color.outside"));
      if (m.hasImage(HERE_IMAGE)) m.removeImage(HERE_IMAGE);
      m.addImage(HERE_IMAGE, here as unknown as ImageData, { pixelRatio: here.pixelRatio });
    };
    pill.current = addPill;
    m.on("styleimagemissing", (e: { id: string }) => {
      if (e.id === "gauge-pill" || e.id === HERE_IMAGE || e.id.endsWith("-hatch"))
        pill.current?.();
    });
    m.on("load", addPill);
    m.once("load", () => styledOnce.add(m));
    map.current = m;
    const r0 = webRenderer(m as never, FIT_MAX_ZOOM);
    renderer.current = r0;
    /*
     * EACH TILE COLOURED IN ITS OWN LOAD EVENT (colour-latency AP).
     *
     * GL JS fires `sourcedata` with the tile right after initialising the tile's feature-state and before it is ever
     * drawn; state set here is applied in the next frame's source update, so the tile's FIRST frame wears its status.
     * The status used to arrive after `idle` (every tile in, the 300 ms symbol fade over) and a React round trip —
     * half a second of grey on every new tile. Synchronous, no React, ids already seen skipped: about 0.1 ms a pan.
     *
     * The ids come from 5.24's internals (`tile-ids.web.ts`, pinned by a test). Where they are not there, the PUBLIC
     * route: one `querySourceFeatures` scan per burst of tile events, in a microtask — still before the next frame —
     * and on `moveend` as well, because the public route cannot see a cached tile coming back into view.
     */
    const c = new Colouring(adapter.current);
    colouring.current = c;
    const target = atlasTarget(m, r0);
    let scanQueued = false;
    const scan = (kind: string) => {
      scanQueued = false;
      if (!c.active || !m.getSource("atlas")) return;
      const t0 = performance.now();
      const calls = c.label(target, publicSections(m));
      devCost({ t: t0, ms: performance.now() - t0, calls, kind });
    };
    const onTile = (e: maplibregl.MapSourceDataEvent & { tile?: unknown }) => {
      if (e.sourceId !== "atlas" || !e.tile || !c.active) return;
      const ids = forcePublic() ? null : tileIds(e.tile, LABEL_SOURCE_LAYERS);
      if (ids === null) {
        if (!scanQueued) { scanQueued = true; queueMicrotask(() => scan("public-tile")); }
        return;
      }
      const t0 = performance.now();
      const calls = c.label(target, ids);
      devCost({ t: t0, ms: performance.now() - t0, calls, kind: "tile" });
    };
    const onMoveEnd = () => {
      if (c.active && (forcePublic() || sourceTiles(m, "atlas") === null)) scan("public-moveend");
    };
    m.on("sourcedata", onTile);
    m.on("moveend", onMoveEnd);
    // The canvas follows its container, not the window (see size.ts): a box that settles after mount — search's
    // preview map on the way back from the eye — would otherwise keep the first size and every fit would frame it.
    const unfollow = followSize(m, () => refit.current?.());
    // Development only: a map is the one component you cannot inspect from the React tree,
    // and every question about it ("where is the camera", "did that source load") needs the
    // instance. Never present in a production bundle.
    if (typeof __DEV__ !== "undefined" && __DEV__)
      (globalThis as { __map?: unknown }).__map = m;
    return () => {
      m.off("sourcedata", onTile); m.off("moveend", onMoveEnd);
      unfollow(); m.remove(); map.current = null; colouring.current = null; renderer.current = null;
    };
    // Deliberately keyed on the ARCHIVES only. `theme` and `view` are recoloured by the
    // effect below rather than by rebuilding the map, because remounting would drop the
    // camera and refetch every tile — a theme toggle would look like a crash and a reload.
    // `initial` is a starting camera, not a controlled one; re-reading it here would yank
    // the map back to it on every render.
  }, [at.atlas, at.basemap, at.glyphs, at.sprite, at.assets]);

  /**
   * Confine the BASEMAP's labels to British Columbia, once the mask has arrived.
   *
   * WHY NOT A PROPERTY FILTER. Overpass can select by province because it queries the OSM
   * database, where the admin relations live. This basemap is a pre-built Protomaps archive,
   * and a MapLibre filter can only read properties the TILE carries: `places` ships
   * `kind`, `name`, `population`, `wikidata` and forty translations, and nothing that says
   * which country a label is in, let alone which province. So the test has to be geometric.
   *
   * The mask is a rectangle with the province punched out, so "inside the mask" IS "outside
   * BC" and the filter is that, negated. Applied here rather than in the constructor because
   * the 61 KB arrives a moment after the first frame; the layers are found by source rather
   * than by a hardcoded list, so a Protomaps upgrade that adds a label layer gets it too.
   */
  useEffect(() => {
    const m = map.current;
    if (!m || !outsideData) return;
    const apply = () => {
      for (const l of m.getStyle()?.layers ?? []) {
        if (l.type !== "symbol") continue;
        if ((l as { source?: string }).source !== "basemap") continue;
        const had = (l as { filter?: unknown[] }).filter;
        const inBC = ["!", ["within", outsideData]] as unknown[];
        /*
         * CONVERT BEFORE COMBINING. Protomaps writes most of these in the LEGACY filter
         * syntax (`["in", "kind", "river", "stream"]`), and `all` is a valid operator in
         * both syntaxes — so `["all", <expression>, <legacy>]` is ambiguous and MapLibre
         * resolves it as legacy, where `!` is not an operator:
         *
         *   layers.roads_labels_major.filter[1][0]: expected one of
         *   [==, !=, >, >=, <, <=, in, !in, all, any, none, has, !has], "!" found
         *
         * `toExpression` converts a legacy filter and returns an expression untouched, so
         * it runs on all of them rather than sniffing which syntax each layer uses.
         */
        const kept = had ? toExpression(had) : null;
        // Keep the layer's own kind filter; this narrows, it does not replace.
        m.setFilter(l.id, (kept ? ["all", inBC, kept] : inBC) as never);
      }
    };
    if (m.isStyleLoaded()) apply(); else m.once("load", apply);
  }, [outsideData]);

  /**
   * THE LOOK, KEYED ON THE LOOK ALONE: its CONTENT, not its props' identities. A caller hands fresh-but-equal objects on
   * every render (the Map tab's `hide={[]}`, a horizon change's new view model), and each one re-ran the whole pass —
   * 139 `setPaintProperty` per horizon click, every one a no-op (engine polish, 2026-10-21; one-engine.render.test
   * "A HORIZON CHANGE").
   */
  const lookKey = JSON.stringify([view, theme, modes ?? null, groups ?? null, hide ?? null, show ?? null,
                                  basemap ?? "map"]);
  const lookNow = useRef({ view, theme, modes, groups, hide, show, basemap });
  lookNow.current = { view, theme, modes, groups, hide, show, basemap };
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r) return;
    const apply = () => {
      const { view, theme, modes, groups, hide, show, basemap } = lookNow.current;
      const handle = layerHandle(m, r);
      // The pill is painted in the theme's paper, so a light one under dark text on a dark
      // map is the exact failure it replaced a halo to avoid. Repainted with everything else.
      pill.current?.();
      // THE LOOK — view, wash, gauge scale, per-layer modes, hidden layers, groups, the ground — is one sequence
      // shared with the native map (`applyLook`); a bad mode or group name is reported, never thrown (a blank screen
      // is not recoverable, a map in its previous colouring is). The ground is here, not in an effect of its own
      // behind `whenStyled`, which dropped a toggle made mid tile burst (one-engine.render.test).
      applyLook(adapter.current, handle, (id) => m.getLayer(id) !== undefined,
                { view, theme, modes, hide, show, groups, basemap: basemap ?? "map" },
                (e) => lookError.current?.(e));
      // The route overlay's paper and ink: runtime layers outside the catalogue, web only (the overlay lives here).
      for (const [id, paint] of Object.entries(tracePaint(theme)))
        if (m.getLayer(id))
          for (const [prop, value] of Object.entries(paint))
            m.setPaintProperty(id, prop as never, value as never);
    };
    // `afterLoad`, not `isStyleLoaded() ? apply() : once("load", apply)`: mid tile burst (a zoom out to the basin
    // field) `isStyleLoaded()` is false on a map that loaded long ago and `load` never fires again (N2 review web walk)
    return afterLoad(m, apply);
    // NOT keyed on `onError`: callers pass an inline arrow, so every render of the screen re-ran the whole look — ~800
    // paint calls, each a no-op — on every feed refresh (the Conditions measurement, 2026-10-21). A ref, below.
  }, [lookKey]);
  /*
   * PUSHED DATA, apart from the look: feature-state a mode reads that no engine channel sets. A section's status or
   * standing is NEVER pushed on the web — those are the engine's channels (one pathway, tools/one-colouring-engine);
   * a push of either is refused and reported (`withoutEngineKeys`), so a second pathway cannot creep back in.
   */
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r || !data) return;
    return afterLoad(m, () => {
      const handle = layerHandle(m, r);
      for (const [layerId, values] of Object.entries(withoutEngineKeys(data, (e) => lookError.current?.(e))))
        adapter.current.setData(handle, layerId, values);
    });
  }, [data]);
  const lookError = useRef(onError);
  lookError.current = onError;

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
    if (!pin.current)
      pin.current = new maplibregl.Marker({ color: furniture.accent });
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

  /**
   * The ids lit `selected` last, and apart from them the ids lit `water` (P15) — cleared by id on the next highlight,
   * and what a release swap puts back (below). Kept apart, and `wasLit` meaning what it meant before P15 (the selected
   * ids only), so the one colouring engine's `restore` (below) can never put the water's
   * other parts back as `selected`; the restore re-applies both.
   */
  const wasLit = useRef<SectionKey[]>([]);
  const wasWater = useRef<SectionKey[]>([]);
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r || !highlight) return;
    // `whenStyled`, not `once("load")`: a search lights a water on a map that loaded long
    // ago, often mid-tile-burst, and a missed `load` meant the river never lit.
    const apply = () => {
      // Clears ONLY the highlight's keys, per feature, from the ids this map lit last time — see `stateHandle`.
      const handle = r.states(() => [...wasLit.current, ...wasWater.current]);
      adapter.current.highlight(handle, [...highlight], [...(water ?? [])]);
      wasLit.current = [...highlight];
      wasWater.current = [...(water ?? [])];
    };
    return whenStyled(m, apply);
  }, [highlight, water]);

  /**
   * THE BOUNDARY MARKS (P15): DOM markers, so a ring stays finger-sized at any zoom and a label sits over it — the
   * prototype's `.mk` (a 44 px button, a 16 px ring, the label above). Rebuilt when any mark changes: a water has a few.
   */
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

  /**
   * THE CHANNELS (the colouring engine, `colouring.ts`): the day's status and the Conditions standings go to ONE
   * engine, which labels the tiles already loaded and installs the values. New values are one `Colouring.set`: a day is
   * one paint property (the day is in the style); a feed refresh or a horizon relabels the loaded tiles' sections
   * (feature-state only). New labels, or none, are the release-swap sequence in `Colouring.set` — and what it clears that
   * is not ours (the selected highlight, the active mode's pushed data) is put back here.
   */
  const latestData = useRef(data);
  latestData.current = data;
  const latestTheme = useRef(theme);
  latestTheme.current = theme;
  /** The tiles this map holds, and what a clear must put back — the same for every channel. */
  const tileRenderer = (m: maplibregl.Map, r: WebRenderer): TileRenderer => ({
    loaded: () => {
      const held = forcePublic() ? null : sourceTiles(m, "atlas");
      const read = held?.map((t) => tileIds(t, LABEL_SOURCE_LAYERS));
      return read && read.every((x) => x !== null) ? read as TileSections[] : [publicSections(m)];
    },
    restore: () => {
      adapter.current.highlight(r.states(() => []), [...wasLit.current], [...wasWater.current]);
      for (const [layerId, values] of Object.entries(withoutEngineKeys(latestData.current ?? {}, () => {})))
        adapter.current.setData(layerHandle(m, r), layerId, values);
    },
  });
  /** One channel's input, to the engine, once the atlas exists — the one path both channels take. */
  const toChannel = (channel: Channel, input: ChannelInput | null, kind: string) => {
    const m = map.current, c = colouring.current, r = renderer.current;
    if (!m || !c || !r) return;
    return whenAtlas(m, () => {
      const t0 = performance.now(), c0 = c.calls;
      c.set(layerHandle(m, r), atlasTarget(m, r), tileRenderer(m, r), channel, input, latestTheme.current);
      devCost({ t: t0, ms: performance.now() - t0, calls: c.calls - c0, kind });
    });
  };
  const statusDay = status ? dayOfYear(status.day) : 0;
  useEffect(() => toChannel("status", status ? statusInput(status) : null, `status:${statusDay}`),
            [status?.index, statusDay, at.atlas]);
  useEffect(() => toChannel("standing", standing ? standingInput(standing) : null, "standing"),
            [standing?.labels, standing?.values, at.atlas]);


  /**
   * SEND THE CAMERA to a subject, then let the water's own shape frame it.
   *
   * The box comes from the bundle's evidence (gauges, stocking sites, town distances) and
   * is not an outline — the Chilliwack's gauges are all on its lower half. So after each
   * move settles, the `highlight` features the tiles have actually delivered are measured
   * and the camera re-fits to them while `refineFit` says there is more to see. Bounded
   * rounds, so a river that runs off every tile set cannot walk the camera forever.
   *
   * The highlight is read through a ref: it arrives with the same subject, and a new array
   * for the same water must not restart the fit.
   */
  const lit = useRef(highlight);
  lit.current = highlight;
  /** Re-frame the current subject after the container changed size — unless the reader has moved off it. */
  const refit = useRef<(() => void) | null>(null);
  /** The drawn route overlay's extent, once its tiles are in — see the trace effects below. */
  const traceBox = useRef<Bbox | null>(null);
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r || !fit) return;
    let rounds = 0;
    let alive = true;
    let last: Bbox | null = null;
    let moved = false;
    // a pan or zoom by the READER (it carries the DOM event); our own fitBounds does not
    const touched = (e: { originalEvent?: unknown }) => { if (e.originalEvent) moved = true; };
    m.on("movestart", touched);
    refit.current = () => { if (alive && last && !moved) go(last); };
    const go = (b: Bbox) => {
      last = b;
      // measure the box BEFORE framing it: a stale canvas centres the water on a size nobody sees
      syncSize(m);
      const el = m.getContainer();
      const pad = Math.round(Math.min(56, el.clientWidth * 0.12, el.clientHeight * 0.12));
      r.fitBounds(b, { padding: Math.max(pad, 8), duration: 450 });
    };
    const done = () => { alive = false; m.off("movestart", touched); };
    // A route overlay already drawn is part of the subject: "fit" frames the routes' own
    // shapes as well as their ends, so a river that bends outside its gauges stays on screen.
    const withTrace = (b: Bbox | null) =>
      (traceBox.current ? unionBbox(b, traceBox.current) : b);
    const first = fit.bbox ? withTrace([...fit.bbox] as Bbox) : null;
    if (first) go(first);
    if (!fit.refine) return done;
    const measure = async () => {
      const ids = (lit.current ?? []).map(Number).filter(Number.isFinite);
      if (!alive || !ids.length) return;
      const found = await r.waterBox(ids);
      const next = refineFit(await r.bounds(), found, rounds);
      if (!alive) return;
      if (next) { rounds += 1; go(padBbox(next, 0.4)); }
      else m.off("idle", measure);
    };
    m.on("idle", measure);
    return () => { done(); m.off("idle", measure); };
  }, [fit?.key]);

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

  /**
   * THE ROUTE OVERLAY — see `trace-overlay.ts` and `trace-geometry.ts`.
   *
   * TWO EFFECTS, two lifetimes. The LINES are copied out of the atlas tiles when the routes
   * change (`trace.key`) and at no other time — a horizon change re-labels the gauges and
   * must not refetch a single tile. The PINS follow every change of their own content (a
   * share moves with the horizon), and land at once: the reader sees where the gauges are
   * while their routes are still being read.
   */
  const traceHandle = (m: maplibregl.Map): MapHandle => ({
    setVisibility: () => {}, setPaint: () => {}, setFeatureState: () => {},
    clearFeatureStates: () => {},
    setSourceData: (id, d) =>
      (m.getSource(id) as { setData?: (x: unknown) => void } | undefined)?.setData?.(d),
  });
  const tracePins = useRef(trace?.pins ?? []);
  tracePins.current = trace?.pins ?? [];
  const pinKey = JSON.stringify(trace?.pins ?? []);
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    return whenTraced(m, () =>
      adapter.current.setTrace(traceHandle(m), tracePins.current, null));
  }, [pinKey]);

  useEffect(() => {
    const m = map.current;
    if (!m) return;
    let alive = true;
    traceBox.current = null;
    // The previous route goes the moment the subject changes, never lingering under a new one.
    // ONE PENDING WRITE AT A TIME: a clear still waiting on `styledata` when the lines arrive
    // must be cancelled, or it fires afterwards and wipes the route it was clearing the way for.
    let pending = whenTraced(m, () =>
      adapter.current.setTrace(traceHandle(m), tracePins.current, EMPTY_TRACE));
    const routes = (trace?.routes ?? []).filter((r) => r.path.length > 0);
    if (routes.length) {
      traceOverlay(at.atlas, routes).then(({ fc, missing, fetched, tiles, ms }) => {
        if (!alive) return;
        traceBox.current = collectionBbox(fc);
        pending();
        pending = whenTraced(m, () =>
          adapter.current.setTrace(traceHandle(m), tracePins.current, fc));
        // Development only: the measured cost, for the record and for the screenshots' README.
        if (typeof __DEV__ !== "undefined" && __DEV__)
          ((globalThis as { __traceCost?: unknown[] }).__traceCost ??= []).push(
            { key: trace?.key, sections: new Set(routes.flatMap((r) => r.path)).size,
              missing: missing.length, fetched, tiles, ms, features: fc.features.length });
      }).catch((e) => onError?.(e instanceof Error ? e : new Error(String(e))));
    }
    return () => { alive = false; pending(); };
  }, [trace?.key, at.atlas]);

  // Held in a ref so a caller passing an inline arrow cannot make this effect thrash: the
  // listener registers once and reads the latest callback when it fires.
  const visibleCb = useRef(onVisible);
  visibleCb.current = onVisible;
  useEffect(() => {
    const m = map.current, r = renderer.current;
    if (!m || !r) return;
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
      /*
       * THE ID IS NOT STRINGIFIED, and it used to be.
       *
       * `String(f.id)` was harmless while a section id WAS a string. It is an integer
       * handle now, and the bundle answers with a Map keyed by that integer — so a
       * stringified id looked up nothing, `standings` came back empty, and the Conditions
       * map painted every river as unmeasured. No error anywhere: a Map miss is a
       * `undefined`, and "no reading" is a legitimate answer this app draws on purpose.
       *
       * A Set of numbers de-duplicates exactly as well as a Set of strings, so passing the
       * id through unchanged costs nothing and removes the conversion that could be wrong.
       */
      // `VISIBLE_LAYERS` (shared with the native map): streams, lakes, and the lake/wetland polygons a stream flows
      // through — a slough drawn as a wetland polygon is a stream (`wetland_flowing`), and coloured
      void r.visible().then((seen) => { if (seen.length) visibleCb.current?.(seen); });
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
    const m = map.current, r = renderer.current;
    if (!m || !r || !onPressFeature) return;
    const onClick = (e: maplibregl.MapMouseEvent) => {
      /**
       * A BOX, not a point.
       *
       * A river is a one-pixel line and a finger is about forty-four. Querying the exact
       * pixel means most taps land on nothing, and the ones that do land feel like luck —
       * which is what "hard to click things" is. 12 px each way is roughly a fingertip at
       * this density and still small enough that two rivers rarely both qualify.
       */
      /*
       * ONLY THE SECTION LAYERS, IN `TAP_GROUPS` ORDER (`MapRenderer.press`, shared with the native map).
       *
       * A label, the under-lake route (the topology's path through a lake, not water anybody fishes: a person aiming
       * at it means the LAKE) and the area layers name no section, so they are not asked — every caller turns the id
       * into a SECTION (`Number(id)`), and a park's `area_id` used to come back as NaN. The stream group comes
       * first — a river and the polygons a stream flows through — then still water: tapping a creek inside a lake
       * returns both, and a person aiming at a creek means the creek. The first argument is the group ("stream" or
       * "lake"), the same on both maps: a native feature does not say which style layer it came from.
       */
      const { lat, lng } = e.lngLat;
      void r.press(e.point.x, e.point.y).then((hit) => {
        // WHERE ON THE FEATURE, not just which feature. A river is hundreds of kilometres
        // long and the answer to "how does this spot reach the gauge" starts at the point
        // under the finger — without it the route panel can mark the station and nothing
        // else. The coordinate is snapped to nothing: it is where the person tapped.
        if (hit) onPressFeature(hit.layer, hit.id, lat, lng);
      }).catch((err: unknown) => onError?.(err instanceof Error ? err : new Error(String(err))));
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
  const vars = useMemo(() => ({
    "--map-ink": furniture.ink,
    "--map-card": furniture.card,
    "--map-tint": furniture.tint,
    "--map-sub": furniture.sub,
    "--map-shadow": furniture.shadow,
    "--map-icon-filter": furniture.iconFilter,
    "--map-scale-bg": furniture.scaleBg,
  } as React.CSSProperties), [furniture]);

  return <div ref={host}
              style={{ position: "absolute", inset: 0, ...vars, ...(style as object) }} />;
}

/**
 * One boundary mark's element: a 44 px button holding a 16 px ring (filled when open), its label above it. The ROOT
 * is a bare wrapper GL JS owns: `.maplibregl-marker{position:absolute;top:0;left:0}` places it and a translate moves
 * it. Nothing inline may set its `position` — an inline `relative` beat the class and stacked the marks in normal
 * flow, each drawn 44 px × its index below its point (P15 review). The button inside is the label's containing block.
 */
function markElement(k: MapMark, c: { ink: string; card: string }, press: () => void): HTMLElement {
  const root = document.createElement("div");
  const b = document.createElement("button");
  root.appendChild(b);
  b.type = "button";
  b.setAttribute("aria-label", k.a11y);
  b.setAttribute("data-mark", k.on ? "on" : "");
  b.style.cssText = "width:44px;height:44px;padding:0;border:0;background:transparent;display:flex;"
    + "align-items:center;justify-content:center;cursor:pointer;position:relative";
  const ring = document.createElement("i");
  ring.style.cssText = `width:16px;height:16px;border-radius:50%;box-sizing:border-box;border:3px solid ${c.ink};`
    + `background:${k.on ? c.ink : c.card}`;
  b.appendChild(ring);
  if (k.label) {
    const t = document.createElement("span");
    t.textContent = k.label;
    t.style.cssText = "position:absolute;bottom:38px;left:50%;transform:translateX(-50%);max-width:180px;"
      + `width:max-content;padding:3px 7px;background:${c.card};color:${c.ink};border:1.5px solid ${c.ink};`
      + "font:700 12px/1.25 Archivo,sans-serif;white-space:normal;pointer-events:none";
    b.appendChild(t);
  }
  b.addEventListener("click", (e) => { e.stopPropagation(); press(); });
  return root;
}
