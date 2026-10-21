/**
 * The native map: MapLibre Native through `@maplibre/maplibre-react-native` 11.5.0 (N1 → N2), behind the SAME `<Map>`
 * contract as `Map.web.tsx` (`./Map.d.ts`, `MapProps`).
 *
 * WHAT IS SHARED WITH THE WEB MAP, and therefore cannot drift:
 *   the style        `runtimeStyle()` (via `nativeStyle`): the same layers, paint, tokens and view logic, through the
 *                    same adapter and the same `applyLook` sequence. Only the URLs differ: `pmtiles://file:///…`
 *                    and `file://` glyphs/sprites from the installed release (P11), where the web has the release URL.
 *   feature-state    status, standing and `selected`, through the same `stateHandle` (the layer's own source-layer,
 *                    the section's NUMBER id). The id is the tiles' MVT feature id: MapLibre Native ignores
 *                    `promoteId`, so the native style has none (N1 §2). The area layers keep property styling.
 *   taps             the same 24 px box (`tapBox`), the same order (`TAP_GROUPS`: streams first), the id read by the
 *                    same `firstSection` — Android hands it back as a STRING ("311274", N1 §3); this is where it
 *                    becomes the number the app keys everything on.
 *   gestures         `nativeMapGestures()` — G1's one policy: pan, pinch, double-tap zoom; no rotate, no tilt, no
 *                    compass. The 8 px TAP SLOP is MapLibre Native's own here: the binding exposes no setting, its
 *                    gesture detector (the system touch slop on Android) decides what is a tap and what is a pan.
 *
 * WHAT IS DIFFERENT, because of the binding:
 *   - The style is handed over whole (`mapStyle`), so a theme, view or layer-mode change is a STYLE SWAP (≈ 225 ms,
 *     N1) and a swap drops every feature-state: status and selection are re-applied each time a style finishes
 *     loading (`gen`). Gauge ticks do NOT swap the style: they go to the style's own `gauges` source.
 *   - The binding mounts its children (the atlas `VectorSource`, which attaches to the style's existing `atlas`
 *     source by id) only once the map is ready: refs, not copies, and nothing is pushed before both are there.
 *   - Every SDK call is async across the bridge. A press event's `nativeEvent` is GONE after the first `await`
 *     (measured, N1), so it is read first, always.
 *   - No runtime images: the hatches and the gauge pill (`addImage` on the web) are not drawn — see `nativeStyle`.
 *   - Markers are native view annotations (`Marker`), not maplibre-gl's DOM markers.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { View, type LayoutChangeEvent } from "react-native";
import {
  Camera, GeoJSONSource, Map as MLMap, Marker, VectorSource,
  type CameraRef, type MapRef, type VectorSourceRef,
} from "@maplibre/maplibre-react-native";
import { CAMERA_BOUNDS, MAX_ZOOM, MIN_ZOOM, bboxOfGeometry, padBbox, refineFit, unionBbox, type Bbox,
         type SectionKey } from "@app/core";
import { baseAdapter, type MapHandle } from "./adapters/contract";
import { mapChrome } from "./chrome";
import { nativeMapGestures } from "./gestures";
import { nativeStyle } from "./native-style";
import { TAP_GROUPS, VISIBLE_LAYERS, firstSection, stateHandle, tapBox, visibleSections } from "./sections";
import type { MapProps } from "./map-props";

/** An empty source, so the gauge layers exist before the first feed tick arrives (as on the web). */
const EMPTY_FC = '{"type":"FeatureCollection","features":[]}';
/** The source-layers a searched water can be drawn in — what `fit.refine` measures. */
const WATER_SOURCE_LAYERS = ["stream", "lake", "wetland"] as const;
/** `CAMERA_BOUNDS` (`[[w, s], [e, n]]`) in the binding's flat `[w, s, e, n]`. */
const MAX_BOUNDS: [number, number, number, number] =
  [CAMERA_BOUNDS[0][0], CAMERA_BOUNDS[0][1], CAMERA_BOUNDS[1][0], CAMERA_BOUNDS[1][1]];

type PressEvent = { nativeEvent: { point: [number, number] | { x: number; y: number };
                                   lngLat: [number, number] } | null };
type RegionEvent = { nativeEvent: { center?: [number, number]; zoom?: number } | null };

/** Feature-state on the atlas source, through the handle both maps share. Failures are reported once per pass. */
function atlasStates(src: VectorSourceRef, lit: () => readonly SectionKey[], report: (e: unknown) => void): MapHandle {
  let failed = false;
  const fail = (e: unknown) => { if (!failed) { failed = true; report(e); } };
  return stateHandle({
    set: (t, id, s) => {
      if (t.source !== "atlas") return;
      void src.setFeatureState({ id, sourceLayer: t.sourceLayer }, s as never).catch(fail);
    },
    remove: (t, id, key) => {
      if (t.source !== "atlas") return;
      void src.removeFeatureState({ id, sourceLayer: t.sourceLayer }, key).catch(fail);
    },
  }, lit);
}

export function Map({ at, theme, view, modes, groups, hide, initial, data, onPressFeature, chrome, onError,
                      onMoved, onMapPoint, highlight, marker, style, gauges, onVisible, pins, bare, fit,
                      basemap }: MapProps) {
  const adapter = useRef(baseAdapter("native"));
  const mapRef = useRef<MapRef | null>(null);
  const camRef = useRef<CameraRef | null>(null);
  const atlasRef = useRef<VectorSourceRef | null>(null);
  /** The map's size, for the fit padding (the web reads its container). */
  const size = useRef({ width: 0, height: 0 });
  /** A style has finished loading at least once. */
  const [styled, setStyled] = useState(false);
  /** Bumped when a style finishes loading (a swap dropped every feature-state) or the atlas source attaches. */
  const [gen, setGen] = useState(0);
  const furniture = useMemo(() => chrome ?? mapChrome(theme), [chrome, theme]);

  // Callbacks through refs: the SDK handlers are registered once, and an inline arrow from a caller must not
  // re-register them (or, for the style, swap it).
  const cb = useRef({ onError, onMoved, onMapPoint, onPressFeature, onVisible });
  cb.current = { onError, onMoved, onMapPoint, onPressFeature, onVisible };
  const report = useCallback((e: unknown) =>
    cb.current.onError?.(e instanceof Error ? e : new Error(String(e))), []);

  /*
   * THE STYLE. Keyed on what changes how the map LOOKS — never on `gauges` (read through a ref: a feed tick goes to
   * the `gauges` source below, not into a 225 ms style swap) and never on a caller's new-but-equal objects.
   */
  const gaugesNow = useRef(gauges);
  gaugesNow.current = gauges;
  const look = useMemo(
    () => nativeStyle({ at, theme, view, modes, hide, groups, basemap: basemap ?? "map",
                        gauges: gaugesNow.current ?? EMPTY_FC }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [at.atlas, at.basemap, at.glyphs, at.sprite, at.assets, at.outside, theme, view, JSON.stringify(modes ?? {}),
     JSON.stringify(hide ?? []), JSON.stringify(groups ?? {}), basemap]);
  useEffect(() => { for (const e of look.errors) report(e); }, [look, report]);

  const gaugeData = useMemo(() => JSON.parse(gauges ?? EMPTY_FC) as GeoJSON.FeatureCollection, [gauges]);
  const camera0 = useRef({ center: [initial.lon, initial.lat] as [number, number], zoom: initial.zoom });

  // The FIRST attach is news (the binding mounts the source only once the map is ready, maybe after the style
  // loaded); later ones are the binding re-creating its handle (`useImperativeHandle` with no deps), which must not
  // re-render this map, or each render would re-attach and render again. A style swap is `onStyle`'s business.
  const attachedOnce = useRef(false);
  const atlasAttached = useCallback((r: VectorSourceRef | null) => {
    atlasRef.current = r;
    if (r && !attachedOnce.current) { attachedOnce.current = true; setGen((g) => g + 1); }
  }, []);

  // STATUS COLOURS (and every other per-feature value a colour mode reads), by feature-state
  useEffect(() => {
    const src = atlasRef.current;
    if (!styled || !src) return;
    const h = atlasStates(src, () => [], report);
    for (const [layerId, values] of Object.entries(data ?? {})) {
      try { adapter.current.setData(h, layerId, values); } catch (e) { report(e); }
    }
  }, [data, gen, styled, report]);

  // THE SELECTION: only `selected` is cleared, per feature, from the ids lit last time
  const wasLit = useRef<SectionKey[]>([]);
  useEffect(() => {
    const src = atlasRef.current;
    if (!styled || !src || !highlight) return;
    adapter.current.highlight(atlasStates(src, () => wasLit.current, report), [...highlight]);
    wasLit.current = [...highlight];
  }, [highlight, gen, styled, report]);

  /** Which sections are on screen — after movement settles, never per frame. */
  const reportVisible = useCallback(async () => {
    const m = mapRef.current;
    if (!cb.current.onVisible || !m) return;
    try {
      const seen = visibleSections(await m.queryRenderedFeatures({ layers: [...VISIBLE_LAYERS] }));
      if (seen.length) cb.current.onVisible?.(seen);
    } catch (e) { report(e); }
  }, [report]);
  // asked once as soon as someone listens and the style is there: a settled map does not move again by itself
  useEffect(() => { if (styled) void reportVisible(); }, [styled, onVisible !== undefined, reportVisible]);

  /*
   * SEND THE CAMERA to a subject, then let the water's own shape frame it (as on the web): after each move settles,
   * the `highlight` features the tiles have delivered are measured and the camera re-fits while `refineFit` says
   * there is more to see. Bounded rounds. Waits for the camera to exist: the binding mounts it with the map.
   */
  const lit = useRef(highlight);
  lit.current = highlight;
  const refine = useRef<(() => Promise<void>) | null>(null);
  const fitted = useRef<string | null>(null);
  useEffect(() => {
    const cam = camRef.current;
    if (!fit || !cam || fitted.current === fit.key) return;
    fitted.current = fit.key;
    let rounds = 0;
    let alive = true;
    const go = (b: Bbox) => {
      const { width, height } = size.current;
      const p = Math.max(8, Math.round(Math.min(56, (width || 400) * 0.12, (height || 800) * 0.12)));
      cam.fitBounds([b[0], b[1], b[2], b[3]], { padding: { top: p, right: p, bottom: p, left: p }, duration: 450 });
    };
    if (fit.bbox) go(fit.bbox);
    if (!fit.refine) { refine.current = null; return () => { alive = false; }; }
    refine.current = async () => {
      const src = atlasRef.current, m = mapRef.current;
      const ids = (lit.current ?? []).map(Number).filter(Number.isFinite);
      if (!alive || !src || !m || !ids.length) return;
      let found: Bbox | null = null;
      for (const sourceLayer of WATER_SOURCE_LAYERS)
        for (const f of await src.querySourceFeatures({ sourceLayer, filter: ["in", ["id"], ["literal", ids]] }))
          found = unionBbox(found, bboxOfGeometry(f.geometry as never));
      const next = refineFit((await m.getBounds()) as Bbox, found, rounds);
      if (!alive) return;
      if (next) { rounds += 1; go(padBbox(next, 0.4)); } else refine.current = null;
    };
    return () => { alive = false; refine.current = null; };
  }, [fit?.key, styled]);

  /** A tap: the 24 px box, streams first, the section as a number. */
  const onPress = useCallback(async (e: PressEvent) => {
    // READ THE EVENT BEFORE ANY await: React Native recycles it, and `nativeEvent` is null afterwards (N1 §3)
    const ne = e.nativeEvent;
    if (!ne) return;
    const [x, y] = Array.isArray(ne.point) ? ne.point : [ne.point.x, ne.point.y];
    const [lon, lat] = ne.lngLat;
    cb.current.onMapPoint?.(lat, lon);
    const press = cb.current.onPressFeature;
    const m = mapRef.current;
    if (!press || !m) return;
    const box = tapBox(x, y);
    try {
      for (const g of TAP_GROUPS) {
        const id = firstSection(await m.queryRenderedFeatures(box, { layers: [...g.layers] }));
        if (id !== null) { press(g.as, id, lat, lon); return; }
      }
    } catch (err) { report(err); }
  }, [report]);

  const onRegionDidChange = useCallback((e: RegionEvent) => {
    const ne = e.nativeEvent;          // before anything async, as with a press
    if (ne?.center && ne.zoom !== undefined)
      cb.current.onMoved?.({ lon: ne.center[0], lat: ne.center[1], zoom: ne.zoom });
    void reportVisible();
    void refine.current?.().catch(report);
  }, [reportVisible, report]);

  const onStyle = useCallback(() => { setStyled(true); setGen((g) => g + 1); }, []);
  const onLayout = useCallback((e: LayoutChangeEvent) => {
    size.current = { width: e.nativeEvent.layout.width, height: e.nativeEvent.layout.height };
  }, []);

  return (
    <MLMap
      ref={mapRef}
      style={[{ flex: 1 }, style]}
      mapStyle={look.style as never}
      // G1: one gesture policy for both maps — pan, pinch, double-tap zoom; no rotate, no tilt, no compass
      {...nativeMapGestures()}
      // a bare map is an illustration: no furniture. Attribution goes with it (the page credits the data, MapProps.bare)
      attribution={!bare}
      logo={false}
      scaleBar={!bare}
      onLayout={onLayout}
      onPress={onPress as never}
      onRegionDidChange={onRegionDidChange as never}
      onDidFinishLoadingStyle={onStyle}
      // an empty map and a map whose tiles failed look the same: say so
      onDidFailLoadingMap={() => report(new Error("map failed to load"))}
    >
      <Camera ref={camRef} initialViewState={camera0.current} minZoom={MIN_ZOOM} maxZoom={MAX_ZOOM}
              maxBounds={MAX_BOUNDS} />
      <VectorSource id="atlas" ref={atlasAttached} />
      <GeoJSONSource id="gauges" data={gaugeData} />
      {marker ? (
        <Marker id="marker" lngLat={[marker.lon, marker.lat]}>
          <View style={dot(furniture.accent, furniture.card, 16)} />
        </Marker>
      ) : null}
      {(pins ?? []).map((p, i) => (
        <Marker key={`${i}:${p.lon},${p.lat}`} id={`pin-${i}`} lngLat={[p.lon, p.lat]}>
          <View style={dot(p.tone ?? furniture.accent, furniture.card, 12)} accessibilityLabel={p.title} />
        </Marker>
      ))}
    </MLMap>
  );
}

/** A marker's dot: the colour with a ring in the theme's card colour, so it reads on water and on land. */
function dot(colour: string, ring: string, d: number) {
  return { width: d, height: d, borderRadius: d / 2, backgroundColor: colour, borderWidth: 2,
           borderColor: ring } as const;
}
