/**
 * The native map: MapLibre Native through `@maplibre/maplibre-react-native` 11.5.0 (N1 → N2), behind the SAME `<Map>`
 * contract as `Map.web.tsx` (`./Map.d.ts`, `MapProps`).
 *
 * WHAT IS SHARED WITH THE WEB MAP, and therefore cannot drift:
 *   the style        `runtimeStyle()` (via `nativeStyle`): the same layers, paint, tokens and view logic, through the
 *                    same adapter and the same `applyLook` sequence. Only the URLs differ: `pmtiles://file:///…`
 *                    and `file://` glyphs/sprites from the installed release (P11), where the web has the release URL.
 *   the renderer     `adapters/native.ts` (`MapRenderer`), the contract's platform half, held by `parity.test.ts` to
 *                    the web renderer's command log: feature-state (status, standing, `selected`) on the layer's own
 *                    source-layer by the section's NUMBER id — the tiles' MVT feature id, because MapLibre Native
 *                    ignores `promoteId` (N1 §2) — taps through the same 24 px box and `TAP_GROUPS` order, the id
 *                    made a number by `sectionIdOf` (Android hands it back as a STRING, "311274", N1 §3), the
 *                    visible set, the fit.
 *   area values      a layer keyed by a property (the Conditions basin field, `basin_id`) has no id on a phone: its
 *                    values from `data` go into the style (`nativeStyle`'s `areaData`), a style swap per change.
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
 *   - The generated images (the hatches, the gauge pill, the route's pin: `images.ts`, `addImage` on the web) go to
 *     the binding's `Images` as PNGs, under a name per theme (`nativeImages`, T74).
 *   - THE COLOURING ENGINE is the web's (`colouring.ts`), fed by the patched binding's per-tile event and sending its
 *     labels in batches (`native/useColouring.ts`, T74): status, standings, the lake modes and salmon colour each tile
 *     as it is requested, as GL JS's `sourcedata` does on the web. No colour is pushed through `data`.
 *   - The scale bar is drawn here (`metricScale`): the binding's is top-left only on Android and in the device's
 *     units, so it sat under the status bar in miles. Updated when the camera settles, not per frame of a pinch.
 *   - Markers are native view annotations (`Marker`), not maplibre-gl's DOM markers.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Pressable, Text, View, type LayoutChangeEvent } from "react-native";
import {
  Camera, GeoJSONSource, Images, Map as MLMap, Marker, VectorSource,
  type CameraRef, type GeoJSONSourceRef, type MapRef, type VectorSourceRef,
} from "@maplibre/maplibre-react-native";
import { CAMERA_BOUNDS, CAMERA_MAX_ZOOM, FIT_MAX_ZOOM, MIN_ZOOM, SPOT_PIN, fitAtMost, padBbox, refineFit, spotMarkers,
         spotUnder, profileLabel, type Bbox, type SpotMarker, type SectionKey } from "@app/core";
import { pressSpot, spotsKey } from "./spots";
import { baseAdapter } from "./adapters/contract";
import { nativeRenderer } from "./adapters/native";
import { mapChrome } from "./chrome";
import { nativeMapGestures } from "./gestures";
import { areaLayer, nativeStyle } from "./native-style";
import { spreadSections, VISIBLE_LAYERS, visibleSections } from "./sections";
import { withoutEngineKeys } from "./colouring";
import { nativeImages } from "./images";
import { useNativeColouring, type AtlasSource } from "./native/useColouring";
import { colourProbe } from "./native/probe";
import { MARK_LABEL_FONT, labelShift } from "./marks";
import { metricScale } from "./scale";
import type { MapProps } from "./map-props";
import { COPY_SOURCE, WaterCopy, copyIds, probeZoom, zoomAround, type CopyTarget } from "./water-copy";

/** A mark label's widest line (the web's `max-width`, `marks.ts`). */
const MARK_LABEL_WIDTH = 180;

/** An empty source, so the gauge layers exist before the first feed tick arrives (as on the web). */
const EMPTY_FC = '{"type":"FeatureCollection","features":[]}';
/** `CAMERA_BOUNDS` (`[[w, s], [e, n]]`) in the binding's flat `[w, s, e, n]`. */
const MAX_BOUNDS: [number, number, number, number] =
  [CAMERA_BOUNDS[0][0], CAMERA_BOUNDS[0][1], CAMERA_BOUNDS[1][0], CAMERA_BOUNDS[1][1]];
/** Where the binding's own scale bar is parked: present (see the `scaleBar` prop), never seen. */
const OFF_SCREEN = { top: -10000, left: -10000 } as const;
/** The web's `ScaleControl({ maxWidth: 92 })`. */
const SCALE_MAX_WIDTH = 92;

type PressEvent = { nativeEvent: { point: [number, number] | { x: number; y: number };
                                   lngLat: [number, number] } | null };
type RegionEvent = { nativeEvent: { center?: [number, number]; zoom?: number; userInteraction?: boolean } | null };
type Data = NonNullable<MapProps["data"]>;

export function Map({ at, theme, view, modes, groups, hide, show, initial, data, onPressFeature, onPressObstacle, onPressTidal, chrome, onError,
                      onMoved, onMapPoint, onLongPress, highlight, water, waterLine, selection = "outline", marker, style, gauges,
                      onVisible, pins, bare, fit, status, standing, lakes, salmon, method,
                      basemap, marks, onPressMark, spots, onImageryError: _noTileErrors,
                      onRegion: _noPrefetch }: MapProps) {
  // `onRegion` is accepted and never called: it feeds the WEB's prefetch, and a phone holds the release installed.
  // `onImageryError` is accepted and never called: the binding has no per-tile error event. A phone learns the imagery
  // is unreachable from `useImageryReach`'s probe instead (satellite is online-only, owner 2026-10-21).
  const adapter = useRef(baseAdapter("native"));
  const mapRef = useRef<MapRef | null>(null);
  const camRef = useRef<CameraRef | null>(null);
  const atlasRef = useRef<VectorSourceRef | null>(null);
  /** (T175) The water copy's GeoJSON source, and the lines it holds — see `water-copy.ts`. */
  const copyRef = useRef<GeoJSONSourceRef | null>(null);
  const copy = useRef(new WaterCopy());
  /** The map's size, for the fit padding (the web reads its container). */
  const size = useRef({ width: 0, height: 0 });
  /** A style has finished loading at least once. */
  const [styled, setStyled] = useState(false);
  /**
   * The map has DRAWN once (`onDidFinishRenderingMapFully`). Nothing touches feature-state or queries before it: the
   * renderer does not exist yet, and MapLibre Native dereferences it — `setFeatureState` crashed the app with SIGSEGV
   * in `MapRenderer::actor()` when a remounted map was handed its status colours straight after its style loaded
   * (emulator, N2 review), exactly as `queryRenderedFeatures` did (N2).
   */
  const [drawn, setDrawn] = useState(false);
  /** Bumped when a style finishes loading (a swap dropped every feature-state) or the atlas source attaches. */
  const [gen, setGen] = useState(0);
  const furniture = useMemo(() => chrome ?? mapChrome(theme), [chrome, theme]);

  // Callbacks through refs: the SDK handlers are registered once, and an inline arrow from a caller must not
  // re-register them (or, for the style, swap it).
  const cb = useRef({ onError, onMoved, onMapPoint, onPressFeature, onPressObstacle, onPressTidal, onVisible, onLongPress });
  cb.current = { onError, onMoved, onMapPoint, onPressFeature, onPressObstacle, onPressTidal, onVisible, onLongPress };
  const report = useCallback((e: unknown) =>
    cb.current.onError?.(e instanceof Error ? e : new Error(String(e))), []);

  /** The contract's renderer (`adapters/native.ts`) over the binding's refs, read when called: they come and go. */
  const renderer = useMemo(() => {
    // (T175) every state for one of the copy's sections is the copy's too: the one pathway, written twice
    const toCopy: CopyTarget = {
      set: (id, s) => { void copyRef.current?.setFeatureState({ id }, s as never).catch(report); },
      remove: (id, key) => {
        const src = copyRef.current;
        if (src) void (key === undefined ? src.removeFeatureState({ id }) : src.removeFeatureState({ id }, key)).catch(report);
      },
    };
    return nativeRenderer({
      map: () => mapRef.current as never,
      atlas: () => atlasRef.current as never,
      camera: () => camRef.current as never,
    }, report, { copy: () => copy.current, target: () => toCopy });
  }, [report]);

  /*
   * THE DATA, SPLIT BY WHO CAN TAKE IT. The sections (stream, lake, wetland: MVT ids) go to feature-state; a layer keyed
   * by a property (`basin`: `basin_id`) has no id on a phone, so its values go into the style (`areaData`). The
   * `SECTION_DATA` key is fanned out to every per-section layer first (`spreadSections`, T124).
   */
  const [sectionData, areaData] = useMemo(() => {
    const sec: Data = {}, area: Data = {};
    for (const [layerId, values] of Object.entries(spreadSections(data ?? {}))) (areaLayer(layerId) ? area : sec)[layerId] = values;
    return [sec, area];
  }, [data]);
  const areaKey = JSON.stringify(areaData);

  /** (T74) THE COLOURING ENGINE: each tile's sections labelled as the binding requests it (`native/useColouring.ts`). */
  const colouring = useNativeColouring({
    atlas: () => atlasRef.current as unknown as AtlasSource | null,
    copy: () => copy.current,
    toCopy: { set: (id, s) => { void copyRef.current?.setFeatureState({ id }, s as never).catch(report); },
              remove: (id, key) => { void copyRef.current?.removeFeatureState({ id }, key).catch(report); } },
    // what a clear took that was not the engine's (the highlight, pushed data) is sent again by the effects below
    restore: () => { renderer.forget(); setGen((g) => g + 1); },
    ready: styled && drawn, theme, atlasUrl: at.atlas, report, status, standing, lakes, salmon, method,
  });
  const cv = colouring.values;
  const basinKey = JSON.stringify(cv.standing?.basins ?? null);

  /*
   * THE STYLE. Keyed on what changes how the map LOOKS — never on `gauges` (read through a ref: a feed tick goes to
   * the `gauges` source below, not into a 225 ms style swap) and never on a caller's new-but-equal objects.
   */
  const gaugesNow = useRef(gauges);
  gaugesNow.current = gauges;
  /** (T175) Where the copy stops (the zoom the atlas draws the water from), per kind; in the style, so a change swaps it. */
  const [ceil, setCeil] = useState<{ line: number | null; fill: number | null }>({ line: null, fill: null });
  const look = useMemo(
    () => nativeStyle({ at, theme, view, modes, hide, show, groups, basemap: basemap ?? "map",
                        gauges: gaugesNow.current ?? EMPTY_FC, areaData: JSON.parse(areaKey) as Data, copyCeiling: ceil,
                        values: cv }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [at.atlas, at.basemap, at.glyphs, at.sprite, at.assets, at.outside, theme, view, JSON.stringify(modes ?? {}),
     JSON.stringify(hide ?? []), JSON.stringify(show ?? []), JSON.stringify(groups ?? {}), basemap, areaKey, ceil.line, ceil.fill,
     cv.status, cv.salmon, cv.method, cv.stocked, cv.survey, basinKey]);
  const images = useMemo(() => nativeImages(theme), [theme]);
  useEffect(() => { for (const e of look.errors) report(e); }, [look, report]);

  const gaugeData = useMemo(() => JSON.parse(gauges ?? EMPTY_FC) as GeoJSON.FeatureCollection, [gauges]);
  const camera0 = useRef({ center: [initial.lon, initial.lat] as [number, number], zoom: initial.zoom });
  /** Where the camera is, for the scale bar: the latitude and zoom it last settled at. */
  const [scaleAt, setScaleAt] = useState({ lat: initial.lat, zoom: initial.zoom });
  /** (T145) The spot marks drawn, and the zoom they were placed at: what a tap is checked against (`spotUnder`). */
  const spotsAt = useRef<{ marks: readonly SpotMarker[]; zoom: number }>({ marks: [], zoom: initial.zoom });

  // The FIRST attach is news (the binding mounts the source only once the map is ready, maybe after the style
  // loaded); later ones are the binding re-creating its handle (`useImperativeHandle` with no deps), which must not
  // re-render this map, or each render would re-attach and render again. A style swap is `onStyle`'s business.
  const attachedOnce = useRef(false);
  const atlasAttached = useCallback((r: VectorSourceRef | null) => {
    atlasRef.current = r;
    if (r && !attachedOnce.current) { attachedOnce.current = true; setGen((g) => g + 1); }
  }, []);
  // (T74) the atlas asked for its tiles' ids — again after every style load (the binding may re-create the source)
  // only once the map has drawn: before that the source's native view may not be resolvable by its tag yet
  useEffect(() => { if (atlasRef.current && styled && drawn) colouring.attached(); }, [gen, styled, drawn, colouring]);

  // PUSHED PER-FEATURE DATA that is not a colouring (the engine's keys are refused, as on the web: one pathway); a
  // value a feature already holds is not sent again (`onceSink`)
  useEffect(() => {
    if (!styled || !drawn || !atlasRef.current) return;
    const h = renderer.states(() => []);
    for (const [layerId, values] of Object.entries(withoutEngineKeys(sectionData, report))) {
      try { adapter.current.setData(h, layerId, values); } catch (e) { report(e); }
    }
  }, [sectionData, gen, styled, drawn, report, renderer]);

  // THE SELECTION: only the highlight's keys (`selected`, and the rest of the water's `water`) are cleared, per
  // feature, from the ids lit last time
  const wasLit = useRef<SectionKey[]>([]);
  useEffect(() => {
    if (!styled || !drawn || !atlasRef.current || !highlight) return;
    adapter.current.highlight(renderer.states(() => wasLit.current), [...highlight], [...(water ?? [])], selection);
    wasLit.current = [...highlight, ...(water ?? [])];
  }, [highlight, water, selection, gen, styled, drawn, report, renderer]);

  /*
   * NO QUERY BEFORE THE FIRST FULL RENDER. MapLibre Native's `queryRenderedFeatures` dereferences the renderer, which
   * does not exist until the map has drawn: asked straight after the style loaded, the app died with SIGSEGV in
   * `MapRenderer::actor()` (measured on the emulator, N2). `rendered` opens the gate on `onDidFinishRenderingMapFully`,
   * which is also this map's `idle`: the moment the tiles on screen are in, so the visible set is the whole one.
   */
  const rendered = useRef(false);
  /**
   * ONE VISIBLE-SET QUERY PER CAMERA MOVE (N2 review). The viewport query sends every rendered feature, geometry and
   * all, across the bridge; the map also renders "fully" again after our own feature-state lands, with nothing new on
   * screen. So the camera settling (or a new style, or a new listener) marks the set stale, and the next full render
   * asks once.
   */
  const stale = useRef(true);
  /** The visible set last reported. */
  const lastSeen = useRef("");
  const reportVisible = useCallback(async () => {
    if (!cb.current.onVisible || !rendered.current || !stale.current) return;
    stale.current = false;
    try {
      const seen = await renderer.visible();
      const key = seen.join(",");
      if (seen.length && key !== lastSeen.current) { lastSeen.current = key; cb.current.onVisible?.(seen); }
    } catch (e) { report(e); }
  }, [report, renderer]);
  // asked again as soon as someone (new) listens: a settled map does not draw again by itself (a tab switch)
  useEffect(() => { lastSeen.current = ""; stale.current = true; void reportVisible(); },
            [onVisible !== undefined, reportVisible]);
  const onRendered = useCallback(() => {
    rendered.current = true;
    setDrawn(true);
    void reportVisible();
    void refine.current?.().catch(report);
    void keepCopy.current?.().catch(report);
  }, [reportVisible, report]);

  /*
   * (T175) THE WATER'S OWN LINE, KEPT — as on the web (`Map.web.tsx`, `water-copy.ts`). Keyed on the water's ids and
   * the archive. After each settle's full render the map's own pick (`litFeatures`, the binding's
   * `querySourceFeatures`) is asked for the water's sections; what a zoom has the first time is kept, given the state
   * its atlas features hold (`getFeatureState`), and drawn from the copy source below the ceiling. With `waterLine`, while
   * no zoom has shown the water, the camera steps in about a point on it, never against the reader's own gesture.
   */
  const keep = useMemo(() => copyIds(highlight, water, waterLine?.sections),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed on the ids' text
    [(highlight ?? []).join(","), (water ?? []).join(","), (waterLine?.sections ?? []).join(",")]);
  const keepKey = keep.join(",");
  const [copyData, setCopyData] = useState(() => copy.current.collection());
  const copyStale = useRef(true);
  const copyTouched = useRef(false);
  const camNow = useRef({ lon: initial.lon, lat: initial.lat, zoom: initial.zoom });
  const lineAt = useRef(waterLine?.at ?? null);
  lineAt.current = waterLine?.at ?? null;
  const lineIds = useRef<ReadonlySet<number>>(new Set());
  lineIds.current = useMemo(() => new Set(copyIds(waterLine?.sections)),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- keyed on the ids' text
    [(waterLine?.sections ?? []).join(",")]);
  const clamps = useRef(false);
  clamps.current = waterLine != null;
  const lookNow = useRef(look);
  lookNow.current = look;
  const keepCopy = useRef<(() => Promise<void>) | null>(null);
  useEffect(() => {
    const c = copy.current;
    const had = c.ids.size > 0;
    if (c.reset(keep, at.atlas) && had) {
      setCopyData(c.collection());
      setCeil((was) => (was.line === null && was.fill === null ? was : { line: null, fill: null }));
      void copyRef.current?.removeFeatureState().catch(report);
      copyTouched.current = false;
    }
    let rounds = 0, alive = true, busy = false;
    copyStale.current = true;
    keepCopy.current = async () => {
      if (!alive || busy || !c.ids.size || !rendered.current || !copyStale.current) return;
      copyStale.current = false;
      busy = true;
      try {
        const at0 = camNow.current;
        const found = await renderer.litFeatures([...c.ids]);
        if (!alive) return;
        const fresh = c.take(found, at0.zoom);
        if (fresh.length) {
          // what the atlas features hold now goes on the copy first (later writes are mirrored as they are made)
          for (const id of fresh) {
            const held = await atlasRef.current?.getFeatureState({ id, sourceLayer: c.layerOf(id)! }).catch(() => null);
            const st = c.fresh(id, (held ?? {}) as Record<string, unknown>);
            if (st) void copyRef.current?.setFeatureState({ id }, st as never).catch(report);
          }
          setCopyData(c.collection());
        }
        // THE CEILING, from the style this map is drawing (the stream's and the lake's floor in this view)
        const floorOf = (id: string) =>
          ((lookNow.current.style.layers as { id: string; minzoom?: number }[]).find((l) => l.id === id)?.minzoom) ?? 0;
        const line = c.ceiling("stream", floorOf("stream")), fill = c.ceiling("lake", floorOf("lake"));
        setCeil((was) => (was.line === line && was.fill === fill ? was : { line, fill }));
        const next = clamps.current
          ? probeZoom({ zoom: at0.zoom, found: found.some((f) => lineIds.current.has(f.id)),
                        kept: c.keptOf(lineIds.current), rounds, touched: copyTouched.current })
          : null;
        if (next !== null) {
          rounds += 1;
          renderer.easeTo(zoomAround(at0, lineAt.current, at0.zoom, next), { duration: 300 });
        }
      } finally { busy = false; }
    };
    void keepCopy.current().catch(report);
    return () => { alive = false; keepCopy.current = null; };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the water (its ids) and the archive are the key
  }, [keepKey, at.atlas, renderer, report]);

  /*
   * SEND THE CAMERA to a subject, then let the water's own shape frame it (as on the web): after each move settles and
   * draws, the `highlight` features the tiles have delivered are measured and the camera re-fits while `refineFit` says
   * there is more to see. Bounded rounds. Waits for the camera to exist: the binding mounts it with the map.
   */
  const lit = useRef(highlight);
  lit.current = highlight;
  const refine = useRef<(() => Promise<void>) | null>(null);
  const fitted = useRef<string | null>(null);
  useEffect(() => {
    if (!fit || !camRef.current || fitted.current === fit.key) return;
    fitted.current = fit.key;
    let rounds = 0;
    let alive = true;
    /** The camera moved since the last measure: a render with nothing new is not a reason to measure again. */
    let moved = false;
    const go = (b: Bbox) => {
      const { width, height } = size.current;
      const p = Math.max(8, Math.round(Math.min(56, (width || 400) * 0.12, (height || 800) * 0.12)));
      moved = true;
      // the binding's fitBounds has no maxZoom, and the camera now goes to z18: widen the box to what FIT_MAX_ZOOM shows
      renderer.fitBounds(fitAtMost(b, FIT_MAX_ZOOM, width, height, p), { padding: p, duration: 450 });
    };
    // (T48) an exact camera to return to (search's cancel): put back as it was, nothing measured
    if (fit.camera) { renderer.easeTo(fit.camera, { duration: 450 }); refine.current = null; return () => { alive = false; }; }
    if (fit.bbox) go(fit.bbox);
    if (!fit.refine) { refine.current = null; return () => { alive = false; }; }
    refine.current = async () => {
      const ids = (lit.current ?? []).map(Number).filter(Number.isFinite);
      if (!alive || !moved || !ids.length || !rendered.current) return;
      moved = false;
      const found = await renderer.waterBox(ids);
      const next = refineFit(await renderer.bounds() as Bbox, found, rounds);
      if (!alive) return;
      if (next) { rounds += 1; go(padBbox(next, 0.4)); } else refine.current = null;
    };
    return () => { alive = false; refine.current = null; };
  }, [fit?.key, styled, renderer]);

  /** A tap: the 24 px box, streams first, the section as a number (`MapRenderer.press`). */
  const onPress = useCallback(async (e: PressEvent) => {
    // READ THE EVENT BEFORE ANY await: React Native recycles it, and `nativeEvent` is null afterwards (N1 §3)
    const ne = e.nativeEvent;
    if (!ne) return;
    const [x, y] = Array.isArray(ne.point) ? ne.point : [ne.point.x, ne.point.y];
    const [lon, lat] = ne.lngLat;
    // (T145) a tap on a spot is the spot's (its annotation's press): never ALSO the map's — no water opens under it,
    // and the off-spot tap that clears the selection does not clear the one this tap makes
    if (spotUnder(spotsAt.current.marks, lat, lon, spotsAt.current.zoom)) return;
    cb.current.onMapPoint?.(lat, lon);
    const press = cb.current.onPressFeature, obstacle = cb.current.onPressObstacle, tidal = cb.current.onPressTidal;
    if ((!press && !obstacle && !tidal) || !rendered.current) return;       // no renderer to query yet (see `rendered`)
    try {
      // (T173) a fish obstacle mark first: a tap on the mark is the mark's, and opens no water under it
      const ob = obstacle ? await renderer.pressObstacle(x, y) : null;
      if (ob) { obstacle!(ob); return; }
      const hit = press ? await renderer.press(x, y) : null;
      if (hit) { press!(hit.layer, hit.id, lat, lon); return; }
      // (T166) no water under the tap: tidal water, if that is what it landed on
      if (tidal && await renderer.pressTidal(x, y)) tidal();
    } catch (err) { report(err); }
  }, [report, renderer]);

  /**
   * A LONG PRESS (owner 2026-10-21): MapLibre Native's own, decided by its gesture detector beside its pan and pinch, so
   * it cannot fight them; the tap it replaces is not reported. Only where the place under the finger is.
   */
  const onLongPressed = useCallback((e: PressEvent) => {
    const ne = e.nativeEvent;          // before anything else, as with a press: React Native recycles the event
    if (!ne) return;
    const [lon, lat] = ne.lngLat;
    cb.current.onLongPress?.(lat, lon);
  }, []);

  const statusNow = useRef(status);
  statusNow.current = status;
  /** (T74) Development measurement only (`EXPO_PUBLIC_COLOUR_PROBE`): pans' frames with a section not yet coloured. */
  const probe = useMemo(() => colourProbe({
    query: async () => visibleSections(await mapRef.current?.queryRenderedFeatures({ layers: [...VISIBLE_LAYERS] }) ?? []),
    coloured: colouring.coloured,
    // a section the day's index labels (the Map tab's status; nothing is wanted where no status is handed over)
    wanted: (id) => { const st = statusNow.current; return !!st && profileLabel(st.index, id) !== undefined; },
  }), [colouring.coloured]);

  /** (T50) each mark label's sideways shift, kept inside the map as the web's `markers.ts` does (`labelShift`): measured
   *  where the camera settles, from each ring's screen x */
  const [markShift, setMarkShift] = useState<readonly number[]>([]);
  const marksNow = useRef(marks);
  marksNow.current = marks;
  const shiftMarks = useCallback(async () => {
    const ms = marksNow.current ?? [], w = size.current.width, map = mapRef.current;
    if (!ms.length || !w || !map) return;
    const xs = await Promise.all(ms.map((k) => map.project([k.lon, k.lat]).then((pt) => pt[0], () => null)));
    const next = xs.map((x) => (x === null ? 0 : labelShift(x, w)));
    setMarkShift((was) => (was.length === next.length && was.every((v, i) => v === next[i]) ? was : next));
  }, []);
  useEffect(() => { void shiftMarks(); }, [marks, shiftMarks]);

  const onRegionDidChange = useCallback((e: RegionEvent) => {
    probe?.settled();
    const ne = e.nativeEvent;          // before anything async, as with a press
    if (ne?.center && ne.zoom !== undefined) {
      cb.current.onMoved?.({ lon: ne.center[0], lat: ne.center[1], zoom: ne.zoom });
      camNow.current = { lon: ne.center[0], lat: ne.center[1], zoom: ne.zoom };
      const next = { lat: ne.center[1], zoom: ne.zoom };
      setScaleAt((was) => (was.lat === next.lat && was.zoom === next.zoom ? was : next));
    }
    void shiftMarks();
    // the next full render reports what is on screen (and measures a fit): the tiles for here may not be in yet
    stale.current = true;
    copyStale.current = true;
    // (T175) the reader's own pan or pinch: the copy never steps in against it
    if (ne?.userInteraction) copyTouched.current = true;
  }, [shiftMarks]);

  // a new style dropped every feature-state (`forget`: send them all again) and may draw other features
  const onStyle = useCallback(() => {
    renderer.forget();
    colouring.swapped();
    copy.current.forgetStates();
    stale.current = true;
    copyStale.current = true;
    setStyled(true);
    setGen((g) => g + 1);
  }, [renderer, colouring]);
  const onLayout = useCallback((e: LayoutChangeEvent) => {
    size.current = { width: e.nativeEvent.layout.width, height: e.nativeEvent.layout.height };
  }, []);

  const scale = metricScale(scaleAt.lat, scaleAt.zoom, SCALE_MAX_WIDTH);
  const spotKey = spotsKey(spots?.points, spots?.selected ?? null);
  const spotMarks = useMemo(() => (spots ? spotMarkers(spots.points, scaleAt.zoom, spots.selected) : []),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [spotKey, scaleAt.zoom]);
  spotsAt.current = { marks: spotMarks, zoom: scaleAt.zoom };
  return (
    <>
      <MLMap
        ref={mapRef}
        style={[{ flex: 1 }, style]}
        onLayout={onLayout}
        mapStyle={look.style as never}
        // G1: one gesture policy for both maps — pan, pinch, double-tap zoom; no rotate, no tilt, no compass
        {...nativeMapGestures()}
        // a bare map is an illustration: no furniture. Attribution goes with it (the page credits the data, MapProps.bare)
        attribution={!bare}
        logo={false}
        // OURS is drawn below (`metricScale`): the binding's is top-left only on Android, in the device's units. But
        // the binding's must EXIST on Android: switched off, the map's surface left a black hole over the next screen
        // when the map unmounted (emulator, N2 review; bisected to this one prop). So it is on, and parked off screen
        scaleBar
        scaleBarPosition={OFF_SCREEN}
        onPress={onPress as never}
        onLongPress={onLongPress ? onLongPressed as never : undefined}
        onRegionDidChange={onRegionDidChange as never}
        onRegionWillChange={probe ? (() => probe.moving()) as never : undefined}
        onDidFinishRenderingFrame={probe ? (() => probe.frame()) as never : undefined}
        onDidFinishLoadingStyle={onStyle}
        onDidFinishRenderingMapFully={onRendered}
        // an empty map and a map whose tiles failed look the same: say so
        onDidFailLoadingMap={() => report(new Error("map failed to load"))}
      >
        <Camera ref={camRef} initialViewState={camera0.current} minZoom={MIN_ZOOM} maxZoom={CAMERA_MAX_ZOOM}
                maxBounds={MAX_BOUNDS} />
        <VectorSource id="atlas" ref={atlasAttached} />
        {/* (T74) the hatches, the gauge pill and the route's pin, in this theme */}
        <Images images={images as never} />
        <GeoJSONSource id="gauges" data={gaugeData} />
        {/* (T175) the water copy: the style's own source by id, its lines from the tiles (`keepCopy`) */}
        <GeoJSONSource id={COPY_SOURCE} ref={copyRef} data={copyData as never} />
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
        {/* (T131) YOUR SPOTS: `spotMarkers` at the zoom the camera settled at, each the app's face in a 44 px box (a view
            annotation: its press is the face's, never the map's) */}
        {spots ? spotMarks.map((mk) => (
          <Marker key={mk.key} id={mk.key} lngLat={[mk.lon, mk.lat]}>
            <View style={{ width: SPOT_PIN.hit, height: SPOT_PIN.hit }}>
              {spots.face(mk, () => pressSpot(mk, spots, (c, o) => renderer.easeTo(c, o)))}
            </View>
          </Marker>
        )) : null}
        {/* THE BOUNDARY MARKS (P15): the web's `markElement` as a view annotation — a 44 px target, a 16 px ring
            (filled when open), the label above it */}
        {(marks ?? []).map((k, i) => (
          <Marker key={`mark-${i}:${k.lon},${k.lat}`} id={`mark-${i}`} lngLat={[k.lon, k.lat]}>
            {/* the root is the label's 180 px band, the 44 px target centred in it: Android's view annotation lays its
                content out no wider than the ROOT, so a label hung off a 44 px root wrapped to 44 px ("fish / ing b /
                ou…", T50 emulator walk). The label stays inside the button (its words are the button's) on a band of
                its own width; `markShift` keeps it inside the map as the web's `labelShift` does. */}
            <View pointerEvents="box-none"
                  style={{ width: MARK_LABEL_WIDTH, height: 44, alignItems: "center", justifyContent: "center" }}>
              <Pressable onPress={() => onPressMark?.(i)} accessibilityRole="button" aria-label={k.a11y}
                         style={{ width: 44, height: 44, alignItems: "center", justifyContent: "center" }}>
                {k.label ? (
                  <View pointerEvents="none"
                        style={{ position: "absolute", bottom: 38, left: (44 - MARK_LABEL_WIDTH) / 2,
                                 width: MARK_LABEL_WIDTH, alignItems: "center",
                                 transform: [{ translateX: markShift[i] ?? 0 }] }}>
                    <Text numberOfLines={3}
                          style={{ paddingHorizontal: 7, paddingVertical: 3, textAlign: "center",
                                   backgroundColor: furniture.card, color: furniture.ink, borderWidth: 1.5,
                                   borderColor: furniture.ink, fontSize: 12, lineHeight: 15,
                                   fontFamily: MARK_LABEL_FONT.native }}>
                      {k.label}
                    </Text>
                  </View>
                ) : null}
                <View style={{ width: 16, height: 16, borderRadius: 8, borderWidth: 3, borderColor: furniture.ink,
                               backgroundColor: k.on ? furniture.ink : furniture.card }} />
              </Pressable>
            </View>
          </Marker>
        ))}
      </MLMap>
      {bare ? null : (
        // THE WEB'S SCALE BAR (controls.css `.maplibregl-ctrl-scale`): bottom-right, a RULER — a hairline with a tick at
        // each end, the distance above its left end, a paper halo and no box — so the bar's LENGTH is the measurement
        // and the number only names it.
        // A SIBLING of the map, positioned in the map's own container — NOT a wrapper View around the map: wrapped,
        // the map's surface left a black rectangle over the next screen on Android when it unmounted (emulator, N2
        // review; the unwrapped map does not)
        <View pointerEvents="none" accessibilityLabel="scale"
              style={{ position: "absolute", right: 10, bottom: 24, width: Math.round(scale.width) }}>
          <Text numberOfLines={1}
                style={{ color: furniture.ink, fontSize: 11, lineHeight: 13, letterSpacing: 0.2, paddingLeft: 4,
                         marginBottom: 2, fontFamily: "Archivo_600SemiBold", fontVariant: ["tabular-nums"],
                         textShadowColor: furniture.card, textShadowRadius: 2, textShadowOffset: { width: 0, height: 0 } }}>
            {scale.label}
          </Text>
          <View style={{ height: 7, borderLeftWidth: 1.5, borderRightWidth: 1.5, borderBottomWidth: 1.5,
                         borderColor: furniture.ink }} />
        </View>
      )}
    </>
  );
}

/** A marker's dot: the colour with a ring in the theme's card colour, so it reads on water and on land. */
function dot(colour: string, ring: string, d: number) {
  return { width: d, height: d, borderRadius: d / 2, backgroundColor: colour, borderWidth: 2,
           borderColor: ring } as const;
}
