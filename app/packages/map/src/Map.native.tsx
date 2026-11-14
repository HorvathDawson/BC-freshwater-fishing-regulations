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
 *   - (T317) The marker, pins and boundary marks are GeoJSON layers (`point-marks.ts`); (T316) the spots too
 *     (`spot-marks.ts`): no view annotation is left.
 */
import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { AccessibilityInfo, Pressable, Text, View, type LayoutChangeEvent } from "react-native";
import {
  Camera, GeoJSONSource, Images, Layer, LogManager, Map as MLMap, VectorSource,
  type CameraRef, type GeoJSONSourceRef, type MapRef, type VectorSourceRef,
} from "@maplibre/maplibre-react-native";
import { CAMERA_BOUNDS, CAMERA_MAX_ZOOM, MIN_ZOOM, fitAtMost, fitZoomLimit, fitZoomOf, padBbox, refineFit, spotMarkers,
         spotsInView, spotUnder, profileLabel, type Bbox, type SpotMarker, type SectionKey, now as clockNow } from "@app/core";
import { pressSpot, spotsKey } from "./spots";
import { SPOT_SOURCE, spotBoxImage, spotBoxName, spotImages, spotsData, spotsLayers } from "./spot-marks";
import { baseAdapter } from "./adapters/contract";
import { nativeRenderer } from "./adapters/native";
import { mapChrome } from "./chrome";
import { zoomStep } from "./zoom-step";
import { nativeMapGestures } from "./gestures";
import { areaLayer, nativeStyle } from "./native-style";
import { NativeLookTracker } from "./look-drawn";
import { liftFor, type Lift, type LiftAnswer } from "./tapLift";
import { isTapTarget, litKey, spreadSections, TAP_GROUPS, tapBox, VISIBLE_LAYERS, visibleSections } from "./sections";
import { withoutEngineKeys } from "./colouring";
import { nativeImages } from "./images";
import { useNativeColouring, type AtlasSource } from "./native/useColouring";
import { colourProbe } from "./native/probe";
import { panProbe } from "./native/panProbe";
import { GoneViews } from "./native/goneView";
import { MARK_BOX_IMAGE, MARK_LABEL, MARK_RING, MARK_SOURCE, PIN_SOURCE, markBox, markBoxImage, markHit, marksData,
         marksLayers, pinDotLayer, pinsData, shiftedLabels, titledPin, PIN_DOT } from "./point-marks";
import { pngDataUri } from "./png";
import { metricScale } from "./scale";
import { GEOMETRY, SHOW_SCALE } from "./chrome";
import { MapCredits } from "./MapCredits";
import { LAB_ON, labStyle, useLab } from "./lab";
import type { MapProps } from "./map-props";
import { COPY_LINE, COPY_SOURCE, COPY_UNTIL, WaterCopy, copyIds, pieceGridZoom, probeZoom, zoomAround, type CopyTarget } from "./water-copy";
import { cameraMs, setReducedMotion } from "./motion";
import { ME_SOURCE, meData, mePaint } from "./me";
import { camProbe } from "./native/camProbe";
import { watchStyleAssets } from "./native/styleAssets";
import { MAX_PIXEL_RATIO } from "./pixel-ratio";

/** (T401) MapLibre's prefetch zoom delta: how many zooms below the shown one it also loads, drawn as the placeholder
 *  while a tile loads. -1 = MapLibre's own (4). Measured 2026-11-06 on the stressed emulator: 2 and 3 moved neither the
 *  frames missing tiles nor frame time beyond run-to-run noise (T401 ticket), so MapLibre's default stays */
const PREFETCH_ZOOM_DELTA = -1;

/** (T317) a style-spec layer as the binding's `Layer` takes it (its source is the enclosing `GeoJSONSource`) */
type LayerSpec = React.ComponentProps<typeof Layer>;

/** An empty source, so the gauge layers exist before the first feed tick arrives (as on the web). */
const EMPTY_FC = '{"type":"FeatureCollection","features":[]}';
const EMPTY_FC_OBJ = { type: "FeatureCollection" as const, features: [] as unknown[] };
/** `CAMERA_BOUNDS` (`[[w, s], [e, n]]`) in the binding's flat `[w, s, e, n]`. */
const MAX_BOUNDS: [number, number, number, number] =
  [CAMERA_BOUNDS[0][0], CAMERA_BOUNDS[0][1], CAMERA_BOUNDS[1][0], CAMERA_BOUNDS[1][1]];
/** Where the binding's own scale bar is parked: present (see the `scaleBar` prop), never seen. */
const OFF_SCREEN = { top: -10000, left: -10000 } as const;
/** The web's `ScaleControl({ maxWidth: 92 })`. */
const SCALE_MAX_WIDTH = GEOMETRY.scaleMax;

type PressEvent = { nativeEvent: { point: [number, number] | { x: number; y: number };
                                   lngLat: [number, number] } | null };
type RegionEvent = { nativeEvent: { center?: [number, number]; zoom?: number; userInteraction?: boolean;
                                    bounds?: [number, number, number, number] } | null };
type Data = NonNullable<MapProps["data"]>;

/** What a tap's hit test answers (`MapRenderer.press`). */
type PressHit = { layer: string; id: number };

/** (T348) How long a settled camera waits for a full render before it asks what is on screen itself. */
const SETTLE_ASK_MS = 700;

export function Map({ at, theme, view, modes, groups, hide, show, initial, data, onPressFeature, onPressStart, onPressObstacle, onPressNotFishing, chrome, onError,
                      onMoved, onMapPoint, onLongPress, onOpenLab, highlight, inSeason, water, waterLine, selection = "outline", marker, style, gauges,
                      onVisible, pins, bare, fit, status, standing, lakes, salmon, method, runs, focus, near,
                      basemap, marks, onPressMark, spots, me, onImageryError: _noTileErrors,
                      onRegion: _noPrefetch, insetBottom = 0, insetTop = 0, zoomControls, trace: _noTrace, onOpenLink, onDrawn, lookKey, onLookDrawn }: MapProps) {
  // `onRegion` is accepted and never called: it feeds the WEB's prefetch, and a phone holds the release installed.
  // `onImageryError` is accepted and never called: the binding has no per-tile error event. A phone learns the imagery
  // is unreachable from `useImageryReach`'s probe instead (satellite is online-only, owner 2026-10-21).
  // (T243) the camera's own glides jump while the OS asks for reduced motion (`motion.ts`)
  useEffect(() => {
    let live = true;
    AccessibilityInfo.isReduceMotionEnabled?.().then((v) => { if (live) setReducedMotion(v); }, () => {});
    const sub = AccessibilityInfo.addEventListener?.("reduceMotionChanged", (v: boolean) => setReducedMotion(v));
    return () => { live = false; sub?.remove?.(); };
  }, []);
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
  const cb = useRef({ onError, onMoved, onMapPoint, onPressFeature, onPressStart, onPressObstacle, onPressNotFishing, onVisible, onLongPress });
  cb.current = { onError, onMoved, onMapPoint, onPressFeature, onPressStart, onPressObstacle, onPressNotFishing, onVisible, onLongPress };
  // (T445) a call that outlived its view (the map left, or a style load made the atlas source anew) is not a failure:
  // `goneView.ts`. Any other — a live view's included — is reported.
  const gone = useRef(new GoneViews());
  useEffect(() => { gone.current.mount(); return () => gone.current.unmount(); }, []);
  const report = useCallback((e: unknown) => {
    if (gone.current.stale(e)) return;
    cb.current.onError?.(e instanceof Error ? e : new Error(String(e)));
  }, []);
  // (T418) a sprite, glyph range or source that would not load is reported once, not only logged (MapLibre's log)
  useEffect(() => { watchStyleAssets(LogManager); }, []);
  /** (T317) the boundary marks and their press, read by the tap (`onPress`) and the label measure (`shiftMarks`) */
  const marksNow = useRef(marks);
  marksNow.current = marks;
  const pressMarkNow = useRef(onPressMark);
  pressMarkNow.current = onPressMark;

  /** The contract's renderer (`adapters/native.ts`) over the binding's refs, read when called: they come and go. */
  const renderer = useMemo(() => {
    // (T175) every state for one of the copy's sections is the copy's too: the one pathway, written twice
    const toCopy: CopyTarget = {
      set: (id, s) => { void copyRef.current?.setFeatureState({ id }, s as never).catch(report); },
      // (T345) a key is unset by a null write, as on the atlas (`adapters/native.ts` `raw.remove`): MapLibre Native drops
      // all but a frame's first per-feature removal in a source and applies removals after sets
      remove: (id, key) => {
        const src = copyRef.current;
        if (src) void (key === undefined ? src.removeFeatureState({ id }) : src.setFeatureState({ id }, { [key]: null } as never)).catch(report);
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
              remove: (id, key) => { void (key === undefined ? copyRef.current?.removeFeatureState({ id })
                : copyRef.current?.setFeatureState({ id }, { [key]: null } as never))?.catch(report); } },
    // what a clear took that was not the engine's (the highlight, pushed data) is sent again by the effects below
    restore: () => { renderer.forget(); setGen((g) => g + 1); },
    ready: styled && drawn, theme, atlasUrl: at.atlas, report, status, standing, lakes, salmon, method, runs,
  });
  const cv = colouring.values;
  /*
   * (T287) THE STYLE-BORNE VALUES BY CONTENT, not by object: an applied update (or a channel re-set) hands equal values
   * in new objects, and a style swap drops every feature-state and relabels the map (~150 ms on the JS thread, then the
   * map's own rebuild) for nothing. A few kB of label lists.
   */
  const valuesKey = useMemo(() => JSON.stringify([cv.status, cv.salmon, cv.method, cv.stocked, cv.survey,
                                                   cv.standing?.basins ?? null, cv.runs?.key ?? null]),
    [cv.status, cv.salmon, cv.method, cv.stocked, cv.survey, cv.standing?.basins, cv.runs?.key]);

  /*
   * THE STYLE. Keyed on what changes how the map LOOKS — never on `gauges` (read through a ref: a feed tick goes to
   * the `gauges` source below, not into a 225 ms style swap) and never on a caller's new-but-equal objects.
   */
  const gaugesNow = useRef(gauges);
  gaugesNow.current = gauges;
  const look = useMemo(
    () => nativeStyle({ at, theme, view, modes, hide, show, groups, basemap: basemap ?? "map", focus: !!focus,
                        gauges: gaugesNow.current ?? EMPTY_FC, areaData: JSON.parse(areaKey) as Data,
                        values: cv }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [at.atlas, at.basemap, at.glyphs, at.sprite, at.assets, at.outside, theme, view, JSON.stringify(modes ?? {}),
     JSON.stringify(hide ?? []), JSON.stringify(show ?? []), JSON.stringify(groups ?? {}), basemap, areaKey, valuesKey, !!focus]);
  // (T317) and the boundary mark label's box, named by its colours (the binding adds a name once and never replaces it)
  const markBoxName = `${MARK_BOX_IMAGE}.${furniture.card}.${furniture.ink}`.replace(/#/g, "");
  // (T316) and the spot faces and their name strip, named by the spot colours
  const spotColours = spots?.colours;
  const spotColourKey = spotColours ? `${spotColours.mark}|${spotColours.ground}` : "";
  const images = useMemo(() => {
    const g = markBoxImage(furniture.card, furniture.ink);
    const spotImgs: Record<string, unknown> = {};
    if (spotColours) {
      for (const f of spotImages(spotColours))
        spotImgs[f.name] = { source: { uri: pngDataUri(f.width, f.height, f.data), scale: f.pixelRatio } };
      const b = spotBoxImage(spotColours);
      spotImgs[spotBoxName(spotColours)] = { source: { uri: pngDataUri(b.width, b.height, b.data), scale: b.pixelRatio } };
    }
    return { ...nativeImages(theme), ...spotImgs,
             [markBoxName]: { source: { uri: pngDataUri(g.width, g.height, g.data), scale: g.pixelRatio },
                              stretchX: g.stretchX, stretchY: g.stretchY, content: g.content } };
  }, [theme, markBoxName, spotColourKey]);  // eslint-disable-line react-hooks/exhaustive-deps
  useEffect(() => { for (const e of look.errors) report(e); }, [look, report]);
  // (T420) a lab build's switches (`lab.ts`): today's style, the same object, unless one is flipped
  const lab = useLab();
  const shown = useMemo(() => labStyle(look.style, lab), [look, lab]);
  /*
   * (T566) THE CALLER'S LOOK, SAID WHEN DRAWN (`MapProps.lookKey`, `look-drawn.ts`): a key handed with a new style waits
   * for that style to load and then for a full frame — the frame the binding's held picture lifts at (T451) — so the
   * caller's key changes with the picture, never half a second before it. `lookWaiting` listens to full frames only while
   * a key waits.
   */
  const looks = useRef(new NativeLookTracker());
  const shownWas = useRef<unknown>(null);
  const lookSaid = useRef(onLookDrawn);
  lookSaid.current = onLookDrawn;
  const [lookWaiting, setLookWaiting] = useState(false);
  const sayLook = useCallback((key: string | null) => {
    if (key !== null) lookSaid.current?.(key);
    setLookWaiting(looks.current.waiting());
  }, []);
  useEffect(() => {
    if (lookKey === undefined) return;
    const changed = shownWas.current !== shown;
    shownWas.current = shown;
    sayLook(looks.current.handed(lookKey, changed));
  }, [shown, lookKey, sayLook]);
  // (T451, the pan probe's build) each style handed to the binding, and what changed since the last one
  const lookWas = useRef<Record<string, string> | null>(null);
  useEffect(() => {
    if (!panProbe.on()) return;
    const now: Record<string, string> = { view: String(view), theme, modes: JSON.stringify(modes ?? {}), hide: JSON.stringify(hide ?? []),
      show: JSON.stringify(show ?? []), groups: JSON.stringify(groups ?? {}), basemap: String(basemap), area: areaKey, values: valuesKey };
    const was = lookWas.current;
    lookWas.current = now;
    panProbe.styleHanded(was ? Object.keys(now).filter((k) => now[k] !== was[k]).join("+") || "same" : "first");
  }, [look]);  // eslint-disable-line react-hooks/exhaustive-deps

  const gaugeData = useMemo(() => JSON.parse(gauges ?? EMPTY_FC) as GeoJSON.FeatureCollection, [gauges]);
  const camera0 = useRef({ center: [initial.lon, initial.lat] as [number, number], zoom: initial.zoom });
  /** Where the camera is, for the scale bar: the latitude and zoom it last settled at. */
  const [scaleAt, setScaleAt] = useState({ lat: initial.lat, zoom: initial.zoom });
  /** (T674) What the map shows, for the spot marks' buttons: where the camera settled and the map's own size. */
  const [seenAt, setSeenAt] = useState({ lon: initial.lon, lat: initial.lat, zoom: initial.zoom, width: 0, height: 0 });
  /** (T145) The spot marks drawn, and the zoom they were placed at: what a tap is checked against (`spotUnder`). */
  const spotsAt = useRef<{ marks: readonly SpotMarker[]; zoom: number }>({ marks: [], zoom: initial.zoom });
  const spotsNow = useRef(spots);
  spotsNow.current = spots;

  // The FIRST attach is news (the binding mounts the source only once the map is ready, maybe after the style
  // loaded); later ones are the binding re-creating its handle (`useImperativeHandle` with no deps), which must not
  // re-render this map, or each render would re-attach and render again. A style swap is `onStyle`'s business.
  const attachedOnce = useRef(false);
  const atlasAttached = useCallback((r: VectorSourceRef | null) => {
    atlasRef.current = r;
    let tag: number | null = null;
    try { tag = r?.nativeTag() ?? null; } catch { /* not mounted yet: its tag is learnt on the next attach */ }
    gone.current.held(tag);
    if (r && !attachedOnce.current) { attachedOnce.current = true; setGen((g) => g + 1); }
  }, []);
  // (T74) the atlas asked for its tiles' ids — again after every style load (the binding may re-create the source)
  // only once the map has drawn: before that the source's native view may not be resolvable by its tag yet
  // (T420) keyed on the STABLE `attached`, never the hook's result object (new each render): keyed on that, every render
  // — one per camera settle — asked the binding to watch again, and each watch re-sent every tile it had requested with
  // all its ids (~28 a second while circling one place on the stressed emulator, every label of them redundant)
  const { attached: colouringAttached, swapped: colouringSwapped } = colouring;
  useEffect(() => { if (atlasRef.current && styled && drawn) colouringAttached(); }, [gen, styled, drawn, colouringAttached]);

  // PUSHED PER-FEATURE DATA that is not a colouring (the engine's keys are refused, as on the web: one pathway); a
  // value a feature already holds is not sent again (`onceSink`)
  useEffect(() => {
    if (!styled || !drawn || !atlasRef.current) return;
    const h = renderer.states(() => []);
    panProbe.src("data");
    for (const [layerId, values] of Object.entries(withoutEngineKeys(sectionData, report))) {
      try { adapter.current.setData(h, layerId, values); } catch (e) { report(e); }
    }
  }, [sectionData, gen, styled, drawn, report, renderer]);

  // THE SELECTION: only the highlight's keys (`selected`, and the rest of the water's `water`) are cleared, per
  // feature, from the ids lit last time
  const wasLit = useRef<SectionKey[]>([]);
  /** (T508) what the lit effect last wrote (`litText` at the style generation it wrote into), for `onDrawn` */
  const litWritten = useRef<string | null>(null);
  // (T499) keyed on WHAT is lit (`litKey`), never on the arrays: the same ids in new arrays (a caller's re-render) clear
  // nothing and light nothing — on a phone that clear and relight drew a frame with the water unlit
  const litText = litKey(highlight, water, selection, near, inSeason);
  const litNow = useRef({ highlight, water, near, inSeason });
  litNow.current = { highlight, water, near, inSeason };
  useEffect(() => {
    const { highlight, water, near, inSeason } = litNow.current;
    if (!styled || !drawn || !atlasRef.current || !highlight) return;
    panProbe.src("lit");
    adapter.current.highlight(renderer.states(() => wasLit.current), [...highlight], [...(water ?? [])], selection,
                              [...(near ?? [])], [...(inSeason ?? [])]);
    wasLit.current = [...highlight, ...(water ?? []), ...(near ?? []), ...(inSeason ?? [])];
    litWritten.current = `${litText}|${gen}`;
    // (T716, test bed only) when a lit set reached the map, and the frame after: the tap cue's arrival (`[cue]`, epoch ms)
    if ((globalThis as { __openProbe?: boolean }).__openProbe) {
      const raf = (globalThis as { requestAnimationFrame?: (f: () => void) => unknown }).requestAnimationFrame;
      const n = highlight.length;
      raf?.(() => console.log(`[cue] ${JSON.stringify({ wall: clockNow(), n })}`));
    }
  }, [litText, selection, gen, styled, drawn, report, renderer]);

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
  /**
   * (T348, problem report 98935871: "the number of reaches coloured doesn't change when zoom or scroll changes") A
   * settle over tiles the map already holds need not render "fully" again, so nothing asked and the legend's count and
   * marks kept the old view's. The settle asks itself when no full render has by `SETTLE_ASK_MS`; one that comes first
   * asks (and clears `stale`), so it stays one query per camera move.
   */
  const settleAsk = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (settleAsk.current) clearTimeout(settleAsk.current); }, []);
  /** (T411, test bed only) which of the water's sections the copy AND the atlas both draw now (`camProbe.copy`) */
  const copyDraw = useRef(async () => {
    const m = mapRef.current;
    if (!camProbe.on() || !m || !copy.current.ids.size) return;
    const ids = new Set(copy.current.ids);
    const drawn = async (layers: string[]) => new Set(visibleSections(
      await m.queryRenderedFeatures({ layers }).catch((e: unknown) => { report(e); return []; })).filter((id) => ids.has(id)));
    const byCopy = await drawn([COPY_LINE]), byAtlas = await drawn(["stream"]);
    camProbe.copy("copydraw", { zoom: camNow.current.zoom, both: [...byCopy].filter((id) => byAtlas.has(id)).sort(),
                                copyOnly: [...byCopy].filter((id) => !byAtlas.has(id)).sort() });
  });
  /*
   * (T508) DRAWN WHOLE (`MapProps.onDrawn`): a full render that came after the style loaded, after the sections handed
   * to light were written into it, and with the camera at rest where the fit sent it. Re-read at every full render (a
   * ref, refreshed each render of this map); said once per lit + fit pair, and at once to a listener that arrives after.
   */
  const whole = useRef(() => {});
  const wholeSaid = useRef<string | null>(null);
  const drawnNow = useRef(onDrawn);
  drawnNow.current = onDrawn;
  const wholeKey = `${litText}|${fit?.key ?? ""}`;
  whole.current = () => {
    // (test bed only, the open probe's build) why a full render was or was not the whole one
    const why = !styledNow.current || !rendered.current ? "unstyled"
      : highlight && litWritten.current !== `${litText}|${gen}` ? "unlit"
      : fit && (fitted.current !== fit.key || fitMoving.current) ? "moving" : null;
    if (drawnNow.current && (globalThis as { __openProbe?: boolean }).__openProbe)
      console.log(`[whole] ${JSON.stringify({ t: clockNow(), why: why ?? "whole", h: size.current.height })}`);
    if (why) return;
    wholeSaid.current = wholeKey;
    drawnNow.current?.();
  };
  const styledNow = useRef(styled);
  styledNow.current = styled;
  useEffect(() => { if (onDrawn && wholeSaid.current === wholeKey) onDrawn(); }, [onDrawn, wholeKey]);
  /*
   * (T508, emulator) MapLibre Native's "map fully rendered" (`onRendered`) came ONCE, at the map's first draw: the fit's
   * glide and the lit sections after it drew frames with every tile in, but no second "map fully rendered" — a held
   * Fraser waited out its whole hold. So while someone waits (`onDrawn`, not yet said for these props) the map also hears
   * each frame (`lastFull`: whether the last one had every tile in), and a camera coming to rest after a full frame is
   * whole at once.
   */
  const wantWhole = !!onDrawn && wholeSaid.current !== wholeKey;
  const lastFull = useRef(false);
  const onRendered = useCallback(() => {
    if (looks.current.waiting()) sayLook(looks.current.fullFrame());
    panProbe.src("idle");               // (T499) a full render (MapLibre's "map idle")
    rendered.current = true;
    setDrawn(true);
    whole.current();
    void reportVisible();
    void refine.current?.().catch(report);
    void keepCopy.current?.().catch(report);
    void copyDraw.current().catch(report);
  }, [reportVisible, report, sayLook]);

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
        // (T399) EACH SECTION'S OWN CEILING (`copy_until`, read by the copy layers' filter): the zoom it was kept at, or
        // the view's floor for its layer (the Conditions view's z9 for streams). Data only: no style swap mid-flight.
        const floorOf = (id: string) =>
          ((lookNow.current.style.layers as { id: string; minzoom?: number }[]).find((l) => l.id === id)?.minzoom) ?? 0;
        if (fresh.length) {
          // what the atlas features hold now goes on the copy first (later writes are mirrored as they are made)
          for (const id of fresh) {
            const held = await atlasRef.current?.getFeatureState({ id, sourceLayer: c.layerOf(id)! }).catch(() => null);
            const st = c.fresh(id, (held ?? {}) as Record<string, unknown>);
            if (st) void copyRef.current?.setFeatureState({ id }, st as never).catch(report);
          }
          setCopyData(c.collection(floorOf));
        }
        if (camProbe.on()) {
          // a z<13 atlas tile is a 2,048 grid (tippe LOW_DETAIL): the zoom of the tile each piece was cut from
          const grids = (g: { type?: string; coordinates?: unknown } | null | undefined) => {
            const c = Array.isArray(g?.coordinates) ? g!.coordinates as unknown[] : [];
            const pieces = g?.type === "LineString" || g?.type === "Polygon" ? [c] : c;
            return [...new Set(pieces.map((p) => pieceGridZoom(p)))].sort();
          };
          const foundBy: Record<number, (number | null)[]> = {};
          for (const f of found) for (const z of grids(f.geometry)) (foundBy[f.id] ??= []).includes(z) || foundBy[f.id]!.push(z);
          camProbe.copy("copy", { zoom: at0.zoom, fresh,
            found: Object.entries(foundBy).map(([id, zs]) => ({ id: Number(id), grid: zs.sort() })),
            kept: c.collection(floorOf).features.map((f) => ({ id: f.id, until: f.properties[COPY_UNTIL], grid: grids(f.geometry) })) });
        }
        const next = clamps.current
          ? probeZoom({ zoom: at0.zoom, found: found.some((f) => lineIds.current.has(f.id)),
                        kept: c.keptOf(lineIds.current), rounds, touched: copyTouched.current })
          : null;
        if (next !== null) {
          rounds += 1;
          camProbe.move("probe", rounds, size.current.height);
          renderer.easeTo(zoomAround(at0, lineAt.current, at0.zoom, next), { duration: cameraMs(300) });
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
  /** (test bed only) the map's rect in the window, dp, and the pending `[target]` search — see `findTarget` */
  const winRect = useRef<{ x: number; y: number; w: number; h: number } | null>(null);
  const targetAt = useRef<ReturnType<typeof setTimeout> | null>(null);
  const findTarget = useRef<(() => Promise<void>) | null>(null);
  const lit = useRef(highlight);
  lit.current = highlight;
  const refine = useRef<(() => Promise<void>) | null>(null);
  const fitted = useRef<string | null>(null);
  /** (T443) frame the current fit's box again (the map's size changed), and whether the reader has moved the camera */
  const refit = useRef<(() => void) | null>(null);
  const fitTouched = useRef(false);
  /** (T508) a fit (or refit) sent and the camera not yet at rest from it: the map is not drawn whole (`onDrawn`) */
  const fitMoving = useRef(false);
  const fitSentAt = useRef(0);
  const fitGlide = useRef(0);
  const fitDone = useRef<ReturnType<typeof setTimeout> | null>(null);
  useEffect(() => () => { if (fitDone.current) clearTimeout(fitDone.current); }, []);
  // (T303) My location: the camera goes to the fix each time the button asks and, while following, to each new fix (as
  // the web's `useMe`). The dot is ours (`me.ts`), not MapLibre's UserLocation, which drew nothing on Android.
  const centred = useRef(0);
  useEffect(() => {
    if (!me?.on || !me.fix) return;
    const pressed = me.centre !== centred.current;
    centred.current = me.centre;
    if (!pressed && !me.following) return;
    renderer.easeTo({ lon: me.fix.lon, lat: me.fix.lat, zoom: pressed && me.zoom != null ? me.zoom : Math.max(camNow.current.zoom, 15) },
                    { duration: cameraMs(pressed ? 500 : 300) });
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [me?.centre, me?.on, me?.following, me?.fix?.lat, me?.fix?.lon]);
  const meFix = me?.on ? me.fix : null;
  const meJson = useMemo(() => meData(meFix), [meFix?.lat, meFix?.lon]);  // eslint-disable-line react-hooks/exhaustive-deps
  const meLook = useMemo(() => mePaint(theme, meFix), [theme, meFix?.lat, meFix?.accuracy]);  // eslint-disable-line react-hooks/exhaustive-deps
  /** the reader's own pan or pinch stops following (our eases are not user interaction); the dot stays */
  const meNow = useRef(me);
  meNow.current = me;
  // (test bed only) the camera link puts this map where a scenario asks, at once (`native/camProbe.ts`)
  useEffect(() => camProbe.mount((c) => renderer.easeTo(c, { duration: 0 })), [renderer]);
  const pinsNow = useRef(pins);
  pinsNow.current = pins;
  /** (T621, G19) a titled pin's words and where the press was, until the next press or pan */
  const [callout, setCallout] = useState<{ title: string; x: number; y: number } | null>(null);
  const onRegionWillChange = useCallback((e: RegionEvent) => {
    const ne = e.nativeEvent;
    if (ne?.userInteraction && meNow.current?.following) meNow.current.onRelease?.();
  }, []);
  useEffect(() => {
    // (T443) not before the map's first style has loaded: a fit sent while the native map was still being made was lost
    // to its initial camera (test bed: a water's map opened on the last water's place, its fit and refit both dropped)
    if (!fit || !camRef.current || !styled || fitted.current === fit.key) return;
    fitted.current = fit.key;
    let rounds = 0;
    let alive = true;
    /** The camera moved since the last measure: a render with nothing new is not a reason to measure again. */
    let moved = false;
    /** (T443) the box last framed, framed again when the map's size changes (`refit`) */
    let last: Bbox | null = null;
    fitTouched.current = false;
    const go = (b: Bbox) => {
      last = b;
      const { width, height } = size.current;
      const p = Math.max(8, Math.round(Math.min(56, (width || 400) * 0.12, (height || 800) * 0.12)));
      moved = true;
      // (T508) moving until the glide's time is up: the camera's first settle report right after a fit is sent is the
      // camera it starts from (emulator: 4 ms after the fit, the glide itself 450 ms on); a glide that goes nowhere (the
      // camera already there) reports no settle, so its time running out ends it too
      const glide = cameraMs(450);
      fitMoving.current = true;
      fitSentAt.current = clockNow();
      fitGlide.current = glide;
      if (fitDone.current) clearTimeout(fitDone.current);
      fitDone.current = setTimeout(() => { fitDone.current = null; fitMoving.current = false; if (lastFull.current) whole.current(); }, glide + 100);
      camProbe.move(rounds ? "refine" : "fit", rounds, height);
      // the binding's fitBounds has no maxZoom, and the camera now goes to z18: widen the box to what the limit shows
      // (T443) a function of the box and the map's size only: a part is framed the same however it was reached
      const limit = fitZoomLimit(fit, fitZoomOf(b, width, height, p));
      renderer.fitBounds(fitAtMost(b, limit, width, height, p), { padding: p, duration: cameraMs(450) });
    };
    /*
     * (T443, owner on 0.6.7: "the first open frames the part differently from ▲ then ▼") THE MAP'S SIZE CHANGED AFTER THE
     * FIT: framed again, as the web's `useFit` refit does (`followSize`). A water's first fit is sent in the render that
     * also grows the bar under the map (its names and steps arrive with the same view model), so it framed the part in
     * the taller map the loading bar left; a step later framed it in the real one. Not after the reader moved the camera.
     */
    refit.current = () => { if (alive && last && !fitTouched.current) go(last); };
    // (T48) an exact camera to return to (search's cancel): put back as it was, nothing measured
    if (fit.camera) { renderer.easeTo(fit.camera, { duration: cameraMs(450) }); refine.current = null; refit.current = null; return () => { alive = false; }; }
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

  /*
   * (test bed only: the open probe's build, `__openProbe`) THE TAP TARGET of the selected water: once the camera rests,
   * points on a 24 dp grid (nearest the middle first, clear of the top bar and the bottom chrome) are hit-tested as a tap
   * would be (`renderer.press`) until one answers with a highlighted section; logged as `[target] {x, y, win}` (dp, in
   * the map view and the map's rect in the window). A scenario taps there: never a colour hunted on screen, which the
   * "has its own regulations" lines share with the highlight. (T478) Never a point with ANOTHER water's still water under
   * the box as well: the search fires while the water is open and lit, and a lit river answered at a lake's tip; the
   * scenario taps after Back, unlit, and the same point opened the lake. (T689) The lit water's own still water is no
   * reason to skip: a lake is all still water (`isTapTarget`).
   */
  findTarget.current = async () => {
    const STILL_WATER = TAP_GROUPS.find((g) => g.as === "lake")!.layers;
    const ids = new Set((lit.current ?? []).map(Number).filter(Number.isFinite));
    const { width: w, height: h } = size.current;
    if (!ids.size || !w || !h || !rendered.current) return;
    const pts: [number, number][] = [];
    for (let y = Math.round(h * 0.18); y < h * 0.78; y += 24) for (let x = 24; x < w - 24; x += 24) pts.push([x, y]);
    pts.sort((a, b) => Math.hypot(a[0] - w / 2, a[1] - h / 2) - Math.hypot(b[0] - w / 2, b[1] - h / 2));
    // (T716) the nearest point that answers with ANOTHER stream (no still water under it): a tap on a neighbour, most
    // often an unnamed tributary, for the open bench's never-opened case; logged once per search, beside `[target]`
    let other = false;
    for (const [x, y] of pts.slice(0, 400)) {
      const hit = await renderer.press(x, y).catch(() => null);
      if (hit && !other && !ids.has(Number(hit.id)) && hit.layer !== "lake") {
        const still = await mapRef.current?.queryRenderedFeatures(tapBox(x, y), { layers: [...STILL_WATER] })
          .catch((e: unknown) => { report(e); return null; }) ?? null;
        if (still !== null && still.length === 0) {
          other = true;
          console.log(`[target-other] ${JSON.stringify({ x, y, id: hit.id, win: winRect.current })}`);
        }
      }
      if (!hit || !ids.has(Number(hit.id))) continue;
      const still = await mapRef.current?.queryRenderedFeatures(tapBox(x, y), { layers: [...STILL_WATER] })
        .catch((e: unknown) => { report(e); return null; }) ?? null;
      if (isTapTarget(hit, ids, still)) {
        console.log(`[target] ${JSON.stringify({ x, y, id: hit.id, win: winRect.current })}`);
        return;
      }
    }
    console.log(`[target] ${JSON.stringify({ none: true, win: winRect.current })}`);
  };

  /** (T716) the last tap's lift and what it found (`onTapUp`), for that tap's click */
  const lift = useRef<Lift<PressHit> | null>(null);
  /** A tap: the 24 px box, streams first, the section as a number (`MapRenderer.press`). */
  const onPress = useCallback(async (e: PressEvent) => {
    // READ THE EVENT BEFORE ANY await: React Native recycles it, and `nativeEvent` is null afterwards (N1 §3)
    const ne = e.nativeEvent;
    if (!ne) return;
    // (T286) the open probe's zero (test bed only: `@app/data` openProbe), before the hit test
    // (T716) and the press as an instant (epoch ms): the test bed sets it beside the device clock read just after the
    // injected tap, which is how long the press took to reach JS (MapLibre's single-tap confirmation included)
    const probe = globalThis as { __openProbe?: boolean; __openPressAt?: number; __openPressWall?: number };
    if (probe.__openProbe) { probe.__openPressAt = globalThis.performance?.now?.() ?? clockNow(); probe.__openPressWall = clockNow(); }
    const [x, y] = Array.isArray(ne.point) ? ne.point : [ne.point.x, ne.point.y];
    const [lon, lat] = ne.lngLat;
    // (T716) this tap's lift already asked what is under it (a spot, a mark, an obstacle, the water): its answer, if it
    // decided, stands for the queries below; undecided (or no lift: iOS, a lift elsewhere) asks as before
    const early = await (liftFor(lift.current, x, y, clockNow()) ?? Promise.resolve(null));
    lift.current = null;
    // (T145) a tap on a spot is the spot's: never ALSO the map's — no water opens under it, and the off-spot tap that
    // clears the selection does not clear the one this tap makes. (T316) the marks are map layers: their 44 px target is
    // `spotUnder` at the zoom they were placed at, and the press is `pressSpot`
    const spotHit = spotUnder(spotsAt.current.marks, lat, lon, spotsAt.current.zoom);
    if (spotHit) {
      const sp = spotsNow.current;
      if (sp) pressSpot(spotHit, sp, (c, o) => renderer.easeTo(c, o));
      return;
    }
    // (T317) a boundary mark's 44 px target: the mark's layers, asked first; the tap opens no water under it
    if (!early && marksNow.current?.length && rendered.current) {
      const hit = markHit(await mapRef.current?.queryRenderedFeatures(markBox(x, y), { layers: [MARK_RING, MARK_LABEL] })
        .catch((e: unknown) => { report(e); return []; }) ?? []);   // (T368, Q2) the tap falls through; the host reports it
      if (hit !== null) { pressMarkNow.current?.(hit); return; }
    }
    // (T621, G19) a titled pin says its words on a tap, as the web's popup does; any other tap puts them away
    setCallout(null);
    if (pinsNow.current?.some((p) => p.title) && rendered.current) {
      const p = titledPin(await mapRef.current?.queryRenderedFeatures(tapBox(x, y), { layers: [PIN_DOT] })
        .catch((e: unknown) => { report(e); return []; }) ?? [], pinsNow.current);
      if (p) setCallout({ title: p.title, x, y });
    }
    cb.current.onMapPoint?.(lat, lon);
    const press = cb.current.onPressFeature, obstacle = cb.current.onPressObstacle;
    const notFishing = cb.current.onPressNotFishing;
    if ((!press && !obstacle && !notFishing) || !rendered.current) return;       // no renderer to query yet (see `rendered`)
    try {
      // (T173) a fish obstacle mark first: a tap on the mark is the mark's, and opens no water under it
      const ob = !early && obstacle ? await renderer.pressObstacle(x, y) : null;
      if (ob) { obstacle!(ob); return; }
      const hit = early ? early.hit : press ? await renderer.press(x, y) : null;
      if (probe.__openProbe) console.log(`[tap] ${JSON.stringify({ x, y, hit })}`);   // (T478) beside the [target] line
      if (hit) { press!(hit.layer, hit.id, lat, lon); return; }
      // (T292) no water under the tap: a sewage lagoon / tailings pond / dump says what it is
      const nf = notFishing ? await renderer.pressNotFishing(x, y) : null;
      if (nf) notFishing!(nf.kind);
    } catch (err) { report(err); }
  }, [report, renderer]);

  /**
   * (T716) A TAP'S LIFT (the patched binding's `onTapUp`, Android): what the tap would open, said at once
   * (`onPressStart`) — the click (`onPress`) comes once a double tap is ruled out, 300 ms after the finger went down. The
   * same precedence as the click: a spot, a boundary mark or a fish obstacle mark under the finger is theirs, and no
   * water is said. A failed query is reported and says nothing (the click still asks for itself).
   */
  const onTapUp = useCallback((e: PressEvent) => {
    const ne = e.nativeEvent;          // before any await: React Native recycles the event
    if (!ne) return;
    const start = cb.current.onPressStart;
    if (!start || !cb.current.onPressFeature || !rendered.current) return;
    const [x, y] = Array.isArray(ne.point) ? ne.point : [ne.point.x, ne.point.y];
    const [lon, lat] = ne.lngLat;
    const ask = async (): Promise<LiftAnswer<PressHit> | null> => {
      if (spotUnder(spotsAt.current.marks, lat, lon, spotsAt.current.zoom)) return null;
      try {
        if (marksNow.current?.length) {
          const mk = markHit(await mapRef.current?.queryRenderedFeatures(markBox(x, y), { layers: [MARK_RING, MARK_LABEL] })
            .catch((err: unknown) => { report(err); return []; }) ?? []);
          if (mk !== null) return null;
        }
        // the two asked together: the cue waits on one round trip, not two
        const [ob, hit] = await Promise.all([cb.current.onPressObstacle ? renderer.pressObstacle(x, y) : null, renderer.press(x, y)]);
        if (ob) return null;
        if (hit) start(hit.layer, hit.id);
        return { hit };
      } catch (err) { report(err); return null; }
    };
    // (T716) the click of this same tap reads this answer (`liftFor`) instead of asking again
    lift.current = { x, y, at: clockNow(), answer: ask() };
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
  /** (T420/T401) the pan probe's build (the test bed): frames full/partial and the camera's rest, for "streams drawn" */
  const PAN_PROBE = panProbe.on();

  /** (T50) where each mark's label is centred, kept inside the map as the web's `markers.ts` does (`labelShift`):
   *  measured where the camera settles, from each ring's screen x, and moved as a point of the marks' source (T317) */
  const [labelAt, setLabelAt] = useState<{ key: string; at: readonly ([number, number] | null)[] }>({ key: "", at: [] });
  const shiftMarks = useCallback(async () => {
    const ms = marksNow.current ?? [], w = size.current.width, map = mapRef.current;
    if (!ms.some((k) => k.label) || !w || !map) return;
    const next = await shiftedLabels(ms, w, (lon, lat) => map.project([lon, lat]).then((p) => [p[0], p[1]] as [number, number], () => null),
                                     (x, y) => map.unproject([x, y]).then((l) => [l[0], l[1]] as [number, number], () => null));
    const key = JSON.stringify(ms);
    if (JSON.stringify(marksNow.current ?? []) !== key) return;
    setLabelAt((was) => (was.key === key && JSON.stringify(was.at) === JSON.stringify(next) ? was : { key, at: next }));
  }, []);
  useEffect(() => { void shiftMarks(); }, [JSON.stringify(marks ?? []), shiftMarks]);  // eslint-disable-line react-hooks/exhaustive-deps

  const onRegionDidChange = useCallback((e: RegionEvent) => {
    // (T508) the fit's glide at its end (not the camera it started from), and the last frame drawn had every tile in
    if (fitMoving.current && clockNow() - fitSentAt.current >= fitGlide.current * 0.8) fitMoving.current = false;
    if (lastFull.current) whole.current();
    probe?.settled();
    panProbe.settled();
    const ne = e.nativeEvent;          // before anything async, as with a press
    if (ne?.userInteraction) setCallout(null);      // (T621, G19) a pan puts a pin's words away
    if (ne?.center && ne.zoom !== undefined) {
      const b = ne.bounds;
      cb.current.onMoved?.({ lon: ne.center[0], lat: ne.center[1], zoom: ne.zoom,
                             ...(b && b.length === 4 ? { bounds: [b[0], b[1], b[2], b[3]] as const } : {}) });
      camNow.current = { lon: ne.center[0], lat: ne.center[1], zoom: ne.zoom };
      camProbe.settled({ ...camNow.current, user: !!ne.userInteraction });
      // (test bed only, the open probe's build) once the camera rests: a point the selected water answers a tap at
      if ((globalThis as { __openProbe?: boolean }).__openProbe) {
        if (targetAt.current) clearTimeout(targetAt.current);
        targetAt.current = setTimeout(() => { void findTarget.current?.(); }, 1200);
      }
      const next = { lat: ne.center[1], zoom: ne.zoom };
      setScaleAt((was) => (was.lat === next.lat && was.zoom === next.zoom ? was : next));
      const lon = ne.center[0];
      setSeenAt((was) => (was.lon === lon && was.lat === next.lat && was.zoom === next.zoom ? was : { ...was, lon, ...next }));
    }
    void shiftMarks();
    // the next full render reports what is on screen (and measures a fit): the tiles for here may not be in yet
    stale.current = true;
    copyStale.current = true;
    if (settleAsk.current) clearTimeout(settleAsk.current);
    // (T411) …and the water copy's pass with it: a settle over held tiles fires no full render, and the copy kept what
    // the last one saw (the step back to a part already drawn)
    settleAsk.current = setTimeout(() => {
      settleAsk.current = null; void reportVisible();
      // (T508) a settle over tiles already held draws no full render (T348): at rest, nothing more is coming
      whole.current();
      void keepCopy.current?.().catch(report)
        .then(() => { if (camProbe.on()) setTimeout(() => void copyDraw.current().catch(report), 400); });
    }, SETTLE_ASK_MS);
    // (T175) the reader's own pan or pinch: the copy never steps in against it (T443: nor does a refit)
    if (ne?.userInteraction) { copyTouched.current = true; fitTouched.current = true; }
  }, [shiftMarks, reportVisible, report]);

  // a new style dropped every feature-state (`forget`: send them all again) and may draw other features
  const onStyle = useCallback(() => {
    panProbe.styleLoaded();
    looks.current.styleLoaded();
    renderer.forget();
    colouringSwapped();
    copy.current.forgetStates();
    stale.current = true;
    copyStale.current = true;
    setStyled(true);
    setGen((g) => g + 1);
  }, [renderer, colouringSwapped]);
  const onLayout = useCallback((e: LayoutChangeEvent) => {
    const was = size.current;
    size.current = { width: e.nativeEvent.layout.width, height: e.nativeEvent.layout.height };
    const { width, height } = size.current;
    setSeenAt((s) => (s.width === width && s.height === height ? s : { ...s, width, height }));
    // (T443) a fit framed in the old size is framed again in this one
    if (was.width !== size.current.width || was.height !== size.current.height) refit.current?.();
    if ((globalThis as { __openProbe?: boolean }).__openProbe) {
      const t = e.currentTarget as unknown as { measureInWindow?: (f: (x: number, y: number, w: number, h: number) => void) => void };
      t?.measureInWindow?.((x, y, w, h) => { winRect.current = { x, y, w, h }; });
    }
  }, []);

  const scale = metricScale(scaleAt.lat, scaleAt.zoom, SCALE_MAX_WIDTH);
  const spotKey = spotsKey(spots?.points, spots?.selected ?? null);
  const spotMarks = useMemo(() => (spots ? spotMarkers(spots.points, scaleAt.zoom, spots.selected) : []),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [spotKey, scaleAt.zoom]);
  spotsAt.current = { marks: spotMarks, zoom: scaleAt.zoom };
  // (T674) the marks a screen reader is given a button for: the ones on the map now, never a spot the camera has left
  const spotsSaid = useMemo(() => spotsInView(spotMarks, seenAt), [spotMarks, seenAt]);
  const spotJson = useMemo(() => (spotColours ? spotsData(spotMarks, spotColours) : EMPTY_FC_OBJ),
    [spotMarks, spotColourKey]);  // eslint-disable-line react-hooks/exhaustive-deps
  const spotLook = useMemo(() => (spotColours ? spotsLayers(spotColours) : []), [spotColourKey]);  // eslint-disable-line react-hooks/exhaustive-deps
  const marksKey = JSON.stringify(marks ?? []);
  const markJson = useMemo(() => marksData(marks, labelAt.key === marksKey ? labelAt.at : undefined), [marksKey, labelAt]);  // eslint-disable-line react-hooks/exhaustive-deps
  const markLook = useMemo(() => marksLayers(furniture, markBoxName), [furniture, markBoxName]);
  const pinKey = `${marker ? `${marker.lon},${marker.lat}` : ""}|${(pins ?? []).map((p) => `${p.lon},${p.lat},${p.tone ?? ""},${p.title ?? ""}`).join("|")}`;
  const pinJson = useMemo(() => pinsData(marker, pins, furniture.accent), [pinKey, furniture.accent]);  // eslint-disable-line react-hooks/exhaustive-deps
  const pinLook = useMemo(() => pinDotLayer(furniture.card), [furniture.card]);
  // (T499, the pan probe's build) each GeoJSON source handed new data: a redraw with no camera move
  useEffect(() => { panProbe.src("gauges"); }, [gaugeData]);
  useEffect(() => { panProbe.src("copy"); }, [copyData]);
  useEffect(() => { panProbe.src("me"); }, [meJson]);
  useEffect(() => { panProbe.src("pins"); }, [pinJson]);
  useEffect(() => { panProbe.src("spots"); }, [spotJson]);
  useEffect(() => { panProbe.src("marks"); }, [markJson]);
  useEffect(() => { panProbe.src("images"); }, [images]);
  useEffect(() => { panProbe.src("style"); }, [shown]);
  return (
    <>
      <MLMap
        ref={mapRef}
        style={[{ flex: 1 }, style]}
        onLayout={onLayout}
        mapStyle={shown as never}
        // G1: one gesture policy for both maps — pan, pinch, double-tap zoom; no rotate, no tilt, no compass
        {...nativeMapGestures()}
        // (T424) the binding's own attribution "i" is OFF: the credits are `MapCredits` below, the web's own component. A
        // bare map is an illustration: no furniture, no credits (the page that carries it credits the data, MapProps.bare)
        attribution={false}
        // (T448) at most 2 device pixels per dp, like the web (`pixel-ratio.ts`): the patched binding draws into a smaller
        // surface the compositor scales up (Android) — fewer pixels filled, the same scale, gestures and taps
        maxPixelRatio={lab.pixelCap ? MAX_PIXEL_RATIO : 0}
        // (T401) parent tiles loaded as placeholders while a tile loads (patched binding, Android; iOS ignores it)
        prefetchZoomDelta={LAB_ON ? lab.prefetch : PREFETCH_ZOOM_DELTA}
        logo={false}
        // OURS is drawn below (`metricScale`): the binding's is top-left only on Android, in the device's units. But
        // the binding's must EXIST on Android: switched off, the map's surface left a black hole over the next screen
        // when the map unmounted (emulator, N2 review; bisected to this one prop). So it is on, and parked off screen
        scaleBar
        scaleBarPosition={OFF_SCREEN}
        onPress={onPress as never}
        onTapUp={onPressStart && onPressFeature ? onTapUp as never : undefined}
        onLongPress={onLongPress ? onLongPressed as never : undefined}
        onRegionDidChange={onRegionDidChange as never}
        onRegionWillChange={probe || me?.on || PAN_PROBE ? ((e: RegionEvent) => { probe?.moving(); panProbe.moving(); onRegionWillChange(e); }) as never : undefined}
        onDidFinishRenderingFrame={probe || PAN_PROBE || wantWhole
          ? (() => { lastFull.current = false; probe?.frame(); if (PAN_PROBE) panProbe.frame(false); }) as never : undefined}
        // (T420/T401, test bed only) a frame with every tile in: the pan probe's "streams drawn"; (T508) and while someone
        // waits for this map to be drawn whole, each such frame is asked whether it is the whole one
        onDidFinishRenderingFrameFully={PAN_PROBE || wantWhole || lookWaiting
          ? (() => { lastFull.current = true; if (PAN_PROBE) panProbe.frame(true); if (PAN_PROBE || wantWhole) whole.current();
                     if (looks.current.waiting()) sayLook(looks.current.fullFrame()); }) as never : undefined}
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
        {/* (T303) your location: the accuracy circle, then the dot over it, above every map layer */}
        <GeoJSONSource id={ME_SOURCE} data={meJson}>
          <Layer id="me-accuracy" type="circle" paint={meLook.accuracy as never} />
          <Layer id="me-dot" type="circle" paint={meLook.dot as never} />
        </GeoJSONSource>
        {/* (T317) the "where I tapped" marker and a panel's named points: dots drawn by the map, not view annotations */}
        <GeoJSONSource id={PIN_SOURCE} data={pinJson as never}>
          <Layer {...(pinLook as unknown as LayerSpec)} />
        </GeoJSONSource>
        {/* (T131) YOUR SPOTS: `spotMarkers` at the zoom the camera settled at — (T316) map layers, not view annotations,
            so they move with the map; a tap is the map's (`onPress`: `spotUnder` → `pressSpot`) */}
        <GeoJSONSource id={SPOT_SOURCE} data={spotJson as never}>
          {spotLook.map((l) => <Layer key={l.id} {...(l as unknown as LayerSpec)} />)}
        </GeoJSONSource>
        {/* THE BOUNDARY MARKS (P15): the ring (filled when open) and its label above it in a box — map layers (T317), so
            they move with the map; a tap is the layers' (`onPress`, `markHit`) */}
        <GeoJSONSource id={MARK_SOURCE} data={markJson as never}>
          {markLook.map((l) => <Layer key={l.id} {...(l as unknown as LayerSpec)} />)}
        </GeoJSONSource>
      </MLMap>
      {/* (T316) the spot marks' buttons, for a screen reader only, as the boundary marks' below; (T674) only for the
          marks on the map now (`spotsInView`): a spot the camera has left is neither announced nor reachable */}
      {spots && spotsSaid.length ? (
        <View style={{ position: "absolute", left: 0, top: 0, width: 1, height: 1, overflow: "hidden" }}>
          {spotsSaid.map((mk) => (
            <Pressable key={mk.key} testID={`spot-mark:${mk.key}`} accessibilityRole="button" aria-label={mk.a11y}
                       onPress={() => pressSpot(mk, spots, (c, o) => renderer.easeTo(c, o))}>
              <Text>{mk.label ?? ""}</Text>
            </Pressable>))}
        </View>) : null}
      {/* (T317) the marks' buttons, for a screen reader only: 1 px, out of the way, never moved with the map */}
      {marks?.length ? (
        <View style={{ position: "absolute", left: 0, top: 0, width: 1, height: 1, overflow: "hidden" }}>
          {marks.map((k, i) => (
            <Pressable key={i} onPress={() => onPressMark?.(i)} accessibilityRole="button" aria-label={k.a11y}>
              <Text>{k.label ?? ""}</Text>
            </Pressable>))}
        </View>) : null}
      {zoomControls ? (
        // (T621, G15) the + / − a small map in a scrolling sheet owes a reader (MapProps.zoomControls): the web has them from
        // MapLibre's NavigationControl, top-right, square, a hard ink edge. One level a press, inside the camera's limits.
        <View accessibilityLabel="zoom" style={{ position: "absolute", right: 10, top: 10 + insetTop, borderWidth: 1.5, borderColor: furniture.ink,
                                                backgroundColor: furniture.card }}>
          {([["Zoom in", "+", 1], ["Zoom out", "\u2212", -1]] as const).map(([label, glyph, dir]) => (
            <Pressable key={label} accessibilityRole="button" aria-label={label} testID={`zoom:${dir}`}
                       style={{ width: 36, height: 36, alignItems: "center", justifyContent: "center",
                                borderTopWidth: dir === -1 ? 1.5 : 0, borderColor: furniture.ink }}
                       onPress={() => renderer.easeTo({ ...camNow.current, zoom: zoomStep(camNow.current.zoom, dir) }, { duration: 250 })}>
              <Text style={{ color: furniture.ink, fontSize: 20, lineHeight: 22, fontFamily: "Archivo_600SemiBold" }}>{glyph}</Text>
            </Pressable>))}
        </View>) : null}
      {callout ? (
        // (T621, G19) the web's popup: the pin's words in a card above the press, never over a tap (it takes none)
        <View pointerEvents="none" accessibilityLiveRegion="polite" testID="pin-callout"
              style={{ position: "absolute", left: Math.max(8, callout.x - 90), top: Math.max(8, callout.y - 64), width: 180,
                       paddingHorizontal: 8, paddingVertical: 5, backgroundColor: furniture.card,
                       borderWidth: 1.5, borderColor: furniture.ink }}>
          <Text style={{ color: furniture.ink, fontSize: 12, lineHeight: 15, fontFamily: "Archivo_600SemiBold" }}>{callout.title}</Text>
        </View>) : null}
      {/* (T424) THE CREDITS: the web's own component, bottom-right (Esri's "Powered by Esri" beside it while its imagery is
          drawn, T275); the scale bar is bottom-LEFT, so the two never meet */}
      {!bare && <MapCredits look={{ view, modes, groups, basemap }} chrome={furniture} insetBottom={insetBottom} onOpenLink={onOpenLink}
                            onLongPress={LAB_ON ? onOpenLab : undefined} />}
      {bare || !SHOW_SCALE ? null : (
        // THE WEB'S SCALE BAR (controls.css `.maplibregl-ctrl-scale`): bottom-left (T424), a RULER — a hairline with a tick at
        // each end, the distance above its left end, a paper halo and no box — so the bar's LENGTH is the measurement
        // and the number only names it.
        // A SIBLING of the map, positioned in the map's own container — NOT a wrapper View around the map: wrapped,
        // the map's surface left a black rectangle over the next screen on Android when it unmounted (emulator, N2
        // review; the unwrapped map does not)
        <View pointerEvents="none" accessibilityLabel="scale"
              style={{ position: "absolute", left: GEOMETRY.scaleLeft, bottom: GEOMETRY.scaleBottom + insetBottom,
                       width: Math.round(scale.width) }}>
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
