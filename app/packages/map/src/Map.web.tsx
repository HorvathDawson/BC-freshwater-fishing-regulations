/**
 * The web map: maplibre-gl, reading PMTiles by byte range.
 *
 * This is the ONLY component in the workspace that renders a map SDK (layers.json), and it is
 * deliberately thin. It creates the map, hands it the composed style, and forwards the
 * adapter's calls. Everything about how water is coloured lives in the generated style and
 * the view definitions — not here — because a colour decided in a component is a colour
 * the native map will not have.
 *
 * ONE HOOK PER CONCERN, in `web/` (T62): the map itself (`useMapInstance`), the basemap labels confined to BC
 * (`outsideMask`), the look and pushed data (`useLook`), what the reader does (`useInput`), the DOM markers
 * (`markers`: map layers since T317), the highlight (`useHighlight`), the spots (`useSpots`), the colouring channels (`useChannels`), the fit
 * (`useFit`), the gauges and the route overlay (`useOverlays`), the water copy (`useWaterCopy`) and the visible set
 * (`useVisible`). They are CALLED IN THIS ORDER ON PURPOSE: React runs effects in the order they are declared, and the
 * map has to exist (`useMapInstance`) before anything that writes to it.
 *
 * Renders a plain <div>, which is correct even inside a react-native-web tree: maplibre
 * needs a real DOM node to attach a canvas to, and on this platform RNW is producing divs
 * anyway. The native renderer is a different file for exactly this reason.
 */
import { useEffect, useMemo, useRef } from "react";
import "maplibre-gl/dist/maplibre-gl.css";
import "./controls.css";
import { mapChrome } from "./chrome";
import { MapCredits } from "./MapCredits";
import { LAB_ON } from "./lab";
import { setReducedMotion } from "./motion";
import type { MapProps } from "./map-props";
import { useMapInstance } from "./web/useMapInstance";
import { useLabelsInBC, useOutsideMask } from "./web/outsideMask";
import { useLook, usePushedData } from "./web/useLook";
import { useHoverCursor, useLongPress, useMapPoint, useMoved, usePressFeature } from "./web/useInput";
import { useMarks, usePins } from "./web/markers";
import { useHighlight } from "./web/useHighlight";
import { useSpots } from "./web/useSpots";
import { useChannels } from "./web/useChannels";
import { useFit } from "./web/useFit";
import { useGauges, useTrace } from "./web/useOverlays";
import { useWaterCopy } from "./web/useWaterCopy";
import { useDrawn, useRegion, useVisible } from "./web/useVisible";
import { useWebMapRefs } from "./web/refs";
import { useMe } from "./web/useMe";
import { useFirstSeen } from "./web/firstSeen";

/**
 * (T409, owner 2026-11-05) BUILT WHEN FIRST SEEN: until the map's box is seen (`web/firstSeen.ts`: under the Shell's
 * opacity-0 warm map behind a water's own page, at any width) only an empty box of the same size is drawn; then
 * the one map below mounts with the props of that moment — the same mount a map shown at once gets.
 */
export function Map(props: MapProps) {
  const probe = useRef<HTMLDivElement | null>(null);
  const seen = useFirstSeen(probe);
  if (seen) return <SeenMap {...props} />;
  return <div ref={probe} data-map-unbuilt="" style={{ position: "absolute", inset: 0, ...(props.style as object) }} />;
}

function SeenMap({ at, theme, view, modes, groups, hide, show, initial, data, onPressFeature, onPressStart, onPressObstacle, onPressNotFishing,
                     chrome,
                      onError, onMoved, onMapPoint, onLongPress, onOpenLab, highlight, inSeason, water, waterLine, selection = "outline", marker,
                      style,
                      gauges, onVisible, pins, bare, fit, basemap, trace, onImageryError,
                      zoomControls, status, standing, lakes, salmon, method, runs, marks, onPressMark, spots, me, insetTop = 0, insetBottom = 0, onRegion, onOpenLink, focus, near, onDrawn, lookKey, onLookDrawn }: MapProps) {
  const refs = useWebMapRefs(onError);
  // (T243) the camera's own glides jump while the OS asks for reduced motion (`motion.ts`)
  useEffect(() => {
    const mq = typeof matchMedia === "function" ? matchMedia("(prefers-reduced-motion: reduce)") : null;
    if (!mq) return;
    const on = () => setReducedMotion(mq.matches);
    on();
    mq.addEventListener?.("change", on);
    return () => mq.removeEventListener?.("change", on);
  }, []);
  // Confines the BASEMAP's labels to British Columbia — see `useOutsideMask`. Absent on the
  // first frame and present a moment later, which is why it is applied by an effect after the map is made
  // rather than only in the constructor.
  const outsideData = useOutsideMask(at.outside);
  /** MapLibre's own controls and markers, in this theme's chrome unless told otherwise. */
  const furniture = useMemo(() => chrome ?? mapChrome(theme), [chrome, theme]);

  useMapInstance(refs, { at, theme, modes, gauges, initial, bare, zoomControls, onError, onImageryError });
  useLabelsInBC(refs.map, outsideData);
  useLook(refs, { view, theme, modes, groups, hide, show, basemap, focus }, { lookKey, onLookDrawn });
  usePushedData(refs, data);
  useMoved(refs, onMoved);
  useLongPress(refs, onLongPress, at);
  useMapPoint(refs, onMapPoint);
  useMe(refs, me, theme);
  usePins(refs, marker, pins, furniture);
  const lit = useHighlight(refs, { highlight, water, selection, near, inSeason });
  useMarks(refs, marks, onPressMark, furniture);
  const spotLayer = useSpots(refs, spots, at);
  useChannels(refs, lit, { at, theme, data, status, standing, lakes, salmon, method, runs });
  const traceBox = useFit(refs, fit, highlight);
  useGauges(refs, gauges);
  useTrace(refs, { trace, at, onError }, traceBox);
  useWaterCopy(refs, { highlight, water, waterLine, at, onError });
  useVisible(refs, onVisible);
  useRegion(refs, onRegion);
  useDrawn(refs, onDrawn);
  usePressFeature(refs, onPressFeature, onError, onPressObstacle, onPressNotFishing, onPressStart);
  useHoverCursor(refs, { feature: !!onPressFeature, obstacle: !!onPressObstacle,
                        notFishing: !!onPressNotFishing, mark: !!onPressMark }, onError);

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
    // (T48) the app's own bar over the map's top: the top-right controls start under it
    "--map-inset-top": `${insetTop}px`,
    // (T275) a block over the map's foot: the credits and the scale sit above it
    "--map-inset-bottom": `${insetBottom}px`,
  } as React.CSSProperties), [furniture, insetTop, insetBottom]);

  // the spots' layer is the map's sibling, over it in the same box (the host is the map's: MapLibre owns its children)
  return <>
    <div ref={refs.host} style={{ position: "absolute", inset: 0, ...vars, ...(style as object) }} />
    {spotLayer}
    {/* (T317) the boundary marks are map layers now; their buttons stay, for the keyboard and a screen reader only —
        out of sight and never moved with the map (a tap on the map is the layer's: `useMarks`) */}
    {marks?.length ? (
      <div data-marks-a11y="" style={SR_ONLY}>
        {marks.map((k, i) => (
          <button key={i} type="button" aria-label={k.a11y} data-mark={k.on ? "on" : ""}
                  onClick={() => onPressMark?.(i)}>{k.label ?? ""}</button>))}
      </div>) : null}
    {/* (T424) THE CREDITS: our own chip and card, the phone's component (Esri's "Powered by Esri" beside it while its
        imagery is drawn); the renderer's own attribution control is off */}
    {!bare && <MapCredits look={{ view, modes, groups, basemap }} chrome={furniture} insetBottom={insetBottom} onOpenLink={onOpenLink}
                         onLongPress={LAB_ON ? onOpenLab : undefined} />}
  </>;
}

/** Out of sight, still read and focused (the usual visually-hidden recipe). */
const SR_ONLY = { position: "absolute", width: 1, height: 1, padding: 0, margin: -1, overflow: "hidden",
                  clip: "rect(0 0 0 0)", whiteSpace: "nowrap", border: 0 } as const;
