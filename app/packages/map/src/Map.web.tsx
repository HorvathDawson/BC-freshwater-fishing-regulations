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
 * (`markers`), the highlight (`useHighlight`), the spots (`useSpots`), the colouring channels (`useChannels`), the fit
 * (`useFit`), the gauges and the route overlay (`useOverlays`), the water copy (`useWaterCopy`) and the visible set
 * (`useVisible`). They are CALLED IN THIS ORDER ON PURPOSE: React runs effects in the order they are declared, and the
 * map has to exist (`useMapInstance`) before anything that writes to it.
 *
 * Renders a plain <div>, which is correct even inside a react-native-web tree: maplibre
 * needs a real DOM node to attach a canvas to, and on this platform RNW is producing divs
 * anyway. The native renderer is a different file for exactly this reason.
 */
import { useMemo } from "react";
import "maplibre-gl/dist/maplibre-gl.css";
import "./controls.css";
import { mapChrome } from "./chrome";
import type { MapMark, MapProps } from "./map-props";
import { MARK_LABEL_FONT } from "./marks";
import { useMapInstance } from "./web/useMapInstance";
import { useLabelsInBC, useOutsideMask } from "./web/outsideMask";
import { useLook, usePushedData } from "./web/useLook";
import { useLongPress, useMapPoint, useMoved, usePressFeature } from "./web/useInput";
import { useMarker, useMarks, usePins } from "./web/markers";
import { useHighlight } from "./web/useHighlight";
import { useSpots } from "./web/useSpots";
import { useChannels } from "./web/useChannels";
import { useFit } from "./web/useFit";
import { useGauges, useTrace } from "./web/useOverlays";
import { useWaterCopy } from "./web/useWaterCopy";
import { useRegion, useVisible } from "./web/useVisible";
import { useWebMapRefs } from "./web/refs";

export function Map({ at, theme, view, modes, groups, hide, show, initial, data, onPressFeature, onPressObstacle, onPressTidal,
                     chrome,
                      onError, onMoved, onMapPoint, onLongPress, highlight, water, waterLine, selection = "outline", marker,
                      style,
                      gauges, onVisible, pins, bare, fit, basemap, trace, onImageryError,
                      zoomControls, status, standing, lakes, salmon, method, runs, marks, onPressMark, spots, insetTop = 0, onRegion }: MapProps) {
  const refs = useWebMapRefs(onError);
  // Confines the BASEMAP's labels to British Columbia — see `useOutsideMask`. Absent on the
  // first frame and present a moment later, which is why it is applied by an effect after the map is made
  // rather than only in the constructor.
  const outsideData = useOutsideMask(at.outside);
  /** MapLibre's own controls and markers, in this theme's chrome unless told otherwise. */
  const furniture = useMemo(() => chrome ?? mapChrome(theme), [chrome, theme]);

  useMapInstance(refs, { at, theme, modes, gauges, initial, bare, zoomControls, onError, onImageryError });
  useLabelsInBC(refs.map, outsideData);
  useLook(refs, { view, theme, modes, groups, hide, show, basemap });
  usePushedData(refs, data);
  useMoved(refs, onMoved);
  useLongPress(refs, onLongPress, at);
  useMapPoint(refs, onMapPoint);
  useMarker(refs, marker, furniture);
  usePins(refs, pins, furniture);
  const lit = useHighlight(refs, { highlight, water, selection });
  useMarks(refs, marks, onPressMark, furniture, markElement);
  const spotLayer = useSpots(refs, spots, at);
  useChannels(refs, lit, { at, theme, data, status, standing, lakes, salmon, method, runs });
  const traceBox = useFit(refs, fit, highlight);
  useGauges(refs, gauges);
  useTrace(refs, { trace, at, onError }, traceBox);
  useWaterCopy(refs, { highlight, water, waterLine, at, onError });
  useVisible(refs, onVisible);
  useRegion(refs, onRegion);
  usePressFeature(refs, onPressFeature, onError, onPressObstacle, onPressTidal);

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
  } as React.CSSProperties), [furniture, insetTop]);

  // the spots' layer is the map's sibling, over it in the same box (the host is the map's: MapLibre owns its children)
  return <>
    <div ref={refs.host} style={{ position: "absolute", inset: 0, ...vars, ...(style as object) }} />
    {spotLayer}
  </>;
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
      // (T151) normal weight: a mark's words name a place, they are not an emphasis competing with the water's colours
      + `font:${MARK_LABEL_FONT.web};white-space:normal;pointer-events:none`;
    b.appendChild(t);
  }
  b.addEventListener("click", (e) => { e.stopPropagation(); press(); });
  return root;
}
