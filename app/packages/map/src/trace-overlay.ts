/**
 * THE ROUTE OVERLAY — how the gauges reach the place being read, drawn over the map.
 *
 * Owner 2026-10-21, on the Conditions map:
 *
 *   · UPSTREAM AND DOWNSTREAM APART. A gauge downstream of you is drawn SOLID along the
 *     water from you to it; a gauge upstream DASHED, from it down to you. Both in the
 *     donor's own tone, so a route, its gauge's dot and its row in the panel are one colour.
 *   · YOU ARE HERE IS A DIFFERENT SHAPE. Gauges are round dots; the spot is a pin
 *     (`here-mark.ts`) in the accent, with its words under it.
 *   · EACH GAUGE SAYS WHAT IT CONTRIBUTES — its share of the estimate ("62%") in a label
 *     beside the dot. The words are handed in finished (from `donorShare`, the panel's own),
 *     never computed here. Labels take whichever side is free (`text-variable-anchor-offset`)
 *     and the heaviest gauge places first (`symbol-sort-key`), so a crowded valley moves a
 *     label rather than drawing two on top of each other — and when no side is free (a
 *     province-wide zoom) the label is still drawn, never dropped; the key repeats it.
 *   · IT DRAWS AT EVERY ZOOM, because its lines are copied out of the tiles once
 *     (`trace-geometry.ts`) instead of being lit on whatever the current zoom's tile holds.
 *
 * Runtime GeoJSON sources, like the gauges: the style declares them empty and the adapter's
 * `setTrace` fills them (both renderers, one contract). Colours come from the theme's tokens
 * or, for a donor's tone, from the feature (the app's palette, itself built from tokens).
 */
import { resolveTheme } from "./style";
import type { TraceCollection, TraceRoute } from "./trace-geometry";

export const TRACE_SOURCE = "trace";
export const TRACE_PINS_SOURCE = "trace-pins";
export const HERE_IMAGE = "trace-here";

/** A marked point on the route map. */
export interface TracePin {
  lat: number;
  lon: number;
  /** A gauge's dot, or the spot's pin. */
  kind: "here" | "gauge";
  /** A gauge's donor tone; the spot's is the accent and ignores this. */
  tone: string;
  /** Drawn beside it: a gauge's share ("62%"), the spot's words. Null draws no label. */
  label: string | null;
  /** 0 is the heaviest gauge: places its label first. */
  rank: number;
}

/** What a screen hands the map: the routes to copy out of the tiles, and the pins. */
export interface TraceProp {
  /** Changes when the routes do — the overlay is refetched on a new key and at no other time. */
  key: string;
  routes: readonly TraceRoute[];
  pins: readonly TracePin[];
}

export const EMPTY_TRACE: TraceCollection = { type: "FeatureCollection", features: [] };

export interface PinCollection {
  type: "FeatureCollection";
  features: { type: "Feature";
              properties: { kind: TracePin["kind"]; tone: string; rank: number; label?: string };
              geometry: { type: "Point"; coordinates: [number, number] } }[];
}

/** The pins as GeoJSON. A null label is LEFT OUT, so the label layers' `has` skips it. */
export function pinCollection(pins: readonly TracePin[]): PinCollection {
  return {
    type: "FeatureCollection",
    features: pins.map(({ lat, lon, label, kind, tone, rank }) => ({
      type: "Feature",
      properties: label === null ? { kind, tone, rank } : { kind, tone, rank, label },
      geometry: { type: "Point", coordinates: [lon, lat] },
    })),
  };
}

/**
 * The theme-dependent paint of every overlay layer — written into the style on the first
 * frame and re-applied on a theme change (the style is not rebuilt for one).
 */
export function tracePaint(theme: string): Record<string, Record<string, unknown>> {
  const t = resolveTheme(theme);
  const paper = t["color.outside"] as string;
  const ink = t["color.ui.ink"] as string;
  return {
    "trace-line-down": { "line-color": ["get", "tone"] },
    "trace-line-up": { "line-color": ["get", "tone"] },
    "trace-arrows": { "text-color": ["get", "tone"], "text-halo-color": paper },
    "trace-gauge-halo": { "circle-color": ink },
    "trace-gauge": { "circle-color": ["get", "tone"], "circle-stroke-color": paper },
    "trace-here-label": { "text-color": t["color.ui.accent"] as string, "text-halo-color": paper },
    "trace-label": { "text-color": ink },
    "trace-join": { "circle-color": paper, "circle-stroke-color": ["get", "tone"] },
    "trace-join-label": { "text-color": ink, "text-halo-color": paper },
  };
}

/** The overlay's layer ids, in draw order. */
export const TRACE_LAYER_IDS = [
  "trace-line-down", "trace-line-up", "trace-arrows", "trace-join", "trace-join-label", "trace-gauge-halo", "trace-gauge",
  "trace-label", "trace-here", "trace-here-label"] as const;

/**
 * The overlay's layers. Appended LAST to the style, so the route and its marks sit on top
 * of every river and every gauge dot — at a zoom where the Conditions view stands the
 * rivers down, the overlay is the only water drawn, which is the point.
 */
export function traceLayers(theme: string):
    { id: string; type: string; source: string; [k: string]: unknown }[] {
  const p = tracePaint(theme);
  const line = (kind: string) => ["==", ["get", "kind"], kind];
  return [
    /*
     * ARROWS ONLY (owner 2026-10-21: "the dotted line is bad with the arrows. arrows should be enough"). The route used to
     * be a paper casing + a tone line (solid down, dashed up) + a lake wash, drawn over rivers already coloured by their
     * standing — two colours on one reach. Now the river keeps its own colour and the route is its arrows, in the tone of
     * the gauge it leads to (up and down told apart by that tone).
     */
    /*
     * WHICH WAY THE WATER GOES — as on the Conditions map's `stream__arrows`: stream lines
     * run mouth to source (the tile contract's `flows`; the copy keeps vertex order), so a
     * line-placed ◀ points DOWNSTREAM; `keep-upright: false` stops MapLibre flipping it.
     *
     * ◀ IN REGULAR, NOT ▲ IN MEDIUM TURNED 180 (2026-10-21). Protomaps' Medium stack has no
     * geometric shapes, so these arrows were silently never drawn; a line-placed ▲ points
     * ACROSS the line, not along it; and a 180° `text-rotate` pivots the glyph off the line
     * (`tools/flow-arrows.test.ts`).
     */
    /*
     * THE PATH ITSELF (owner 2026-10-28, "no stream path is drawn"): a gauge downstream is a SOLID line in its tone,
     * one upstream a DASHED one, over the river's own colour; the one picked in the list is heavier. A donor on another
     * water has both — solid to the confluence, dashed up its own river — and the arrows still say which way water goes.
     */
    { id: "trace-line-down", type: "line", source: TRACE_SOURCE,
      filter: ["all", line("line"), ["==", ["get", "role"], "down"]],
      layout: { "line-cap": "round", "line-join": "round" },
      paint: { ...p["trace-line-down"], "line-opacity": 0.85,
               "line-width": ["case", ["get", "on"], 5, 3] } },
    { id: "trace-line-up", type: "line", source: TRACE_SOURCE,
      filter: ["all", line("line"), ["==", ["get", "role"], "up"]],
      layout: { "line-cap": "butt", "line-join": "round" },
      paint: { ...p["trace-line-up"], "line-opacity": 0.85, "line-dasharray": [2, 1.6],
               "line-width": ["case", ["get", "on"], 5, 3] } },
    { id: "trace-arrows", type: "symbol", source: TRACE_SOURCE,
      filter: line("line"),
      layout: { "symbol-placement": "line", "symbol-spacing": 56, "text-field": "◀",
                "text-font": ["Noto Sans Regular"], "text-size": ["interpolate", ["linear"], ["zoom"], 8, 12, 13, 15],
                "text-keep-upright": false, "text-allow-overlap": true,
                "text-ignore-placement": true, "text-rotation-alignment": "map" },
      paint: { ...p["trace-arrows"], "text-halo-width": 1.6 } },
    /*
     * (T252) THE CONFLUENCE, where a gauge's water and this one meet: a small ring in the route's tone, and its words
     * ("joins at the mouth of … · about N km along the river") — optional, so a crowded map drops them before a share.
     */
    { id: "trace-join", type: "circle", source: TRACE_SOURCE,
      filter: line("join"),
      paint: { ...p["trace-join"], "circle-radius": 4.5, "circle-stroke-width": 2.5 } },
    { id: "trace-join-label", type: "symbol", source: TRACE_SOURCE,
      filter: ["all", line("join"), ["has", "label"]],
      layout: { "text-field": ["get", "label"], "text-font": ["Noto Sans Medium"], "text-size": 11.5,
                "text-variable-anchor-offset": ["literal", ["left", [0.9, 0], "right", [-0.9, 0],
                                                            "top", [0, 0.8], "bottom", [0, -0.8]]],
                "text-max-width": 14, "text-optional": true },
      paint: { ...p["trace-join-label"], "text-halo-width": 1.6 } },
    /*
     * A FAINT INK RING under every dot. The paper ring alone vanishes on a dark map, and a
     * quiet gauge's tone is the palette's dark grey there — a dot drawn in the map's own
     * colours. The ink ring is the one edge that contrasts with the map in either theme.
     */
    { id: "trace-gauge-halo", type: "circle", source: TRACE_PINS_SOURCE,
      filter: ["==", ["get", "kind"], "gauge"],
      paint: { ...p["trace-gauge-halo"], "circle-radius": 10.5, "circle-opacity": 0.6 } },
    // EVERY GAUGE A ROUND DOT in its donor tone.
    { id: "trace-gauge", type: "circle", source: TRACE_PINS_SOURCE,
      filter: ["==", ["get", "kind"], "gauge"],
      paint: { ...p["trace-gauge"], "circle-radius": 7, "circle-stroke-width": 2.5 } },
    // ITS SHARE beside it, on whichever side is free; the heaviest places first.
    { id: "trace-label", type: "symbol", source: TRACE_PINS_SOURCE,
      filter: ["all", ["==", ["get", "kind"], "gauge"], ["has", "label"]],
      layout: {
        "text-field": ["get", "label"], "text-font": ["Noto Sans Medium"], "text-size": 12.5,
        "symbol-sort-key": ["get", "rank"],
        "text-variable-anchor-offset": ["literal", ["left", [1.1, 0], "right", [-1.1, 0],
                                                    "top", [0, 0.95], "bottom", [0, -0.95]]],
        "icon-image": "gauge-pill", "icon-text-fit": "both",
        "icon-text-fit-padding": [3, 7, 3, 7],
        /*
         * A SHARE IS NEVER DROPPED (review 2026-10-21). Overlap allowed + variable anchors:
         * MapLibre first tries every side WITHOUT overlap and takes the first free one; only
         * when none is free does it draw on the first side anyway. Refusing overlap instead
         * hid the share of a gauge at the spot (the pin took every side) and, zoomed out,
         * most of them. The pill is not tested in the free-side pass (its icon allows
         * overlap), so the text box is padded to the pill's size.
         */
        "text-allow-overlap": true, "icon-allow-overlap": true, "text-padding": 7,
        // It still takes its place, so later (lighter) shares look for a side clear of it.
        "text-ignore-placement": false,
      },
      paint: { ...p["trace-label"] } },
    // YOU ARE HERE: a pin standing on the place, its words beneath.
    { id: "trace-here", type: "symbol", source: TRACE_PINS_SOURCE,
      filter: ["==", ["get", "kind"], "here"],
      layout: { "icon-image": HERE_IMAGE, "icon-anchor": "bottom",
                "icon-allow-overlap": true,
                // It still TAKES its place: the gauges' labels move off the pin, not under it.
                "icon-ignore-placement": false } },
    { id: "trace-here-label", type: "symbol", source: TRACE_PINS_SOURCE,
      filter: ["all", ["==", ["get", "kind"], "here"], ["has", "label"]],
      layout: { "text-field": ["get", "label"], "text-font": ["Noto Sans Medium"],
                "text-size": 12, "text-anchor": "top", "text-offset": [0, 0.35],
                "text-optional": true },
      paint: { ...p["trace-here-label"], "text-halo-width": 1.6 } },
  ];
}

/** The overlay's sources, declared empty: the adapter fills them. */
export function traceSources(): Record<string, unknown> {
  return {
    [TRACE_SOURCE]: { type: "geojson", data: EMPTY_TRACE },
    [TRACE_PINS_SOURCE]: { type: "geojson", data: pinCollection([]) },
  };
}
