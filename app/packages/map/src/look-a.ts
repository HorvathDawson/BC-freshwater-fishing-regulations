/**
 * (T1208, T1209; owner 2026-11-26: "build A") THE ZOOMED-OUT LOOK A, the parts the generated style cannot say: the soft band
 * inside each region's edge, "Region n" once per region, the towns to find your way by, and the basemap's quieter ground and
 * one water blue. Prototyped and pictured in T1200 (the dev-only `?t1200=` lab, never shipped); built here from tokens and
 * two small data files.
 *
 * EVERY LAYER IS ONE BOTH ENGINES DRAW (T1154): a GeoJSON source with its data inline, `line-offset`, `concat`,
 * `text-variable-anchor` + `text-radial-offset`, `symbol-sort-key`, a zoom `interpolate` / `step` at the top of a property
 * and `zoom` in a filter (read at whole zooms by both). No MapLibre-only expression.
 */
import REGION_LABELS from "./region-labels.json" with { type: "json" };
import TOWNS from "./towns.json" with { type: "json" };
import type { Tokens } from "./style";

/** The band just inside each region's edge (`line-offset`: a polygon ring's right side is its inside). */
export const REGION_BAND = "region-band";
/** "Region n", once per region, from its fixed point. */
export const REGION_NAMES = "region-names";
export const REGION_LABEL_SOURCE = "region-labels";
/** The towns: a dot and a name each, one source. */
export const TOWN_SOURCE = "towns";
export const TOWN_DOTS = "town-dots";
export const TOWN_NAMES = "town-names";

/** The zoom each tier is first named at: tier 1 the whole province, tier 2 a region, tier 3 a lake district. */
export const TIER_ZOOM: Readonly<Record<1 | 2 | 3, number>> = { 1: 4, 2: 6, 3: 8 };
/** From this zoom the basemap's own places name the towns (`labels.ts` tiers); ours stop. */
export const TOWNS_UNTIL = 10;
/** The region names stop where the units' numbers start (as the tiles' region label did). */
export const REGION_NAMES_UNTIL = 7;

export interface Town { name: string; tier: 1 | 2 | 3; lon: number; lat: number; why: string; region: string; position: string;
  /** the basemap's own string for this place where it is not the official name (de-duplication only) */
  basemap?: string }
export const TOWN_LIST = TOWNS.towns as readonly Town[];
export const REGION_POINTS = REGION_LABELS.regions as readonly { region_id: string; lon: number; lat: number }[];

const tk = (t: Tokens, k: string) => {
  const v = t[k];
  if (v === undefined) throw new Error(`look-a: the theme has no ${k}`);
  return v;
};

/** The sources: the region label points and the towns, inline (small, fixed: no request). */
/** (T1208, T1209) the region label points and the towns as GeoJSON: the style's sources, and what the phone's label hold
 *  mounts by the same ids (native `View.tsx`), one object each so both hand the map the same data */
export const REGION_LABEL_DATA = { type: "FeatureCollection", features: REGION_POINTS.map((r) => (
  { type: "Feature", properties: { region_id: r.region_id }, geometry: { type: "Point", coordinates: [r.lon, r.lat] } })) } as const;
export const TOWN_DATA = { type: "FeatureCollection", features: TOWN_LIST.map((t, i) => (
  { type: "Feature", properties: { name: t.name, tier: t.tier, order: t.tier * 1000 + i },
    geometry: { type: "Point", coordinates: [t.lon, t.lat] } })) } as const;

export function lookASources(): Record<string, Record<string, unknown>> {
  return {
    [REGION_LABEL_SOURCE]: { type: "geojson", data: REGION_LABEL_DATA },
    [TOWN_SOURCE]: { type: "geojson", data: TOWN_DATA },
  };
}

/** The soft band inside each region's edge: under the dashed edge, a tint not a line, to z10. */
export function regionBandLayer(t: Tokens) {
  const w = Number(tk(t, "width.region.band"));
  return {
    id: REGION_BAND, type: "line" as const, source: "atlas", "source-layer": "region", maxzoom: 10,
    layout: { "line-join": "round" as const },
    paint: { "line-color": tk(t, "color.region.line") as string, "line-opacity": Number(tk(t, "opacity.region.band")),
             "line-width": ["interpolate", ["linear"], ["zoom"], 4, w / 2, 8, w],
             "line-offset": ["interpolate", ["linear"], ["zoom"], 4, w / 4, 8, w / 2] },
  };
}

/** "Region n" once per region (its point is inside its biggest piece, never on a tile edge), z4 to z7. */
export function regionNamesLayer(t: Tokens) {
  return {
    id: REGION_NAMES, type: "symbol" as const, source: REGION_LABEL_SOURCE, minzoom: 4, maxzoom: REGION_NAMES_UNTIL,
    layout: { "text-field": ["concat", "Region ", ["get", "region_id"]], "text-font": ["Noto Sans Medium"],
              "text-size": ["interpolate", ["linear"], ["zoom"], 4, 10.5, 7, 12.5], "text-letter-spacing": 0.12,
              // centred on its point where there is room, else a line above or below it: a town keeps its name
              "text-variable-anchor": ["center", "top", "bottom"], "text-radial-offset": 0.9,
              "text-padding": 3, "text-allow-overlap": false },
    paint: { "text-color": tk(t, "color.label.region") as string, "text-halo-color": tk(t, "color.outside") as string,
             "text-halo-width": 2, "text-halo-blur": 0.5 },
  };
}

/** A town is drawn from its tier's zoom (`TIER_ZOOM`) to `TOWNS_UNTIL`. */
export const TIER_SHOWN = ["any",
  ["==", ["get", "tier"], 1],
  ["all", ["==", ["get", "tier"], 2], [">=", ["zoom"], TIER_ZOOM[2]]],
  ["all", ["==", ["get", "tier"], 3], [">=", ["zoom"], TIER_ZOOM[3]]]];

/** The towns' dots and names, in the place names' inks (`labels.ts`); a lake's name is placed before a town's. */
/** (review of T1208) the towns' dots, UNDER the water's lines (drawn just after the region edge): Hope's dot sat on the
 *  Fraser, a mark over the answer; the rule line wins where they meet, as a boundary's does (T83). */
export function townDotsLayer(t: Tokens) {
  return { id: TOWN_DOTS, type: "circle" as const, source: TOWN_SOURCE, maxzoom: TOWNS_UNTIL, filter: TIER_SHOWN,
           paint: { "circle-radius": 2.6, "circle-color": tk(t, "color.label.place") as string,
                    "circle-stroke-color": tk(t, "color.outside") as string, "circle-stroke-width": 1 } };
}

/** the towns' names, with the labels (over the basemap's names, under the water's) */
export function townLayers(t: Tokens) {
  return [
    { id: TOWN_NAMES, type: "symbol" as const, source: TOWN_SOURCE, maxzoom: TOWNS_UNTIL, filter: TIER_SHOWN,
      layout: { "text-field": ["get", "name"], "text-font": ["Noto Sans Medium"],
                "text-size": ["interpolate", ["linear"], ["zoom"], 4, ["case", ["==", ["get", "tier"], 1], 11, 10.5],
                              9, ["case", ["==", ["get", "tier"], 3], 11.5, 12.5]],
                "text-variable-anchor": ["left", "right", "top", "bottom"], "text-radial-offset": 0.5, "text-justify": "auto" as const,
                "symbol-sort-key": ["get", "order"], "text-padding": 3 },
      paint: { "text-color": tk(t, "color.label.place") as string, "text-halo-color": tk(t, "color.outside") as string,
               "text-halo-width": 1.5, "text-halo-blur": 0.5 } },
  ];
}

/** The basemap's own place names leave out ours (no name twice) and wait for z10 for the rest of our towns' tiers. */
export const OUR_TOWN_NAMES = ["literal", TOWN_LIST.flatMap((t) => (t.basemap ? [t.name, t.basemap] : [t.name]))];

/**
 * THE BASEMAP'S GROUND AND WATER (Protomaps' `earth`, `water`, its own river lines, land cover and woods): the light theme's
 * softer ground and one water blue, from tokens; the dark theme's tokens are its flavour's own, so it is unchanged.
 */
export function quieterGround<L extends { id: string; type?: string; paint?: Record<string, unknown> }>(l: L, t: Tokens): L {
  const cover = Number(tk(t, "opacity.ground.cover"));
  const scaled = (e: unknown) => (cover === 1 ? e : Array.isArray(e) && e[0] === "interpolate"
    ? [...e.slice(0, 3), ...e.slice(3).map((v, i) => (i % 2 === 1 ? ["*", v, cover] : v))] : ["*", e ?? 1, cover]);
  if (l.id === "earth") return { ...l, paint: { ...l.paint, "fill-color": tk(t, "color.ui.basemap.ground") } };
  if (l.id === "water") return { ...l, paint: { ...l.paint, "fill-color": tk(t, "color.ui.basemap.water") } };
  if (l.id === "water_stream" || l.id === "water_river") return { ...l, paint: { ...l.paint, "line-color": tk(t, "color.ui.basemap.water") } };
  if (l.id === "landcover" || l.id === "landuse_park")
    return { ...l, paint: { ...l.paint, "fill-opacity": scaled(l.paint?.["fill-opacity"]) } };
  return l;
}
