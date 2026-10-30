/**
 * ONE LABEL STYLE FOR EVERY NAME ON THE MAP (owner 2026-10-22: "make names and labels and icons universally well
 * labelled and styled", in regulations, conditions, plain and satellite, light and dark).
 *
 * THREE INKS, ONE HALO. Water names wear `color.label.water`; every other name a reader steers by — towns, islands, and
 * our own parks, reserves, closures and Indigenous land — wears `color.label.place`; the quiet names (roads, addresses,
 * neighbourhoods, points of interest) wear `color.label.minor`; region and unit numbers the boundary's own grey. Every one
 * is cut out of `color.outside`, the map's paper. No name wears a status colour: a closure's name in the closure's
 * crimson read as one more closed thing, and the basemap's station blue sat a step from `status.base` — the status
 * colours mean exactly one thing each, and a label is never that thing (tools/label-colours.test.ts).
 *
 * OVER SATELLITE IMAGERY THE INKS TURN LIGHT. Aerial photography is a dark, busy ground in both themes; dark words on
 * paper halos sat on it as pale stickers. Every name — ours and the basemap's — takes the imagery tokens there (light
 * text, near-black halo; water names keep a blue cast), and returns to its theme's inks when the drawn map comes back.
 * Switched in the same pass as the ground (`setBasemap`), so a name is never painted for the other ground.
 *
 * THE BASEMAP'S LABELS ARE PAINTED HERE TOO. Protomaps' own text colours are its own palette (a park green, a station
 * blue, a pale grey region name); left as they were, half the names on a screen came from a second label style.
 */
import { MAP_STYLE, STYLE_META, type Tokens } from "./style";
import { BC_OUTLINE } from "./bc-outline.generated";
import { toExpression } from "./legacy-filter";

export type Ground = "map" | "satellite";

/** The ink a basemap label takes, by layer id. Absent: not a name we paint (a road shield's text sits on its icon). */
const BASEMAP_INK: Readonly<Record<string, "water" | "place" | "minor">> = {
  water_label_ocean: "water",
  earth_label_islands: "place",
  places_locality: "place",
  places_country: "place",
  places_region: "minor",
  places_subplace: "minor",
  roads_labels_major: "minor",
  roads_labels_minor: "minor",
  address_label: "minor",
  pois: "minor",
};

/** The basemap's symbol layers this file paints — every Protomaps name layer we keep, except the shields. */
export const BASEMAP_LABELS: readonly string[] = Object.keys(BASEMAP_INK);

/** The halo every name wears, in px — one width, so no name is louder for its halo. Admin numbers are wider (bigger type). */
export const HALO_PX = 1.5;
const HALO_BLUR = 0.5;

const INK_TOKEN = { water: "color.label.water", place: "color.label.place", minor: "color.label.minor" } as const;

/** A basemap name's paint on `ground`. */
export function basemapLabelPaint(layerId: string, tokens: Tokens, ground: Ground): Record<string, unknown> {
  const ink = BASEMAP_INK[layerId];
  if (!ink) return {};
  if (ground === "satellite")
    return { "text-color": tokens[ink === "water" ? "color.ui.imagery.label-water" : "color.ui.imagery.label"],
             "text-halo-color": tokens["color.ui.imagery.halo"], "text-halo-width": HALO_PX, "text-halo-blur": HALO_BLUR };
  return { "text-color": tokens[INK_TOKEN[ink]], "text-halo-color": tokens["color.outside"],
           "text-halo-width": HALO_PX, "text-halo-blur": HALO_BLUR };
}

/**
 * (T299) PLACE NAMES BY IMPORTANCE (owner 2026-10-30: "it puts a ton of names on the screen but should show most
 * important first and hide a lot until zoomed in a lot more").
 *
 * Protomaps' `places_locality` draws every settlement point its tile carries, and at z7 a tile carries a hundred
 * hamlets and OSM `place=locality` names ("Suicide Hill", "The Missing Link") beside Vancouver. So each kind of place
 * waits for its own zoom — a city always, a town from z7, a village from z9, a hamlet from z10, everything else
 * (localities, farms, isolated dwellings) from z12 — and among what is shown the bigger kind, then the bigger
 * population, is placed first (`symbol-sort-key`, lowest first).
 */
const PLACE_TIER = ["match", ["get", "kind_detail"], "city", 0, "town", 1, "village", 2, "hamlet", 3, 4];
/** The zoom each tier is first named at, by tier (city, town, village, hamlet, other). */
export const PLACE_TIER_ZOOM = [0, 7, 9, 10, 12] as const;

export function placesByImportance<L extends { id: string; filter?: unknown; layout?: Record<string, unknown> }>(l: L): L {
  if (l.id !== "places_locality") return l;
  const [, town, village, hamlet, other] = PLACE_TIER_ZOOM;
  return {
    ...l,
    filter: ["all", ["==", ["get", "kind"], "locality"],
             ["<=", PLACE_TIER, ["step", ["zoom"], 0, town, 1, village, 2, hamlet, 3, other, 4]]],
    layout: { ...l.layout,
              "symbol-sort-key": ["-", ["*", PLACE_TIER, 100], ["coalesce", ["get", "population_rank"], 0]] },
  };
}

/**
 * (T299) THE BASEMAP'S NAMES, ONLY INSIDE BRITISH COLUMBIA — in the style, on every platform, from the first frame.
 *
 * The basemap ship stage drops them (cut 7), but a phone draws whatever basemap it holds, and one from before that cut
 * named Seattle (owner, app 0.3.0). The web confined them with the fetched mask (`web/outsideMask.ts`); the phone had
 * nothing. So every basemap symbol layer also asks `within` BC's outline (`bc-outline.generated.ts`, from the same
 * boundary, pushed out 500 m so a border town stays). The layer's own filter is converted first (`toExpression`):
 * Protomaps writes legacy filters, and `["all", <expression>, <legacy>]` parses as legacy.
 */
export function inBC<L extends { type?: string; source?: string; filter?: unknown }>(l: L): L {
  if (l.type !== "symbol" || l.source !== "basemap") return l;
  const within = ["within", BC_OUTLINE as unknown];
  return { ...l, filter: l.filter ? ["all", within, toExpression(l.filter)] : within };
}

/** (T299) The names that outrank the basemap's (placed before a town's): a river's and a lake's. `runtime-style.ts`. */
export const WATER_NAMES: readonly string[] = ["stream__label", "lake__label"];

/** Our own name layers (the generated `__label` companions) — every symbol layer with a label halo that is not an arrow. */
export const OWN_LABELS: readonly string[] = MAP_STYLE.layers.map((l) => l.id)
  .filter((id) => id.endsWith("__label") && (STYLE_META.labelHalos ?? {})[id] !== undefined);

/** Is this one of ours that names WATER (so it keeps a blue cast over imagery)? */
const namesWater = (id: string) => {
  const t = STYLE_META.colorModes[id]?.plain;
  return t?.scale === "static" && t.color.token === "color.label.water";
};

/**
 * Our name's paint over imagery. On the drawn map a name's paint is its colour mode's (`paintFor`), which the adapter
 * puts back itself; this is only the imagery override.
 */
export function ownLabelOnImagery(layerId: string, tokens: Tokens): Record<string, unknown> {
  return { "text-color": tokens[namesWater(layerId) ? "color.ui.imagery.label-water" : "color.ui.imagery.label"],
           "text-halo-color": tokens["color.ui.imagery.halo"] };
}
