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
