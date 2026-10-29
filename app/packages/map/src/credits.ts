/**
 * (T235, owner 2026-10-28: "credits must live in the proper location, like attribution on the map") THE MAP'S OWN
 * CREDITS FOR WHAT IT DRAWS, in the renderer's attribution control (the ⓘ on both maps).
 *
 * The always-drawn sources carry their credit on the source itself (`runtime-style.ts`: the basemap — OpenStreetMap
 * and Protomaps; the atlas — the Province's Freshwater Atlas and synopsis, and the OpenStreetMap names and streams it
 * now holds; the gauges — ECCC; the imagery — Esri, from `layers.source.json`). What a MODE or an OVERLAY adds draws on
 * the atlas, so it has no source of its own to carry a credit: each gets one here — an empty GeoJSON source holding
 * only the credit, and one layer that draws nothing, visible exactly while the look draws that data (`creditDrawn`).
 *
 *   web     MapLibre's control lists the credit of every source a VISIBLE layer reads (`tileManager.used`), and re-reads
 *           it when a layer's visibility changes — so the credit comes and goes with the data, by the renderer's own rule.
 *   phone   MapLibre Native lists every source's credit in the style, used or not: `nativeStyle` drops the credit of
 *           a source whose layer the look hid (as it does Esri's while the drawn map is chosen).
 *
 * One sequence for both maps: `applyLook` sets these layers' visibility with everything else.
 */
import type { Look } from "./look";

/** The licences, linked once. */
export const LICENCE_URLS = {
  oglBc: "https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc",
  oglCanada: "https://open.canada.ca/en/open-government-licence-canada",
  odbl: "https://www.openstreetmap.org/copyright",
  bcCopyright: "https://www2.gov.bc.ca/gov/content/home/copyright",
  dfoSalmon: "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/index-eng.html",
  psf: "https://www.salmonexplorer.ca",
} as const;
const a = (href: string, text: string) => `<a href="${href}" target="_blank" rel="noopener">${text}</a>`;

/** The basemap's credit (always on while the drawn ground is): ODbL asks for "© OpenStreetMap contributors", linked. */
export const BASEMAP_CREDIT = `${a(LICENCE_URLS.odbl, "© OpenStreetMap contributors")} · © Protomaps`;

/**
 * The atlas's credit (always on): the Province's Freshwater Atlas (OGL-BC) and synopsis, and — since T222/T224 — names
 * and side channels taken from OpenStreetMap, which ODbL says must be credited wherever they are shown, satellite or not
 * — and (owner 2026-10-28 audit) the always-drawn no-access land and reserve outlines, five lake outlines and the town
 * search, which are OpenStreetMap too.
 */
export const ATLAS_CREDIT =
  `${a(LICENCE_URLS.oglBc, "Province of British Columbia")} (OGL-BC) · Freshwater Atlas &amp; Freshwater Fishing ` +
  `Synopsis · some names, waters, places and access land ${a(LICENCE_URLS.odbl, "© OpenStreetMap contributors")}`;

export interface MapCredit {
  readonly id: string;
  /** the credit, as the control shows it (HTML: a link where the licence asks for one) */
  readonly html: string;
  /** does this look draw the data? */
  readonly drawn: (look: Look) => boolean;
}

/** Any layer of the look in this colour mode. */
const inMode = (look: Look, mode: string) => Object.values(look.modes ?? {}).includes(mode);

export const MAP_CREDITS_DRAWN: readonly MapCredit[] = [
  { id: "salmon", drawn: (l) => inMode(l, "salmon"),
    html: `Salmon: ${a(LICENCE_URLS.dfoSalmon, "Fisheries and Oceans Canada")} (not an official version)` },
  { id: "runs", drawn: (l) => inMode(l, "runs"),
    html: `Run timing: ${a(LICENCE_URLS.psf, "Pacific Salmon Foundation")} (with permission) · Fisheries and Oceans ` +
      `Canada (${a(LICENCE_URLS.oglCanada, "OGL-Canada")})` },
  { id: "stocking", drawn: (l) => inMode(l, "stocked") || l.view === "stocked",
    html: `Stocking: Province of British Columbia (${a(LICENCE_URLS.oglBc, "OGL-BC")})` },
  { id: "obstacles", drawn: (l) => l.groups?.obstacles === true,
    html: `Fish obstacles: Province of British Columbia — FISS, WRIS, PSCIS (${a(LICENCE_URLS.oglBc, "OGL-BC")})` },
];

export const CREDIT_PREFIX = "credit-";
export const creditId = (c: Pick<MapCredit, "id">) => `${CREDIT_PREFIX}${c.id}`;

/** Is this credit's data in the look? (the layer's visibility, on both maps) */
export const creditDrawn = (c: MapCredit, look: Look): boolean => c.drawn(look);

/** The credits' sources: empty, holding only the credit. */
export function creditSources(): Record<string, { type: "geojson"; data: unknown; attribution: string }> {
  return Object.fromEntries(MAP_CREDITS_DRAWN.map((c) => [creditId(c), {
    type: "geojson" as const, data: { type: "FeatureCollection", features: [] }, attribution: c.html,
  }]));
}

/** One layer per credit that draws nothing; hidden until the look draws its data. */
export function creditLayers() {
  return MAP_CREDITS_DRAWN.map((c) => ({
    id: creditId(c), type: "circle" as const, source: creditId(c),
    layout: { visibility: "none" as const },
    paint: { "circle-radius": 0, "circle-opacity": 0 },
  }));
}
