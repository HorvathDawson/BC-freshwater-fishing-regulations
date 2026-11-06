/**
 * (T424) ONE LIST OF CREDITS, ONE COMPONENT: this file is the only place the map's credits are written, as DATA
 * (`CreditLine`: a label and parts, a part being plain text or a link). `creditLines(look)` returns the lines for what the
 * look draws, in one order; `MapCredits.tsx` draws them in our own card on the web AND the phone (neither renderer's own
 * attribution control is on any more), so the wording, order and look cannot differ between them. The HTML the style's
 * sources carry (`BASEMAP_CREDIT`, `ATLAS_CREDIT`, each `MapCredit.html`) is derived from the same parts.
 *
 * (T235, owner 2026-10-28: "credits must live in the proper location, like attribution on the map") THE MAP'S OWN
 * CREDITS FOR WHAT IT DRAWS, in the renderer's attribution control (the ⓘ on both maps) — until T424 moved them to ours.
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
import { IMAGERY_ATTRIBUTION } from "@app/core";
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
/** A credit's part: plain text, or text linked to its licence or publisher. */
export type CreditPart = string | { readonly text: string; readonly href: string };
/** One credit as it is shown: what it covers (`label`, e.g. "Basemap") and who is credited, in what words. */
export interface CreditLine { readonly id: string; readonly label: string; readonly parts: readonly CreditPart[] }

const esc = (t: string) => t.replace(/&/g, "&amp;").replace(/</g, "&lt;");
const a = (href: string, text: string) => `<a href="${href}" target="_blank" rel="noopener">${text}</a>`;
/** The parts as HTML (the style's source `attribution` strings, which no renderer control draws any more). */
export const partsHtml = (parts: readonly CreditPart[]) =>
  parts.map((p) => (typeof p === "string" ? esc(p) : a(p.href, esc(p.text)))).join("");
/** The parts as plain text (what a reader sees; tests and a screen reader). */
export const partsText = (parts: readonly CreditPart[]) => parts.map((p) => (typeof p === "string" ? p : p.text)).join("");
/** A line as one string: "Label: credit". */
export const lineText = (l: CreditLine) => `${l.label}: ${partsText(l.parts)}`;

/** ODbL asks for exactly this wording, linked, visible where the data is used. */
export const OSM_PART: CreditPart = { text: "© OpenStreetMap contributors", href: LICENCE_URLS.odbl };

/** The basemap's credit (while the drawn ground is): ODbL asks for "© OpenStreetMap contributors", linked. */
export const BASEMAP_LINE: CreditLine = { id: "basemap", label: "Basemap", parts: [OSM_PART, " · © Protomaps"] };
export const BASEMAP_CREDIT = partsHtml(BASEMAP_LINE.parts);

/**
 * The atlas's credit (always on): the Province's Freshwater Atlas (OGL-BC) and synopsis, and — since T222/T224 — names
 * and side channels taken from OpenStreetMap, which ODbL says must be credited wherever they are shown, satellite or not
 * — and (owner 2026-10-28 audit) the always-drawn no-access land and reserve outlines, five lake outlines and the town
 * search, which are OpenStreetMap too.
 */
export const ATLAS_LINE: CreditLine = { id: "atlas", label: "Streams, lakes and rules", parts: [
  { text: "Province of British Columbia", href: LICENCE_URLS.oglBc }, " (OGL-BC) · Freshwater Atlas & Freshwater Fishing " +
  "Synopsis · some names, waters, places and access land ", OSM_PART] };
export const ATLAS_CREDIT = partsHtml(ATLAS_LINE.parts);

/** The gauge dots' source (`gaugeDots.ts`): always on every view. */
export const GAUGES_LINE: CreditLine = { id: "gauges", label: "Gauges", parts: ["Environment and Climate Change Canada"] };
/** Esri's required wording while its imagery is the ground: "Powered by Esri" and the service's copyright text. */
export const IMAGERY_LINE: CreditLine = { id: "imagery", label: "Imagery", parts: [IMAGERY_ATTRIBUTION] };

export interface MapCredit extends CreditLine {
  /** the credit, as a source would carry it (HTML: a link where the licence asks for one) */
  readonly html: string;
  /** does this look draw the data? */
  readonly drawn: (look: Look) => boolean;
}

/** Any layer of the look in this colour mode. */
const inMode = (look: Look, mode: string) => Object.values(look.modes ?? {}).includes(mode);

const credit = (id: string, label: string, parts: readonly CreditPart[], drawn: (l: Look) => boolean): MapCredit =>
  ({ id, label, parts, drawn, html: `${esc(label)}: ${partsHtml(parts)}` });

export const MAP_CREDITS_DRAWN: readonly MapCredit[] = [
  credit("salmon", "Salmon", [{ text: "Fisheries and Oceans Canada", href: LICENCE_URLS.dfoSalmon }, " (not an official version)"],
         (l) => inMode(l, "salmon")),
  credit("runs", "Run timing", [{ text: "Pacific Salmon Foundation", href: LICENCE_URLS.psf }, " (with permission) · Fisheries and Oceans Canada (",
         { text: "OGL-Canada", href: LICENCE_URLS.oglCanada }, ")"], (l) => inMode(l, "runs")),
  credit("stocking", "Stocking", ["Province of British Columbia (", { text: "OGL-BC", href: LICENCE_URLS.oglBc }, ")"],
         (l) => inMode(l, "stocked") || l.view === "stocked"),
  credit("obstacles", "Fish obstacles", ["Province of British Columbia — FISS, WRIS, PSCIS (", { text: "OGL-BC", href: LICENCE_URLS.oglBc }, ")"],
         (l) => l.groups?.obstacles === true),
];

/**
 * THE CREDITS FOR WHAT THE LOOK DRAWS, in one order: the ground (the drawn basemap, or Esri's imagery), the Province's
 * atlas, the gauges, then each mode or overlay that is on. Only what is on screen: a source the look hides is not named.
 * (Bathymetry has no credit here: no bathymetry is drawn on the map.)
 */
export function creditLines(look: Pick<Look, "view" | "modes" | "groups" | "basemap">): CreditLine[] {
  const l = { theme: "", ...look } as Look;
  return [look.basemap === "satellite" ? IMAGERY_LINE : BASEMAP_LINE, ATLAS_LINE, GAUGES_LINE,
          ...MAP_CREDITS_DRAWN.filter((c) => c.drawn(l))];
}

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
