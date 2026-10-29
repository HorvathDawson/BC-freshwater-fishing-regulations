/**
 * (T235, owner 2026-10-28) SOURCES AND LICENCES: every dataset the app shows, who publishes it, the licence or
 * permission it is used under (linked), and the words that licence asks for — on the Sources and licences page (More,
 * and About's link). The map's own credit control names the same publishers (`@app/map` credits.ts); a sheet that shows
 * one source's data credits it at its foot (the salmon card, the Conditions fold, the obstacle sheet, run timing).
 *
 * Words only, no dates: each source's freshness is the More tab's (freshness.ts).
 */

/** The licences, named and linked once. */
export const LICENCES = {
  oglBc: { label: "Open Government Licence – British Columbia",
           url: "https://www2.gov.bc.ca/gov/content/data/open-data/open-government-licence-bc" },
  oglCanada: { label: "Open Government Licence – Canada", url: "https://open.canada.ca/en/open-government-licence-canada" },
  odbl: { label: "Open Database License (ODbL) 1.0", url: "https://www.openstreetmap.org/copyright" },
  bcCopyright: { label: "Province of British Columbia copyright terms",
                 url: "https://www2.gov.bc.ca/gov/content/home/copyright" },
  canadaTerms: { label: "Government of Canada terms and conditions (non-commercial reproduction)",
                 url: "https://www.dfo-mpo.gc.ca/terms-conditions-avis-eng.htm" },
  ccBy: { label: "Creative Commons Attribution 4.0 (CC BY 4.0)", url: "https://creativecommons.org/licenses/by/4.0/" },
  esri: { label: "Esri Master Agreement / World Imagery terms of use",
          url: "https://www.esri.com/en-us/legal/terms/full-master-agreement" },
  permission: { label: "Used with the publisher's written permission", url: null },
  isc: { label: "ISC licence", url: "https://lucide.dev/license" },
  municipal: { label: "Each city's open data licence", url: null },
} as const;
export type LicenceKey = keyof typeof LICENCES;

export interface LicenceRow {
  readonly k: string;
  /** the dataset, as the app names it */
  readonly name: string;
  /** who publishes it */
  readonly from: string;
  /** what the app shows from it */
  readonly shows: string;
  readonly licence: LicenceKey;
  /** the words the licence or the publisher asks for, verbatim; null: a credit by name is enough */
  readonly wording: string | null;
  /** the publisher's page for the dataset */
  readonly url: string | null;
}

const OGL_BC_WORDS = "Contains information licensed under the Open Government Licence – British Columbia.";
const OGL_CA_WORDS = "Contains information licensed under the Open Government Licence – Canada.";

/**
 * THE LIST, in the order the page reads: the regulations, the water and the map, the conditions, the fish, the rest.
 * (Run timing shows only while that mode is on; it is listed so a reader can find its terms.)
 */
export const LICENCE_ROWS: readonly LicenceRow[] = [
  { k: "synopsis", name: "Freshwater Fishing Regulations Synopsis", from: "Province of British Columbia",
    shows: "The regulations the app reads, and a copy of the printed book.", licence: "bcCopyright",
    wording: "© Province of British Columbia. This app is not the official version and is not endorsed by the Province.",
    url: "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations" },
  { k: "inseason", name: "In-season regulation changes", from: "Province of British Columbia",
    shows: "Banners on the waters they name, as the Province wrote them.", licence: "bcCopyright", wording: null,
    url: "https://www2.gov.bc.ca/gov/content/sports-culture/recreation/fishing-hunting/fishing/fishing-regulations#In-Season-changes" },
  { k: "dfo", name: "Freshwater salmon sport fishing regulations", from: "Fisheries and Oceans Canada (DFO)",
    shows: "Salmon limits and closures, the Salmon map colours.", licence: "canadaTerms",
    wording: "Reproduced from Fisheries and Oceans Canada. This is not an official version, and it was not made in "
      + "affiliation with, or with the endorsement of, the Government of Canada.",
    url: "https://www.pac.dfo-mpo.gc.ca/fm-gp/rec/fresh-douce/index-eng.html" },
  { k: "fwa", name: "Freshwater Atlas", from: "Province of British Columbia",
    shows: "Every stream, lake and wetland on the map, and their names.", licence: "oglBc", wording: OGL_BC_WORDS,
    url: "https://www2.gov.bc.ca/gov/content/data/geographic-data-services/topographic-data/freshwater" },
  { k: "gnis", name: "BC Geographical Names", from: "Province of British Columbia",
    shows: "Official water names.", licence: "oglBc", wording: OGL_BC_WORDS,
    url: "https://apps.gov.bc.ca/pub/bcgnws/" },
  { k: "osm", name: "OpenStreetMap", from: "© OpenStreetMap contributors",
    shows: "The basemap (roads, towns, land), some local water names, and two side channels the Atlas does not have "
      + "(Northrop Channel and Creek).", licence: "odbl", wording: "© OpenStreetMap contributors",
    url: "https://www.openstreetmap.org/copyright" },
  { k: "protomaps", name: "Protomaps basemap", from: "© Protomaps", shows: "The basemap's tiles and style.",
    licence: "odbl", wording: "© Protomaps", url: "https://protomaps.com" },
  { k: "municipal", name: "City stream layers", from: "City of Abbotsford, City of Burnaby, City of Port Moody, "
      + "District of Squamish", shows: "Streams the Atlas does not have, and their local names.",
    licence: "municipal", wording: null, url: null },
  { k: "access", name: "Parks, ecological reserves and land status", from: "Province of British Columbia",
    shows: "Closed parks, reserves and no-access land on the map.", licence: "oglBc", wording: OGL_BC_WORDS,
    url: "https://catalogue.data.gov.bc.ca" },
  { k: "obstacles", name: "Fish obstacles: FISS, WRIS dams, PSCIS crossings", from: "Province of British Columbia",
    shows: "Falls, dams and culverts (the Fish obstacles layer).", licence: "oglBc", wording: OGL_BC_WORDS,
    url: "https://catalogue.data.gov.bc.ca" },
  { k: "stocking", name: "Fish stocking and lake surveys", from: "Province of British Columbia (Fisheries Inventory)",
    shows: "Stocked lakes and lake depth.", licence: "oglBc", wording: OGL_BC_WORDS,
    url: "https://www2.gov.bc.ca/gov/content/environment/plants-animals-ecosystems/fish/fish-and-habitat-data-and-information" },
  { k: "hydat", name: "Hydrometric data (HYDAT and real-time)", from: "Environment and Climate Change Canada",
    shows: "Gauge readings, flow history and the Conditions map.", licence: "oglCanada", wording: OGL_CA_WORDS,
    url: "https://wateroffice.ec.gc.ca" },
  { k: "forecast", name: "River forecasts (CLEVER, COFFEE, ELF)", from: "BC River Forecast Centre",
    shows: "Forecast flows on the Conditions view.", licence: "bcCopyright",
    // REQUIRED VERBATIM wherever a forecast appears (sources.ts MAP_CREDITS holds the same words)
    wording: "Forecast data provided by the BC River Forecast Centre, Province of British Columbia, and used under the "
      + "Province's copyright terms (https://www2.gov.bc.ca/gov/content/home/copyright). Users should use the "
      + "information on this website with caution and at their own risk.",
    url: "https://www2.gov.bc.ca/gov/content/environment/air-land-water/water/drought-flooding-dikes-dams/river-forecast-centre" },
  { k: "runs", name: "Salmon run timing", from: "Pacific Salmon Foundation (Pacific Salmon Explorer); Fisheries and "
      + "Oceans Canada (NuSEDS)", shows: "The Run timing map and lines.", licence: "permission",
    wording: "Pacific Salmon Foundation, used with permission. NuSEDS: " + OGL_CA_WORDS,
    url: "https://www.salmonexplorer.ca" },
  { k: "weather", name: "Weather", from: "Open-Meteo.com", shows: "The weather on a saved spot.", licence: "ccBy",
    wording: "Weather data by Open-Meteo.com", url: "https://open-meteo.com" },
  { k: "imagery", name: "Satellite imagery", from: "Esri", shows: "The Satellite ground, online only, never stored.",
    licence: "esri", wording: "Powered by Esri · Source: Esri, Vantor, Earthstar Geographics, and the GIS User Community",
    url: "https://www.arcgis.com/home/item.html?id=10df2279f9684e4a9f6a7f08febac2a9" },
  { k: "icons", name: "Lucide icons", from: "Lucide Contributors", shows: "The app's icons.", licence: "isc",
    wording: null, url: "https://lucide.dev" },
];

export const LICENCE_WORDS = {
  title: "Licences",
  intro: "Every dataset the app shows, the licence it is used under, and the words that licence asks for.",
  shows: "Shows",
  licence: "Licence",
  /** a licence link's spoken name: one per row, so a screen reader can tell the rows' same-named links apart */
  licenceFor: (name: string, licence: string) => `Licence for ${name}: ${licence}`,
  words: "Required wording",
  open: "Open the publisher's page",
  /**
   * ODbL 4.6: data we derive from OpenStreetMap and show publicly is offered under the ODbL. The file itself is an
   * owner call (T235); until it exists the offer is on request.
   */
  odbl: "Data this app derives from OpenStreetMap (some water names and two side channels) is © OpenStreetMap "
    + "contributors and available under the Open Database License: ask through Contact or Report a problem.",
  /** the About page's link here */
  fromAbout: "Sources and licences",
} as const;

export interface LicenceRowVM extends LicenceRow {
  readonly licenceLabel: string;
  readonly licenceUrl: string | null;
}
export interface LicencesVM {
  readonly title: string;
  readonly intro: string;
  readonly rows: readonly LicenceRowVM[];
  readonly odbl: string;
}

export function licencesVM(): LicencesVM {
  return {
    title: LICENCE_WORDS.title, intro: LICENCE_WORDS.intro, odbl: LICENCE_WORDS.odbl,
    rows: LICENCE_ROWS.map((r) => ({ ...r, licenceLabel: LICENCES[r.licence].label, licenceUrl: LICENCES[r.licence].url })),
  };
}
