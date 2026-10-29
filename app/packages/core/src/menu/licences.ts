/**
 * (T235, owner 2026-10-28) SOURCES AND LICENCES: every dataset the app shows, who publishes it, the licence or
 * permission it is used under (linked), and the words that licence asks for — on the Sources and licences page (More,
 * and About's link). The map's own credit control names the same publishers (`@app/map` credits.ts); a sheet that shows
 * one source's data credits it at its foot (the salmon card, the Conditions fold, the obstacle sheet, run timing).
 *
 * Words only, no dates: each source's freshness is the More tab's (freshness.ts).
 */
import { MENU_WORDS } from "./words";

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
  oglAbbotsford: { label: "City of Abbotsford Open Data Licence",
                   url: "https://opendata-abbotsford.hub.arcgis.com/pages/city-of-abbotsford-open-data-licence" },
  oglBurnaby: { label: "Open Government Licence – City of Burnaby", url: "https://data.burnaby.ca/pages/open-government-licence" },
  oglPortMoody: { label: "Open Government Licence – Port Moody (Open Data Terms of Use)",
                  url: "https://www.portmoody.ca/city-government/open-data-portal/open-data-terms-of-use/" },
  oglSquamish: { label: "Open Government Licence – Squamish",
                 url: "https://squamish.ca/government-and-administration/maps-and-data/open-data/" },
} as const;
export type LicenceKey = keyof typeof LICENCES;

/**
 * (owner 2026-10-28) THE REPO IS PUBLIC, so the OpenStreetMap-derived data in it is offered under the ODbL from there:
 * each file carries its own `_licence` record and `data/LICENSE-ODbL.md` lists exactly which files and entries. These are
 * links to those files on GitHub. The branch is the one the data lives on; change `REPO_BLOB` when it merges.
 */
export const REPO_URL = "https://github.com/HorvathDawson/BC-freshwater-fishing-regulations";
export const REPO_BLOB = `${REPO_URL}/blob/redesign/stream-sections`;
const repoFile = (path: string, label = path.split("/").pop()!) => ({ label, url: `${REPO_BLOB}/${path}` });

/** A data file in the repo a row's licence applies to, linked. */
export interface LicenceFile { readonly label: string; readonly url: string }

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
  /** (ODbL) the files in this repo that hold the data, linked on GitHub; absent: none */
  readonly files?: readonly LicenceFile[];
}

/** (owner 2026-10-28) the OpenStreetMap-derived files in the repo, offered under the ODbL (data/LICENSE-ODbL.md). */
export const ODBL_FILES: readonly LicenceFile[] = [
  repoFile("data/LICENSE-ODbL.md", "LICENSE-ODbL.md (which files and entries)"),
  repoFile("data/source/bc_places.json"),
  repoFile("data/curated/waters/mined_names.json"),
  repoFile("data/curated/waters/added_lakes.geojson"),
  repoFile("data/curated/waters/added_streams.json"),
  repoFile("data/source/municipal/osm_streams.geojson"),
  repoFile("data/curated/waters/splits.json"),
];
/** the fetch that builds the access land and reserve layers from OpenStreetMap (not committed: built, then shipped) */
export const ODBL_LAND_FILES: readonly LicenceFile[] = [
  repoFile("data/LICENSE-ODbL.md", "LICENSE-ODbL.md (which files and entries)"),
  repoFile("data/fetch_data.py", "fetch_data.py (how the layers are built)"),
];

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
  { k: "fwa", name: MENU_WORDS.sources.names.fwa, from: "Province of British Columbia",
    shows: "Every stream, lake and wetland on the map, and their names.", licence: "oglBc", wording: OGL_BC_WORDS,
    url: "https://www2.gov.bc.ca/gov/content/data/geographic-data-services/topographic-data/freshwater" },
  { k: "gnis", name: "BC Geographical Names", from: "Province of British Columbia",
    shows: "Official water names.", licence: "oglBc", wording: OGL_BC_WORDS,
    url: "https://apps.gov.bc.ca/pub/bcgnws/" },
  { k: "osm", name: "OpenStreetMap", from: "© OpenStreetMap contributors",
    shows: "The basemap (roads, towns, land), the towns search finds, some local water names, a side channel the "
      + "Atlas does not have (Northrop Channel), five lake and pond outlines, and the position of some bridges, dams "
      + "and falls that mark where a regulation starts or ends. Offered under the ODbL in the app's public repository.",
    licence: "odbl", wording: "© OpenStreetMap contributors", url: "https://www.openstreetmap.org/copyright",
    files: ODBL_FILES },
  { k: "osmland", name: "No-access land and reserves (OpenStreetMap)", from: "© OpenStreetMap contributors",
    shows: "Land the public may not enter or may enter only with a permit (municipal watersheds such as Coquitlam, "
      + "military land, the Malcolm Knapp Research Forest), and Indian reserve and treaty land outlines.",
    licence: "odbl", wording: "© OpenStreetMap contributors", url: "https://www.openstreetmap.org/copyright",
    files: ODBL_LAND_FILES },
  { k: "protomaps", name: "Protomaps basemap", from: "© Protomaps", shows: "The basemap's tiles and style.",
    licence: "odbl", wording: "© Protomaps", url: "https://protomaps.com" },
  { k: "abbotsford", name: "City of Abbotsford stream layer", from: "City of Abbotsford",
    shows: "Streams the Atlas does not have, and their local names (Abbotsford).", licence: "oglAbbotsford",
    wording: "Contains information licensed under the Open Government Licence – City of Abbotsford.",
    url: "https://opendata-abbotsford.hub.arcgis.com" },
  { k: "burnaby", name: "City of Burnaby waterways layer", from: "City of Burnaby",
    shows: "Streams the Atlas does not have, and their local names (Burnaby).", licence: "oglBurnaby",
    wording: "Contains information licensed under the Open Government Licence – City of Burnaby.",
    url: "https://data.burnaby.ca" },
  { k: "portmoody", name: "City of Port Moody stream layer (ESA streams)", from: "City of Port Moody",
    shows: "Streams the Atlas does not have, and their local names (Port Moody).", licence: "oglPortMoody",
    wording: "Contains information licensed under the Open Government Licence – Port Moody.",
    url: "https://www.portmoody.ca/city-government/open-data-portal/" },
  { k: "squamish", name: "District of Squamish watercourse layer", from: "District of Squamish",
    shows: "Streams the Atlas does not have, and their local names (Squamish).", licence: "oglSquamish",
    wording: "Contains information licensed under the Open Government Licence – Squamish.",
    url: "https://data.squamish.ca" },
  { k: "access", name: "Parks, ecological reserves and land title", from: "Province of British Columbia",
    shows: "National and provincial parks, ecological reserves, wildlife management areas and private land "
      + "(ParcelMap BC) on the map.", licence: "oglBc", wording: OGL_BC_WORDS,
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
  /** (ODbL) the row's files in the app's public repository */
  files: "Files (in the app's public repository)",
  /** a file link's spoken name: per row, as `licenceFor` (two rows link the same LICENSE-ODbL.md) */
  filesFor: (name: string, file: string) => `Files for ${name}: ${file}`,
  open: "Open the publisher's page",
  /** the About page's link here */
  fromAbout: "Sources and licences",
  /** the About page's link, with its go mark */
  fromAboutLink: "Sources and licences ›",
} as const;

export interface LicenceRowVM extends LicenceRow {
  readonly licenceLabel: string;
  readonly licenceUrl: string | null;
}
export interface LicencesVM {
  readonly title: string;
  readonly intro: string;
  readonly rows: readonly LicenceRowVM[];
}

export function licencesVM(): LicencesVM {
  return {
    title: LICENCE_WORDS.title, intro: LICENCE_WORDS.intro,
    rows: LICENCE_ROWS.map((r) => ({ ...r, licenceLabel: LICENCES[r.licence].label, licenceUrl: LICENCES[r.licence].url })),
  };
}
