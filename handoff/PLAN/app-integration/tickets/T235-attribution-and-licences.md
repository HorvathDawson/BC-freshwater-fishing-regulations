# T235 — data attribution: credits on the map, on the sheets, and an About → Sources & licences page

Status: DONE — cf41b443 (web walked desktop + phone; native style unit-tested, not walked on a device)

Source: owner 2026-10-28 — approved proposal P1 ("credits must live in the proper location, like attribution on the
map").

## Built
1. **Map credit control names what is drawn (web + native)** — `app/packages/map/src/credits.ts`.
   - Always-drawn sources carry their own credit: basemap `© OpenStreetMap contributors` (linked to
     openstreetmap.org/copyright, ODbL wording) · © Protomaps; atlas `Province of British Columbia (OGL-BC) · Freshwater
     Atlas & Freshwater Fishing Synopsis · some names and channels © OpenStreetMap contributors`; gauges ECCC (as before);
     Esri while satellite is drawn (as before).
   - A mode/overlay that paints on the atlas gets its own empty GeoJSON source holding only its credit plus a layer that
     draws nothing, made visible by `applyLook` while the look draws that data: Salmon → DFO ("not an official
     version"), Run timing → PSF (with permission) + DFO (OGL-Canada), Stocked → Province (OGL-BC), Fish obstacles →
     FISS/WRIS/PSCIS (OGL-BC). Web: MapLibre lists sources a visible layer reads (verified: salmon credit appears on
     switching to Salmon). Phone: `nativeStyle` drops a hidden credit (MapLibre Native lists every source), as it does Esri.
   - The compact ⓘ stays collapsed on arrival (unchanged); one tap expands it.
2. **Sheets**: already credited and checked — salmon card ("Source: … DFO", page dates), Conditions fold (ECCC, the RFC
   verbatim notice, weather), obstacle sheet (source row + OGL-BC line), run timing (`runCredit`, T199 lane), name source
   labels (T224). No change needed.
3. **Sources and licences page** (More → "Sources and licences", and a link on About): the existing source list + a new
   Licences section (`core/menu/licences.ts`, `ui-native/menu/Licences.tsx`): 17 datasets, each with publisher, what the
   app shows, licence (linked) and the licence's required wording (OGL-BC, OGL-Canada, ODbL, DFO non-endorsement, RFC
   verbatim, Esri, Open-Meteo CC BY, PSF permission, Lucide ISC), (the ODbL offer line was removed 10-28).
4. Tests: `map/src/credits.test.ts`, `core/src/menu/legal.test.ts` (licences part), `ui-native/src/Legal.test.tsx`;
   updated menu/surfaces pins for the new label. Shots: `handoff/PLAN/app-integration/T235-shots/`
   (map-credits desktop/phone, map-credits-salmon, sources-licences desktop/phone).

## ODbL — DECIDED (owner 2026-10-28)
No separate `osm-derived.json`. OpenStreetMap is credited through the map attribution (the basemap pmtiles already come
from OSM) and on Sources and licences; the "available on request / offered under ODbL" line is removed. ODbL's
share-alike for derived databases (177 accepted OSM names + the Northrop geometry) is an owner-accepted risk, not legal
advice.

## ODbL — REVISED (owner 2026-10-28, later the same day): offered under the ODbL from the public repo
The repo is public, so its OSM-derived data is already public: each OSM-derived committed file now carries a top-level
`_licence` record (spdx ODbL-1.0, "© OpenStreetMap contributors", `applies_to`, url) and `data/LICENSE-ODbL.md` lists
exactly which files/entries are ODbL (the rest of the repo is not). Two bare-list files became
`{"_licence", "<list>"}` (`mined_names.json` → `entries`, `bc_places.json` → `places`); readers unwrap through
`pipeline/common/licence.py` `entries` (`load_name_variants`, bundle `_places`, tiles `export_places`), and the writers
(`tools/mined_names`, `fetch_osm_places`, `added_streams.build_dataset.write`, `tidal_words osm`) emit the record. Tests:
`pipeline/tests/test_licence_records.py` (record present + listed, loaders return the same entries as HEAD). The
Sources and licences page links the files on GitHub (`licences.ts` `ODBL_FILES`, `REPO_BLOB` = the
`redesign/stream-sections` branch — flip on merge) and has a new row **No-access land and reserves (OpenStreetMap)**;
the `access` row no longer claims no-access land for the Province. The atlas credit now reads "some names, waters,
places and access land © OpenStreetMap contributors". Owner decision, recorded; not legal advice.

### Audit: every shipped dataset (2026-10-28)
| dataset | ships as | source | licence | OSM? |
|---|---|---|---|---|
| basemap `bc.pmtiles` | basemap tiles | Protomaps build of OSM | ODbL (Produced Work: attribution) | yes (credited) |
| `land_access` (Coquitlam etc. watersheds, DND land, nature reserves; **Malcolm Knapp** geocoded by name via osmnx/Nominatim) | tile `no_access`, bundle `access_area`/`section_access` | Overpass (`fetch_overpass_land_access`) | ODbL | **yes — newly found** |
| `aboriginal_lands` (Indian reserves, treaty land) | tile `indigenous_land`, bundle `access_area` tier reserve | Overpass (`fetch_overpass_aboriginal_lands`) | ODbL | **yes — newly found** (not CLSS/Canada Lands) |
| `bc_places.json` gazetteer | bundle `place` (search), `near` | Overpass (`fetch_osm_places`) | ODbL | **yes — newly found** (committed) |
| `mined_names.json` | names in atlas + bundle | 182 OSM (basemap), 9 municipal, 5 gauge | ODbL / city OGL / OGL-Canada | part |
| `added_lakes.geojson` | atlas lakes | ids 2–6 from OSM (Marsh Pond, Children's Fishing Pond, Cheam Lake, Minnekhada Marsh, Cahilty Lake); 10 manual | ODbL / own | **part — newly found** |
| `added_streams.json` + `osm_streams.geojson` | atlas streams | Northrop Channel OSM; 361 from 3 city layers (+ Abbotsford names) | ODbL / city OGL | part |
| `splits.json` | cut points in atlas | 52 anchor coords placed on an OSM way/node (bridges, dams, falls); rest FWA/curator | ODbL (the 52) / own | **part — newly found** |
| `added_areas.geojson` (2 sign zones) | tile `sign_zone` | curator-drawn, OSM only consulted | own | no |
| `name_variants.json` | names | synopsis + curator (notes mention OSM) | own / Province | no |
| `areas.json` defs; parks_bc, eco reserves, WMA, WMU/regions, historic sites | tiles `park`/`eco_reserve`/`wma`/`mu`/`region` | BC Data Catalogue (TANTALIS, WAA, HIST) | OGL-BC | no |
| national parks | tile `park` (national) | `WHSE_ADMIN_BOUNDARIES.CLAB_NATIONAL_PARKS` via BC catalogue | OGL-BC as listed (CLAB is Canada Lands data; recheck) | no |
| private land | tile `parcel`, bundle `access_area` tier private | ParcelMap BC parcel fabric | OGL-BC | no |
| FWA streams/lakes/wetlands/watersheds, GNIS names, bathymetry contours | atlas | Province | OGL-BC | no |
| tidal boundary / PFMA | tile `tidal_water` | DFO via R2 / BC catalogue | OGL-Canada / OGL-BC | no |
| obstacles (FISS, WRIS dams, PSCIS) | tile `obstacle` | Province | OGL-BC (PSCIS owner Q165) | no |
| gauges, HYDAT | bundle, feeds | ECCC | OGL-Canada | no |
| BC outline / outside mask | `bc_outside.geojson` | union of WMUs (Province) | OGL-BC | no |
| city stream layers | atlas streams | 4 cities | city OGLs | no |
| synopsis, in-season, DFO salmon, run timing, stocking, forecasts, weather, imagery, icons | bundle/feeds | as the table above | as above | no |
| not shipped: `waterfalls`, `water_access_points` gpkg layers; `tidal-osm-landmarks.json` (curation cache, committed: record added) | — | Overpass | ODbL | yes, not shipped |
| roads | not shipped (forest service roads layer unused; roads are the basemap's) | — | — | — |

## City stream layers (2026-10-28 follow-up) — one row each on Sources and licences
Sources: `data/source/municipal/*.geojson` (pipeline/atlas/waters/added_streams/SOURCES.md); no others.
| city | licence | page | wording on the page |
|---|---|---|---|
| Abbotsford | City of Abbotsford Open Data Licence (OGL-Canada 2.0 variant) | https://opendata-abbotsford.hub.arcgis.com/pages/city-of-abbotsford-open-data-licence | default form (see caveat) |
| Burnaby | Open Government Licence – City of Burnaby (OGL-BC 2.0 variant) | https://data.burnaby.ca/pages/open-government-licence | default form (see caveat) |
| Port Moody | Open Data Terms of Use = Open Government Licence – Port Moody | https://www.portmoody.ca/city-government/open-data-portal/open-data-terms-of-use/ | "Contains information licensed under the Open Government Licence – Port Moody." (read from the page) |
| Squamish | Open Government Licence – Squamish (OGL-BC 2.0 variant) | https://squamish.ca/government-and-administration/maps-and-data/open-data/ (portal data.squamish.ca) | "Contains information licensed under the Open Government Licence – Squamish." (from search of the page; fetch was 403) |
All four are on the OSMF LWG "OGL Canada and local variants" compatible list (https://osmfoundation.org/wiki/OGL_Canada_and_local_variants).
Caveat: the Burnaby and Abbotsford pages are JS-rendered and could not be read verbatim; the licences are confirmed, the
wording is the licences' standard default ("Contains information licensed under the Open Government Licence – <city>.").
Recheck on the pages in a browser (owner, 2 min); Abbotsford's own text may use "licenced" / "City of Abbotsford".

## Not done / follow-ups
- Native attribution not checked on a device (MapLibre RN shows the style's source credits behind its ⓘ; the style
  logic is unit-tested). Rides T77 phone testing.

Depends: none. Size: S. Owner: confirm the Burnaby/Abbotsford default wording on their licence pages.
