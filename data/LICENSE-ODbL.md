# OpenStreetMap-derived data in this repository: ODbL 1.0

Some data in this repository was taken from **OpenStreetMap**: © OpenStreetMap contributors. It is available under the
**Open Database License (ODbL) 1.0**: <https://opendatacommons.org/licenses/odbl/1-0/>
(about OSM's licence: <https://www.openstreetmap.org/copyright>).

The repository is public. The OpenStreetMap-derived data listed below is **offered under the ODbL 1.0**. You may copy,
share and adapt it, but you must credit "© OpenStreetMap contributors", and anything you publish that is derived from it
must also be under the ODbL. This was the owner's decision on 2026-10-28. It is a record of that decision and is not
legal advice.

**Only the files and entries listed here are under the ODbL.** Everything else in the repository has its own terms:
the code, the curation, and data from other publishers (Province of British Columbia under the OGL-BC, the Government
of Canada under the OGL-Canada, the four cities' stream layers under their own open government licences, and so on).
The app's About → Sources and licences page lists those sources and their licences. Taking an ODbL entry out of a file
does not put the rest of that file under the ODbL.

Each JSON file below holds its own licence record in a top-level `_licence` key: `spdx`, `attribution`, `applies_to`
(which entries or fields), `url`. Loaders skip this key (`pipeline/common/licence.py`, with tests in
`pipeline/tests/test_licence_records.py`).

## Committed files

| file | what is OpenStreetMap data | how it was taken |
|---|---|---|
| `data/source/bc_places.json` | the whole list under `places` (name, alternative names, place kind, population, position). `rank` is this repo's own ordering. | Overpass, `data/fetch_data.py` `fetch_osm_places` |
| `data/source/municipal/osm_streams.geojson` | every feature (Northrop Channel) | Overpass, by hand (T222) |
| `data/curated/waters/added_streams.json` | entries in `streams[]` and `name_variants[]` with `source: "osm"` (Northrop Channel, blk -2300000000). The other entries come from city layers. | built from `osm_streams.geojson` |
| `data/curated/waters/added_lakes.geojson` | features with `properties.source: "osm"`: ids 2 Marsh Pond, 3 Children's Fishing Pond, 4 Cheam Lake, 5 Minnekhada Marsh, 6 Cahilty Lake (their polygons) | Overpass, reviewed by hand 2026-09-03 |
| `data/curated/waters/mined_names.json` | each `entries[].names[]` record with `source: "osm"` (182 names). The `municipal` and `gauge` records are not OSM data. | mined from the Protomaps basemap, which is OSM (T224) |
| `data/curated/waters/splits.json` | the anchor `coord` of the 52 splits listed in `_licence.ids`. Each was placed on an OSM bridge, dam, weir, falls, rail line or road crossing. | OSM way/node named in the split's `_note`, or an `[osm-candidate]` road × river intersection |
| `handoff/PLAN/curation-review/tidal/tidal-osm-landmarks.json` | every way in `ways` (curation working file, not shipped) | Overpass, `pipeline/tools/audit/tidal_words.py osm` |

## Built from OpenStreetMap but not committed

These are made by `data/fetch_data.py` into the build GeoPackage (`aboriginal_lands`, `land_access`), and from there go
into the published map tiles and the app's data bundle. They are OSM-derived and are offered under the ODbL in the same
way. Anyone can rebuild them from OpenStreetMap with `data/fetch_data.py`, and they can also be read from the
published tiles.

| built layer | shipped as | source |
|---|---|---|
| `land_access` (restricted and permit-only land: Coquitlam and other municipal watersheds, DND land, private nature reserves, and the Malcolm Knapp Research Forest, which is geocoded from OSM by name) | tile layer `no_access`; bundle tables `access_area` / `section_access` | Overpass + OSM Nominatim (`fetch_overpass_land_access`) |
| `aboriginal_lands` (Indian reserves and treaty lands) | tile layer `indigenous_land`; bundle `access_area` (tier `reserve`) | Overpass (`fetch_overpass_aboriginal_lands`) |
| `bc_places.json` (above) | bundle `place` table (search) and the `near` place-to-water table | — |
| `mined_names.json`, `added_*` (above) | names and shapes in the atlas tiles and the bundle | — |

Snapshot bundles kept for development under `app/packages/data/dev/*-release/` contain these same tables.

The **basemap** (`bc.pmtiles`, Protomaps) is made from OpenStreetMap. It is a Produced Work that needs attribution:
"© OpenStreetMap contributors" appears on the map and on the Sources and licences page. It is not stored in this
repository.
