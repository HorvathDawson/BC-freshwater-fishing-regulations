# Raw municipal stream layers (uncurated)

Four municipal GeoJSON exports in **`pipeline/added_streams/data/`** as **source material** for the
added-streams batch. `clean.py` normalizes them into `data/cleaned/<source>.geojson` (uniform schema,
ArcGIS cruft stripped) and `build_dataset.py` matches them against FWA and mints the kept ones — see
the "Municipal batch" section of `README.md`. Treat these raw files as candidate pools, not the
curated schema.

All four are `FeatureCollection` in **WGS84 lon/lat** (`CRS84` / `EPSG:4326`), geometry is
`LineString` with a minority of `MultiLineString`. Coordinates are 2D (no Z).

| file | source | features | extent | named |
|---|---|---|---|---|
| `port_moody_esa_streams.geojson` | City of Port Moody — Environmentally Sensitive Areas | 484 (481 LS / 3 MLS) | −122.928…−122.814, 49.264…49.337 | 247 |
| `burnaby_waterways.geojson` | City of Burnaby — Waterway | 402 (370 / 32) | −123.027…−122.889, 49.181…49.293 | 306 |
| `squamish_watercourses.geojson` | District of Squamish — Watercourse | 670 (509 / 161) | −123.260…−123.017, 49.639…49.873 | 68 |
| `abbotsford_streams.geojson` | City of Abbotsford — Environment Layers | 18,508 (18,445 / 63) | −122.470…−122.064, 49.002…49.168 | 3,948 |

---

## `port_moody_esa_streams.geojson` — 484 features

Top-level `name: "ESA_Streams"`. Features carry no `id`; identity is `OBJECTID` / `gis_id`.

| prop | type | non-null | notes |
|---|---|---|---|
| `OBJECTID` | int | 484/484 | 2…500, unique |
| `gis_id` | int | 484/484 | 2…588, unique (parallel key, ≠ OBJECTID) |
| `theme` | str | 484/484 | `Stream` 359, `Culvert` 105, `Ditch` 20 |
| `local_stream_name` | str | 247/484 | 29 distinct — the usable name field. One junk value `"hide"` |
| `stream_names` | str | 88/484 | 6 distinct, subset of the above; redundant |
| `stream_type` | str | 260/484 | **dirty**: `HIDDEN DRAINAGE` 94, `STREAM/CREEK` 65, `TRIM DATA` 55, `DITCH` 25, `ADDED WATER FEATURES` 15, plus `Stream`/`STREAM`/`ADDED FEATURES` casing dupes |
| `stream_feature_code` | str | 465/484 | `NUTRIENT SOURCE TO FISH DOWNSTREAM` 161, `CULVERTED` 134, **`KNOWN SALMON HABITAT` 107**, `POTENTIALLY FISH BEARING` 30, `NO VALUE` 17, misc |
| `description` | str | 161/484 | free text; mostly `DRAINS TO PORT MOODY` / `CONNECTED TO PORT MOODY DRAINAGE SYSTEM`. **14 rows encode topology** — `TRIBUTARY TO SUTER BROOK`, `DRAINS TO BURNABY LAKE`, etc. |
| `Shape__Length` | float | 484/484 | 0.39…4037 — **metres** (projected source), not degrees |

Names: Axford, Correl Brook, Dallas, Elginhouse, Goulet, Hatchley, Hett, Hutchinson, Kyle, Melrose,
Mossom, Noble, Noons, Ottley, Pigeon, Schoolhouse Creek North, Schoolhouse South (+Tributary),
Slaughterhouse, Stoney, Suter Brook, Turner, Village, West Noons, West Sundial, Wilkes, Williams,
Windermere.

**Curation notes** — `theme` is the clean filter (drop `Culvert`/`Ditch`, or keep culverted reaches
as connectors). `stream_feature_code` is a free fish-bearing signal. The `description` tributary
strings are hand-written `connect_to` hints.

---

## `burnaby_waterways.geojson` — 402 features

Top-level `name: "Waterway"`. Municipal storm/watercourse asset layer, so props are asset-management,
not hydrology.

| prop | type | non-null | notes |
|---|---|---|---|
| `OBJECTID` | int | 402/402 | 1…412, unique |
| `WATERWAYNAME` | str | 306/402 | **290 distinct** — richest naming of the four. Encodes hierarchy in the string: `Beaver Creek`, `Beaver Trib.1`, `Beaver Trib.1-1` |
| `COMPKEY` | int | 319/402 | 0…687952 — Burnaby asset component key |
| `COMPTYPE` | int | 402/402 | only `28` (339) and `0` (63) — degenerate |
| `UNITID` / `UNITID2` | str | 322/402 | upstream/downstream node codes: `ALCMUS`/`ALCMDS`, `ALCT1US`/`ALCT1DS`. **`US`/`DS` suffix = an explicit topology graph** |
| `MAINCOMP1` / `MAINCOMP2` | int | 402/402 | only `32`/`0` — degenerate |
| `OWNER` | str | 322/402 | always `CITY` |
| `SHEETNO` | — | 0/402 | **entirely null**, drop |
| `SHAPE_Length` | float | 402/402 | 1.38…6848 — metres |

**Curation notes** — the best candidate for the Burnaby Lake / Deer Lake gap the README calls out
(Byrne, Beecher, Buckingham, Stoney, Beaver…). Two free wins: `Trib.N` naming gives parent→child
structure without geometry work, and `UNITID`/`UNITID2` give a ready-made connectivity graph you can
resolve into `connect_to` instead of relying on the 250 m auto-detect.

---

## `squamish_watercourses.geojson` — 670 features

Richest *biological* attribution, sparsest naming. 161 MultiLineStrings (24 %) — highest of the four.

| prop | type | non-null | notes |
|---|---|---|---|
| `OBJECTID` | int | 670/670 | 1…685, unique |
| `StreamName` | str | 68/670 | only 38 distinct. **Dirty**: `Wilson Slough`/`Whitaker Slough`, `Loggers Lane Creek`/`Logger's Lane Creek` are apostrophe dupes |
| `StreamDetail` | str | 111/670 | 73 distinct, finer than `StreamName`: `Brennan Park Creek Trib 1 (Unnamed)` |
| `Contributing_to_DS` | str | 569/670 | **downstream receiver by name** — `Alice Lake`, `Brohm Lake`, `Squamish River`. Direct `connect_to` material, 85 % populated |
| `Fish_Beari` | str | 666/670 | **dirty**: `U` 568, `Yes` 41, `Y` 18, `Unconfirmed` 17, `Potential` 13, `No` 9 — `Y`/`Yes` and `U`/`Unconfirmed` need collapsing |
| `Status` | str | 668/670 | `Unconfirmed` 537, `SHIM Completed` 80, `Named Creek` 35, `Unconfirmed - Probable` 10, + a trailing-space dupe `'Unconfirmed '` and one `''` |
| `Width_BF` | int+float | 666/670 | 0…80 m bankfull; **mixed int/float** |
| `Buffer_Distance` | int+float | 669/670 | 0…40 m riparian setback; mixed int/float |
| `Spp_Pres` | str | 588/670 | comma-joined species codes, but **571 of 588 are literally `NA`** — only ~17 rows carry real data |
| `Spp_Rich` | str | 17/670 | species count **stored as a string** (`"14"`, `"6"`) |
| `Fish_Label` | str | 17/670 | display-formatted duplicate of `Spp_Pres` |

Real species codes (~17 rows): `CO` coho 13, `RB` rainbow 13, `ST` steelhead 13, `CT` cutthroat 11,
`DV` Dolly Varden 10, `CM` chum 9, `CC` 8, `CH` chinook 7, `PK` pink 5, `SK` sockeye 4, plus BT, CAL,
CAS, TSB, SB, L, SP, CCT, AC, DC, BNH, SH, AS, ACT, GSG, MW, PL, TR.

**Curation notes** — `Contributing_to_DS` is the single most valuable field across all four files:
an explicit named receiver on 569 features. The species columns look rich but are ~97 % `NA`; don't
plan around them.

---

## `abbotsford_streams.geojson` — 18,508 features

Largest by 20×, thinnest attribution. Feature-level `"id"` present (mirrors `OBJECTID`). Includes an
ArcGIS edit-tracking block — all four of those columns are constant and droppable.

| prop | type | non-null | notes |
|---|---|---|---|
| `OBJECTID` | int | 18508/18508 | 314573…333080, unique |
| `GlobalID` | str | 18508/18508 | UUID, unique |
| `STREAM_NAME` | str | 3,948/18508 | **144 distinct, ALL-CAPS** — needs title-casing to match `name_variants.json`. One outlier is mixed-case (`Boucher McKee Spring`). Whitespace-only values (`" "`, `"  "`) present |
| `FISH_CLASS` | str | 18507/18508 | see below |
| `created_user` / `last_edited_user` | str | 18508/18508 | constant `ngwebmapadmin_abbotsford` — **drop** |
| `created_date` / `last_edited_date` | str | 18508/18508 | 2 distinct RFC-1123 strings (bulk-load timestamps) — **drop** |

`FISH_CLASS` (the only substantive attribute):

| value | count |
|---|---|
| `UNOFFICIAL: CLASS A (RED)` | 6,053 |
| `UNOFFICIAL: CLASS B1 (ORANGE)` | 5,187 |
| `UNOFFICIAL: CLASS C2 (DASHEDGREEN)` | 2,316 |
| `UNOFFICIAL: CLASS B2 (YELLOW)` | 2,050 |
| `UNOFFICIAL: CLASS C1 (GREEN)` | 1,997 |
| `UNOFFICIAL: UNCLASSIFIED (BLUE)` | 901 |
| `Unclassified` / `null` | 3 / 1 |

Class A = fish-bearing, B = potentially fish-bearing, C = non-fish-bearing (standard BC riparian
classification); the `UNOFFICIAL:` prefix and colour suffix are map-legend cruft. Names skew to
`DITCH-nn-X` codes, so the 144 "names" are fewer real creeks than the count suggests.

**Curation notes** — 78 % unnamed and heavy on ditches. Filtering to `CLASS A`/`B1` with a non-null,
non-`DITCH-` `STREAM_NAME` cuts this to a workable set. ALL-CAPS names must be normalised before they
will match anything.

---

## Cross-file summary

| field you need | best source |
|---|---|
| stream name | `burnaby_waterways.WATERWAYNAME` (290 distinct) |
| downstream receiver (`connect_to`) | `squamish.Contributing_to_DS` (569/670), then `burnaby.UNITID2`, then `port_moody.description` |
| parent/tributary hierarchy | `burnaby.WATERWAYNAME` `Trib.N` strings |
| fish-bearing flag | `abbotsford.FISH_CLASS`, `port_moody.stream_feature_code`, `squamish.Fish_Beari` |
| species detail | `squamish.Spp_Pres` (only ~17 usable rows) |

Common gotchas: `MultiLineString` in all four needs flattening or a merge decision; every
`Shape__Length` / `SHAPE_Length` is metres from a projected original, not degrees; four separate
`OBJECTID` spaces overlap, so mint your own negative `blk` per README rather than reusing them.
