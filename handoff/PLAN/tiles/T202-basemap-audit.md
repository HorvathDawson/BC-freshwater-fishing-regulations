# T202: Basemap audit (measure + propose)

Scope: `basemap.pmtiles`, the OpenStreetMap ground under the atlas. This round only measured and proposed: no live
tiles touched, no pipeline edits, no commits. The atlas has its own audit (`T202-regulation-audit.md`).

## What ships today, and how it is built
- **Live file:** `data/generated/tiles/basemap.pmtiles`, **918.4 MB**, built 2026-09-25. It is gzip MVT, extent 4096,
  z0–14, 664k unique tiles. No release manifest names a basemap yet (`r-c79331d78adf` has no `basemap` block).
- **Source:** `data/source/bc.pmtiles` (4.16 GB, a Protomaps planetiler build of OSM). The schema is
  `@protomaps/basemaps` 5.7.2.
- **Build:** `pipeline/deliver/tiles/basemap.py`. It does **not** re-tile: `pmtiles extract` clips to BC + 5 km at
  z≤14, then `tile-join -l <7 layers>` drops buildings and pois.
  - The live file **predates the T85 feature filter** (`-j`: landuse kinds the style does not draw, and OSM
    `stream` lines). Its `generator_options` has no `-j`. The next build applies it.
- **What the app draws:** `runtime-style.ts` uses the whole Protomaps layer list (`layers("basemap", flavor)`), with
  three changes: the lake and waterway label layers are removed (we label water from the FWA), park and military
  kinds are taken out of `landuse_park`, and the source `maxzoom` is `BASEMAP_MAX_ZOOM = 14` (`core/src/ladder.ts`).
  - Minor-road, path and track names (`roads_labels_minor`) draw from **z15 only**, so today they come from
    overzoomed z14 tiles.
  - Access points (boat launches) are not in the basemap. Protomaps puts them in `pois`, which the build already
    drops. Ours come from the bundle (`access-*.bin`).

Method: `pipeline.tools.audit.tile_sizes` for zoom and layer totals. A scratch decoder (`bm/bm_scan.py`,
`bm/bm_scan2.py`, reusing the atlas audit's `attr_scan.py`) did three more jobs:
- split every unique tile by layer, kind and key;
- found the biggest tiles;
- priced each trim by re-encoding sampled tiles without it and gzipping them. It used every tile at z≤11, every 4th
  at z12, every 10th at z13 and every 25th at z14, scaled per zoom. A plain re-encode lands within 0.15% of the live
  bytes.

The zoom-cap proof uses real bbox builds through the build's own `_trim` (`bm/build_ex.py`) over four extents:
- Vancouver: −123.27, 49.19, −123.02, 49.32
- Chilcotin / Farwell Canyon (FSR-heavy): −123.0, 51.6, −122.4, 52.0
- Squamish backcountry (town plus Garibaldi trails): −123.35, 49.75, −122.95, 50.05
- Lac Le Jeune: −120.52, 50.44, −120.42, 50.50

`bm/zaudit.py` compares the z14 build's z14 tiles with the z13 build's z13 tiles **clipped to the same bbox**, per
(layer, kind, kind_detail, geometry type). It compares line km, polygon km² and point counts, and the name sets.

## Size table

| zoom | live MB | after T85 filter | | layer | live MB | z≤12 | z13 | z14 |
|---|---|---|---|---|---|---|---|---|
| z14 | 447.9 | 273.7 | | water | 407.7 | 41.8 | 135.9 | 230.0 |
| z13 | 259.3 | 155.9 | | landuse | 375.1 | 146.4 | 88.3 | 140.4 |
| z12 | 97.3 | 93.0 | | roads | 78.2 | 11.5 | 23.2 | 43.5 |
| z11 | 53.3 | 50.5 | | earth | 45.2 | 5.9 | 9.4 | 29.8 |
| z10 | 29.8 | 28.1 | | boundaries | 7.5 | 1.9 | 1.9 | 3.6 |
| z0–9 | 29.6 | 27.7 | | places + landcover | 3.5 | | | |
| **total** | **918.4** | **≈629** | | | | | | |

**Where the bytes go.** These are the biggest (layer, kind) groups, in gz MB across all zooms:

| group | MB | note |
|---|---|---|
| water stream lines | 241.7 | removed by the T85 filter |
| landuse wood | 220.4 | drawn: the green ground |
| water polygons | 56.1 | |
| landuse wetland | 51.5 | not drawn; kept as T64's stand-in |
| earth | 27.1 | |
| glacier | 26.6 | |
| river polygons | 25.8 | |
| roads path/track | 18.2 | |
| lake polygons | 12.4 | |
| ocean | 11.0 | |
| forest | 10.8 | |

Roads altogether are only 78 MB, so urban street thinning has little to win.

**Properties are already lean.** The Protomaps build carries almost no `name:xx` keys. The keys that are never read
cost about 19 MB raw:
- `sort_rank` on every layer;
- `ref` on roads (shields read `shield_text`);
- `min_zoom` on unnamed roads;
- `name` on water lines.

**Biggest tiles** are z7–9 landuse in the Rockies and on Vancouver Island, 120–200 KB each (wood and glacier at low
zoom).

## The z13 question: what a z13 cap really loses (owner rule 2026-10-27)
**Geometry: nothing anglers use.** On the same ground, every rural kind has the same line length at z13 as at z14
(±2–5%, which is simplification plus tile buffers). That covers track, path, unclassified, service, residential,
footway, bridleway and cycleway. Only urban kinds exist at z14 alone:

| kind (z14 only) | Vancouver km | Squamish backcountry km |
|---|---|---|
| path / sidewalk | 2,547 | 2.3 |
| path / crossing | 279 | 0.3 |
| subway | 75 | — |
| corridor | 19 | — |
| other / services, rest_area | 2.3 | 0.4 |
| living_street | 1.3 | — |
| platform, miniature rail | 1.9 | — |

Small playgrounds and sand areas also exist at z14 only (0.1 km² in Vancouver). So does T85's `pedestrian` landuse in
Squamish town.

**Names: every minor-road, path and track name is lost.** Protomaps strips them below z14 and merges the unnamed
lines. Names lost in a plain z13 cap:

| extent | names lost | examples |
|---|---|---|
| Chilcotin | 12 unclassified, 2 track, 2 path | 1300 Forestry Service Road, 2700 Road |
| Squamish backcountry | 156 track, 301 path, 16 unclassified, 3 footway | Ashlu River FSR, A100, Four Lakes Trail, Opal Cone Trail |
| Lac Le Jeune | 15 track, 11 path, 3 unclassified, 6 service | Ridge Mountain FSR, Walloper Lake FSR, the park access road |
| Vancouver | 1,043 residential plus every path and cycleway name | |

Other z14-only names:
- Lake, river and cliff label points. **The app does not draw these:** it removes the water label layers, and the
  style has no cliff layer.
- Two island names and one bay name in Vancouver (Deadman's Island, Richmond Island, Coal Harbour).
- No place or town names are lost.

**The fix, measured: re-tile the z13 `roads` layer from the z14 roads.** No app style change is needed.
1. Decode the z14 `roads` layer.
2. Clip each piece to its own tile (no buffer duplicates).
3. Line-merge per identical attribute set, as MultiLineStrings.
4. Drop the urban-only kinds above and `sort_rank`.
5. `tippecanoe -Z13 -z13 -l roads`, then `tile-join` it in place of Protomaps' z13 roads (`bm/roads14.py`).

Audit of the result (`bm/aud_*.json`, variant `13all` = `13nr` + `_allR`):
- **0 names lost and 0 km lost in any kept kind, in all four extents.** That covers every FSR, track, trail, path and
  footway name, plus every residential name. The only losses are the urban-only kinds listed above (sidewalk,
  crossing, corridor, subway, living_street, platform, rest_area).
- **Cost of the re-tiled z13 roads layer**, against Protomaps' unnamed z13 roads: Lac Le Jeune +0.4%, Squamish
  backcountry +3.8%, Chilcotin +4.4%, Vancouver +34%.
- **Province-wide estimate:** z13 roads are 23.2 MB. Split by tile road density, that is 11.1 MB rural, 8.2 MB mid
  and 3.9 MB urban, so **≈ +3 MB** in total.
- A variant that also stripped residential, footway, cycleway and steps names saved almost nothing (Vancouver 508 →
  481 KB). It also lost backcountry trail names, which OSM tags as `footway` (Four Lakes Trail, Opal Cone Trail).
  **Do not thin by class.**

**A separate `roads_names` label layer** over plain z13 roads was also built. It costs about the same (+0.8% to +7.9%
of the whole archive in the bboxes) and needs a cloned style layer. The re-tile is cleaner.

## Ranked wins (from the T85-filtered ≈629 MB build)

| # | change | saves (MB, province) | what is lost | risk / work |
|---|---|---|---|---|
| 1 | **Cap at z13 and re-tile z13 roads from z14** (names kept) | **≈ −271** (z14 is 273.7, re-tiled roads cost +3) → **≈358 MB** | urban only: sidewalks, crossings, corridors, subway, living streets, platforms, rest-area lanes, pedestrian landuse, small playgrounds, oneway arrows (z16+), 2 island and 1 bay label in Vancouver. **No trail, FSR, resource-road or rural name.** | med: a re-tile step in `basemap.py` (decode z14 roads, merge, tippecanoe, tile-join). `MAX_ZOOM` and `BASEMAP_MAX_ZOOM` go to 13, plus the overzoom test. The zoom audit needs an accepted-loss list (the kinds above) and must still fail on any lost rural kind or name. Add Vancouver and Chilcotin to `AUDIT_EXTENTS`. |
| 2 | **Drop properties and features nothing reads** | **−8** | nothing. These are `sort_rank`, roads `ref`, `min_zoom` on unnamed roads, `name` on water lines, lake/river label points (the app removed those layers) and earth cliff lines (no style layer). | low-med: needs a re-encode pass; `tile-join -x` covers keys only, not per-feature rules |
| 3a | **landuse at detail 2048** (z≤13) | **−22.5** | ≤0.8 m snap on fills (2 px at z16, 4 px at z18 when overzoomed from z13) | med: a requantize pass. Roads stay at 4096. Needs shots. |
| 3b | landuse at detail 1024, water and earth at 2048 | −54 | landuse ≤1.6 m (8 px at z18); coastlines and lake edges ≤0.8 m | med: shots at the owner's spots against imagery |
| 4 | **Drop the `wetland` stand-in** | **−32** (z≤13) | nothing drawn today (T85 kept it on purpose, one style layer away from use) | owner call |
| — | urban thinning by class at z13 (unnamed footway, steps, living_street, pedestrian) | −1.8 | **unnamed rural footpaths** (Lac Le Jeune has 4.5 km) | **no**, it breaks the trail rule |
| — | gzip-9 / brotli tiles | not measured; the atlas found −0.5% / −5.7% | — | same reader concerns as the atlas (brotli is unsupported in pmtiles JS / Native) |

**Stacked:** 1 + 2 is **≈350 MB** (from 918 live, or 629 after T85), with nothing lost that anglers use. Adding 3a
gives ≈327 MB. Adding 3b instead gives ≈296 MB. Adding 4 on top gives ≈265–295 MB.

## Owner calls
1. **Accept #1's urban losses** (the list in row 1)? They are the price of z13. Every trail, FSR and resource-road
   geometry and name survives, proven on four extents.
2. **Wetland stand-in:** keep it (0) or drop it (−32 MB)?
3. **Fill coarsening (3a / 3b):** go ahead after shots at z16–18?
4. **The two Vancouver island labels and the Coal Harbour bay label at z14:** accept the loss, or carry the
   earth/water label points in the same re-tile (a few KB)?

Artifacts (session scratchpad `…/scratchpad/bm/`):
- scanners: `bm_scan.py`, `bm_scan2.py`
- scan results: `live_all.json` (full scan), `p2_all.json` (z≤13 variants)
- builds and roads re-tile: `build_ex.py`, `roads14.py`
- audit and roads pricing: `zaudit.py`, `aud.sh`, `roadcmp.py`, `roadsplit.py`
- bbox builds and variants: `ex/`

## Built 2026-10-27 (owner decisions: z13 + roads rebuild, unread trims, wetland dropped; grid = investigate only)
- **Code:**
  - `pipeline/deliver/tiles/basemap.py`: `MAX_ZOOM` 13, `SOURCE_ZOOM` 14, `UNREAD_KEYS`, the earth/water point
    filter, and `LANDUSE_STANDIN = ()` with a note on how to bring the wetland back.
  - `pipeline/deliver/tiles/basemap_roads.py`: the z13 roads rebuild, merged per z11 block.
  - `app/packages/core/src/ladder.ts`: `BASEMAP_MAX_ZOOM` 13.
- **The build** joins three parts:
  - z0–12 with every kept layer;
  - z13 without roads;
  - the z13 roads rebuilt from z14 (tippecanoe, 4 threads).
  The zoom audit runs before every capped build and refuses one that loses anything.
- **Audit (6 extents: Squamish town, Lac Le Jeune, Vancouver, Chilcotin, Squamish backcountry):**
  - 0 kinds, 0 names and 0 road-length shortfalls outside the accepted list.
  - Losses are counted only inside the extent and inside each feature's own tile; buffers made false "losses" before.
  - Accepted: sidewalk, crossing, corridor, living_street, platform, services, rest_area, subway, miniature,
    ladder, narrow_gauge, pedestrian landuse, one Vancouver drain polygon, and the 3 named Vancouver labels.
- **Size:** live 918.4 MB → **322.5 MB**, province, z0–13 (168,856 unique tiles):

  | layer | MB |
  |---|---|
  | landuse | 181.7 |
  | water | 78.7 |
  | roads | 40.2 |
  | earth | 15.1 |
  | boundaries | 3.5 |
  | places | 2.6 |

  | zoom | MB |
  |---|---|
  | z13 | 144.8 |
  | z12 | 83.2 |
  | z11 | 44.3 |
  | z10 | 24.7 |
  | z≤9 | 25.5 |

- **Promoted** file by file:
  - `data/generated/tiles/basemap.pmtiles`, plus `basemap.meta.json` (`basemap-859406091819`), released to
    `data/release/basemap/basemap-859406091819/`.
  - The old file is kept until the owner looks: `data/generated/scratch/basemap-pre-T202-20260925.pmtiles`.
- **Grid coarsening (investigated, NOT shipped):** g1 = landuse at 2048; g2 = landuse at 1024, water and earth at
  2048.
  - Re-encoded from the new build in four shot areas (`bm/grid/*`, `bm/requant.py`).
  - Shots compared live, new, g1 and g2 side by side at z10/12/14/16, light and dark, at Vancouver, Squamish
    backcountry, Chilcotin and Lac Le Jeune (`bm/shots/*.png`, scratch page `bm/web/`).
  - Pixel difference against the new build (pixels off by >24/255): **g1 ≤0.06%, g2 ≤0.19%** of a 420 px map. The
    g2 differences are one-pixel shifts along river-polygon and coastline edges. Nothing visible by eye.
  - Area-extract sizes against the new build: g1 −4.5 to −7%, g2 −10 to −16%. Province bytes are below.
  - In the shots, "live" differs from the others in ways that are not grid effects. Those differences were already
    shipped or decided (T85's filter, this change):
    - the scratch page draws OSM parks, which the app hides;
    - the live file still has stream lines and oneway arrows.
- **Province bytes for the grid variants:** the exact province re-encode was queued behind other lanes' heavy jobs and
  was cancelled. The estimate comes from the sampled z≤13 re-encode above, which still included wetland:
  - g1 ≈ −20 MB (≈ −6%, giving ≈ 300 MB);
  - g2 ≈ −50 MB (≈ −15%, giving ≈ 270 MB).

  To get exact numbers, run `bm/requant.py` on the promoted file through `scripts/heavy.sh`.

  **Verdict:** g1 is invisible and g2 nearly so. Either would be shipped as a re-encode step after `_trim`. That is
  the owner's call.
