# S2 — map download size: easy wins (atlas + basemap)

Owner, 2026-10-21: "optimize the tile size and the basemap size — look for easy wins basically."

## Today
- First-run download on a phone: atlas.pmtiles **546 MB** (z4–14, tile_version d768d6e135e62b99.c9f9604f12bf) +
  basemap.pmtiles **918 MB** (Protomaps) + data release ~14 MB. About 1.5 GB before the app works offline.
- Atlas layer inputs (data/generated/tiles/layers/*.geojsonl): stream 2.6 GB, wetland 751 MB, lake 407 MB, parcel 181 MB,
  under_lake 77 MB, mu 47 MB, park 32 MB, region 23 MB.

## Scope: easy wins only
Measure each candidate on its own (size before/after, and what it costs visually), keep the ones that save real megabytes
for little or no visible loss, and leave anything risky as a recorded option. Never break the owner rules: tiles stay
geometry-only, one data release never needs a tile rebuild, the BC-outside mask stays a separate file, section ids stay
the MVT ids (P18).

## Candidates to measure
**Basemap (the bigger file)**
- Clip to BC plus a small margin: check whether the extract is BC-only or wider.
- Drop or limit layers the app never shows (e.g. landuse/landcover detail, buildings at low zoom, POIs, transit). The
  earlier memory note already lists a "basemap landuse filter" as deferred.
- Lower the basemap max zoom where the app's own atlas carries the detail (water), with overzoom (camera z18 already).

**Atlas**
- z13 overzoom for the atlas: drop z14 and let MapLibre overzoom (deferred in memory; measure the visual cost at z14–18).
- `parcel` (181 MB of input): is it shown, and at which zooms? Raise its minzoom or drop it if unused.
- `wetland` (751 MB): minzoom and simplification; most wetlands are context, not regulated water.
- Simplification and coordinate precision per zoom (tippecanoe -S, the z14 4 m simplification seen in overzoom), and
  attribute pruning (only what the contract declares; P18 kept section_id as a property — measure dropping it now that
  the MVT id carries it, as the P18 agent noted ~6.6 %).
- Tile compression settings (gzip level / brotli support in MapLibre on web and native) and the pmtiles clustering.

**Delivery**
- Split downloads so the app works sooner: a small "region first" package or low-zoom first, then the rest in the
  background (offline promise unchanged once complete).

## Acceptance
- A table of every candidate: MB saved (atlas / basemap), visual cost (screenshots at the affected zooms), risk.
- The easy wins applied (tile rebuild via heavy.sh; promote file by file; release names the new tile version), the
  rest recorded as options with numbers.
- Tile tests, release parity and placeability green; colours and taps unchanged at z4–18.

## Measured 2026-10-24 (tiles window: T64, T85) — live archives, bytes per layer per zoom
Read-only scan of the shipped archives (each unique tile once, its gzip bytes shared by its layers' raw share).
**atlas.pmtiles 548.3 MB** (z14 315.7, z13 131.7, z12 57.8, z11 26.4, ≤z10 15.6): stream 254.4, wetland 111.1,
lake 71.9, mu 40.0, under_lake 24.3, parcel 17.5, park 14.7, watershed 8.1, rest 5.0.
**basemap.pmtiles 918.4 MB** (z14 447.9, z13 259.3, z12 97.3, ≤z11 112.9): water 408.1 (of it OSM `stream` lines
241.8, polygons 108.5), landuse 375.1 (wood 221.3, wetland 52.0, glacier 26.7, forest 10.9, park 8.9, nature_reserve
5.7, residential 4.0 …), roads 78.1, earth 44.8, boundaries 7.4, places 3.1.

| candidate | ticket | saves (est.) | visual cost | status |
|---|---|---|---|---|
| unnamed wetlands in no item not drawn | T64 | atlas −111 MB (333,471 of 333,526 polygons; 99.9% of the layer) | marsh fill gone outside named/folded wetlands; the no-access land fill still shows; basemap `wetland` landuse kept as a stand-in (not drawn today) | BUILT, needs shots |
| atlas z13 max + overzoom (top zoom at −S 5 ≈ old z14's 4 m) | T85 | atlas about −140 MB (z14 315.7 goes; z13 takes most of its content: est. 0.8–0.9×) — measure after build | none expected at z13+ (same ground tolerance); dense z13 tiles in cities | BUILT, needs shots |
| basemap: OSM stream lines (drawn only z14+, under our FWA streams) | T85 | basemap −242 MB | OSM-only creeks (no FWA line, no regulation) vanish at z14+ | BUILT (CLAUDE call, reversible) |
| basemap: landuse kinds the style never draws (parks, residential, farmland, bare rock …) | T85 | basemap −26 MB | none (not drawn) | BUILT |
| basemap z13 max + overzoom | — | basemap about −289 MB more (z14 after the filters) | roads/coast/labels coarser past z13 | OPTION (owner) |
| drop `section_id` property (the MVT id carries it) | — | atlas ~6.6% (P18 note) | none, but the web style's `promoteId` and taps read the property: app work | OPTION |
| parcels from z11, basins one-coverage 100 m | T64 | +2 MB / ±0 | private land visible from z11; no basin seams | in code since 10-22; ships with this build |

Expected after the build: atlas about 295 MB (from 548), basemap about 650 MB (from 918): about −520 MB per native
install (D32 estimated −345).
