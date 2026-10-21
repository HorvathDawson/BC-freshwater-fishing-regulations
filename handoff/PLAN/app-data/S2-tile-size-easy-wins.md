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
