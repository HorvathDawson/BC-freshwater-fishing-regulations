# T173 — A fish obstacles layer (falls, dams, culverts) from the BC Data Catalogue

Status: DONE — c27db165 (data + tile layer), 88b60317 (app), f4923e1f (icons), PSCIS (section "PSCIS"). The marks
reach readers with the next tile build (release lane, commands below); until then the switch draws nothing (the shipped
atlas has no `obstacle` layer). Q165 answered 2026-10-28: owner treats PSCIS as OGL-BC; its culverts are in.

Source: owner 2026-10-23 ("a fish obstacle layer could be interesting from the bc data catalogue"); started 2026-10-28.

## Work
1. Fetch + licence note + a fetch script (data/fetch_data.py style); dedupe the sources.
2. A tile layer within the size budgets; measure it.
3. App: a toggle in the layers menu (off by default); marks small and distinct from gauges and spots; tap sheet with
   type, name, height, source, date.

## As built
- **Data (licences checked on the catalogue records 2026-10-28, `package_show`).**
  - **FISS** — Provincial Obstacles to Fish Passage, `WHSE_FISH.FISS_OBSTACLES_PNT_SP` (35bbac7c-2e2f-4587-9108-f4aa1e862809),
    **Open Government Licence – British Columbia**: 24,832 points.
  - **WRIS** — BC Dams, `WHSE_WATER_MANAGEMENT.WRIS_DAMS_PUBLIC_SVW` (bd632217-35f9-4d01-8e57-a6dbc454f236), **OGL-BC**:
    2,490 crest lines (the T154 layer).
  - **PSCIS** (added later, see "PSCIS"): `WHSE_FISH.PSCIS_ASSESSMENT_SVW` (7ecfafa6-5e18-48cd-8d9b-eae5b5ea2881). The
    catalogue reads "Access Only"; the owner treats it as OGL-BC (Q165, decisions.md).
  - Fetch: `data/fetch_data.py` type `WFS_FILE` (the paginated WFS fetcher into a gpkg of its own), entries
    `fish_obstacles_fiss` / `fish_obstacles_dams` → `data/source/fish_obstacles/fish_obstacles.gpkg` (layers `fiss`, `dams`;
    EPSG:3005; 11 MB; gitignored). Never the main gpkg.
- **What is kept, and the dedupe** (`pipeline/deliver/tiles/obstacles.py`):
  - FISS by OBSTACLE NAME (the code `PD` is both "Private Dam" and "Persistent debris"): falls, cascade (+ chute),
    canyon, velocity barrier, culvert, and every built dam (hydro, irrigation, storage, private, regional, fisheries,
    weir, unknown origin). Dropped (10,312): beaver dams, log jams / LWD, rocks, debris, landslides, bridges, pumps,
    wedges, "other" — transient or not findable again; most surveys are from the 1990s.
  - WRIS: standing dams (Active, Deactivated) on their water (Main, Conversion, Outlet), at the crest's midpoint.
    Dropped: saddle dams, spillways, and dams not built / removed / breached / decommissioned / applied for.
  - Same-type FISS points within 10 m are one obstacle (3,422 pairs sit within 0.5 m — the same record surveyed twice);
    the kept one has a height, then the latest survey, then the lowest id. A FISS dam within 200 m of a kept WRIS crest
    IS that dam (FISS points were placed on 1:50k maps); the WRIS record wins (named, with its height).
  - Heights of 0, below 0 or over 500 m are survey placeholders (-1000, 9999) and are dropped. No dam owner: WRIS names
    private people as owners.
  - **15,185 marks**: falls 5,805 · cascade 3,911 · culvert 2,103 · canyon 555 · velocity 249 · FISS dam 642 · WRIS dam
    1,920. With a height 7,898; a name (dams) 1,920; the water's name (FISS) 7,553.
- **Slim schema** (tile layer `obstacle`, point, z10–13, `layers.FEATURES`): `obstacle_id` (`fiss:<point id>` /
  `wris:<dam file number>`), `type` (falls · cascade · canyon · velocity · culvert · dam), `name` (a dam's), `water`
  (the stream a FISS obstacle is on), `height_m`, `source` (fiss · wris), `year` (surveyed / commissioned). Names go
  through `tiles.names.display` (WRIS names arrive in capitals). `tile-contract.json` regenerated; the tiles export writes
  the layer; `python -m pipeline.deliver.tiles --obstacles-only` writes just `layers/obstacle.geojsonl`; the tiles stage
  key stamps the source file (`build.obstacles_source`; missing → the stage runs and names the fetch).
- **App.** `@app/core` `overlay.obstacles` ("Fish obstacles · Falls, cascades, culverts and dams. From zoom 10"), Map tab
  only, off by default, sets the toggleable style group `obstacles`. Style layers `obstacle_ring` + `obstacle` (circle, z10):
  an ink dot (alias of the gauge-dot ink: near-black on light, near-white on dark) in a paper ring (alias of
  `color.outside`) — no hue, so a mark never reads as a status or a reading; a gauge is a coloured dot (Conditions map
  only) and a spot a pin. Radius ~2.2 px at z10 → 3.2 at z13 → 4.4 at z16. A tap: `MapRenderer.pressObstacle` (same 24 px box,
  both renderers) is asked BEFORE the water — a tap on the mark raises `ObstacleSheet` (`@app/core` `obstacleSheet`:
  title = the dam's name or the type; "On <water>"; Type / Height / Surveyed or Commissioned / Source + record id; a note
  that surveys are old and passage depends on fish and flow; the OGL-BC attribution line) and opens no water under it.
  A hidden layer answers no query, so with the switch off taps are unchanged.
- **Tests.** pytest `test_tile_obstacles.py` (types by name, placeholders, years, dedupe both ways, spec + the app's type
  list held equal to the pipeline's), `test_tiles.py` inventory + stage key, `test_tile_feature_ids.py`; vitest
  `obstacles.test.ts` (whole sheets), `ObstacleSheet.test.tsx`, `parity.test.ts` (same query, same answer on both
  renderers; null when nothing is there), layers / LayersSheet / ConditionsLayers / back-layers pins updated.
  Ran (worktree = HEAD + this change): tsc -b, boundaries, platform, colours, 125 vitest files (1,534 passed); pytest tile
  suites 55 passed. `pnpm check`'s knip / style passed in the main checkout.

## Size (measured 2026-10-28, against the live atlas db853e9556df81a6.16b3906b04bf, 168.1 MB)
The obstacle layer built alone with the archive's own top-band flags (`tippe._common`, z10–13): 21,153 tiles, 4.04 MB gz
standalone (4.80 MB raw MVT). What it ADDS to the atlas: each obstacle tile's MVT appended to the live atlas tile at the
same z/x/y and gzipped, minus the atlas tile gzipped the same way (scratch `measure.py`):

| zoom | tiles with a mark | added (gz) |
|---|---:|---:|
| z10 | 1,346 | +446 kB |
| z11 | 3,470 | +625 kB |
| z12 | 6,655 | +859 kB |
| z13 | 9,682 | +1,106 kB |
| **total** | 21,153 | **+3.04 MB (+1.8 % of the atlas)** |

Confirmed by the real join: `tile-join` of the live atlas + the obstacle layer = 179,346,272 B against 176,303,284 B — **+3,042,988 B (+1.73 %)**.

Budgets: the release gates (`release.build.SIZE_GATES`, `OFFLINE_GATE` 20 MB, `WEB_BUDGET`) price the release files, not
the tiles — the layer touches none of them. No tile-size gate exists; the phone holds ~450 MB of tiles + basemap
(T182's note), so +3.0 MB is +0.7 % of the offline package. Web: a z10–13 tile with marks grows ~0.1–0.3 kB gz, only
where marks are. **Ship decision: in the atlas, offline included, z10–13** (no split needed). If it ever must shrink:
minzoom 11 saves 0.45 MB; dropping `obstacle_id` (unique strings) saves a further share.

## Release lane — fold it into the next build
```
# 1. the source (once per refresh; already fetched on this machine 2026-10-28)
PYTHONPATH="$PWD" .venv/bin/python data/fetch_data.py --layers fish_obstacles_fiss fish_obstacles_dams fish_obstacles_pscis
# 2a. a full tile build exports it with everything else (heavy):
scripts/heavy.sh tiles 30 -- .venv/bin/python -m pipeline.deliver.tiles --build data/generated/atlas/full
# 2b. or, on a tile build whose layer files are current: add just this layer, then re-tile
.venv/bin/python -m pipeline.deliver.tiles --obstacles-only            # writes data/generated/tiles/layers/obstacle.geojsonl
scripts/heavy.sh tiles 30 -- .venv/bin/python -m pipeline.deliver.tiles --tiles-only --build data/generated/atlas/full
# 3. promote as usual: pmtiles + meta + layers/*.geojsonl file by file (layers/obstacle.geojsonl is new)
```
No bundle or release-file change. The app commit is safe before the tiles: the switch draws nothing on an atlas without
the layer.

## Icons (owner 2026-10-28: "small, nice icons per type") — style/app only, no tile change
- The mark is no longer a dot in a ring: one icon per `type` (falls, cascade, canyon, velocity barrier, dam, culvert; any other or
  missing type is the dot), neutral ink (`color.obstacle.mark`) in a paper halo (`color.obstacle.ring`), 14 px at z10 → 16 at z13 →
  18 at z16 (`icon-size` ramp). Style layer `obstacle_ring` and the `width.obstacle.*` tokens are gone; `obstacle` is a symbol layer
  (`layers.source.json` `icon`, emitted by `tools/build-style.mjs`: `icon-image` match on `type`, `icon-allow-overlap` off,
  `symbol-sort-key` ranks dam < falls < canyon < cascade < velocity < culvert < dot so a crowded reach drops the lesser mark, padding 3),
  still z10+ and tapped by the same `pressObstacle`.
- ONE drawing table: `@app/core` `obstacleIcons.ts` (`OBSTACLE_ICON_SHAPES`, a 16 grid: strokes, polygons, an ellipse, a disc).
  The map rasterises it per theme (`@app/map` `obstacle-icons.ts`, 3x, signed-distance AA) into `generatedImages` — `addImage` on the web,
  the binding's `Images` on a phone under per-theme names (the same rail as the hatches / pill / pin, so native needed no new code).
  The UI draws the same table with react-native-svg (`ObstacleIcon` in `icons.tsx`): the tap sheet's header (`Sheet` takes an `icon`),
  and a key (icon + word per type, the dot last) inside the Layers sheet's Fish obstacles row (`obstacle-key`). Colours are the two tokens in every
  theme (light, dark, colour-blind: no new colour family; dark is near-white ink on a dark halo).
- Tests: `obstacleIcons.test.ts` (kinds = tile types + dot, points inside the grid, SVG), `obstacle-icons.test.ts` (pixels, per-theme
  images on both renderers, the style names only images that exist, no ring layer), `ObstacleSheet` header icon, `LayersSheet` key.
- Shots `T173-shots/icons-*`: `desktop|phone-light|dark-z11|z14` (Saanich / Hagan Creek, Victoria area, the densest mixed cluster of
  marks — a scratch GeoJSON of the 15,185 marks stood in for the not-yet-rebuilt atlas layer, same layer spec and images), plus
  `icons-phone-tap-sheet-dark.png` and `icons-desktop-light|phone-dark-layers-sheet.png`. At z14 the 7 marks of one reach collapse to dams + a
  culvert (falls / cascade drop until zoom 15+): the dam ranks first, by design; say if falls should outrank dams.

## Shots
`handoff/PLAN/app-integration/T173-shots/` (Rosedale / Popkum, r-c331a7a3c2cf on the joined scratch atlas, served by a
private tile server and swapped in by a browser route; nothing promoted): `desktop-layers-sheet.png` (the switch on),
`desktop-layer-on.png`, `desktop-tap-sheet.png` (Falls, 35 m, FISS 23953), `phone-layer-on.png`, `phone-tap-sheet.png`
(Culvert on Nevin Creek, FISS 19492). The marks sit on the status lines as ink dots in a paper ring.

## PSCIS stream crossings (owner 2026-10-28, Q165: treat as OGL-BC)
- **Fetch:** `fish_obstacles_pscis` (WFS_FILE, `WHSE_FISH.PSCIS_ASSESSMENT_SVW`) → layer `pscis` of the same gpkg:
  19,905 crossings, one row per `STREAM_CROSSING_ID`, each with its CURRENT result (a remediated crossing reads passable).
  By current type × result: closed-bottom (culverts) 13,066 = barrier 10,330 · potential 1,667 · unknown 195 · passable
  874; open-bottom (bridges) 5,391, all passable; other (fords) 1,448, unknown 1,438 / passable 10.
- **Dedupe (`obstacles.merge_pscis`):** a FISS culvert within 50 m of ANY PSCIS crossing is that crossing — PSCIS wins,
  the FISS record goes even when the PSCIS one is not drawn (rated passable, or a bridge now). 110 of the 2,103 FISS
  culverts go; 50 m, not FISS's 200 m dam radius, because 3,480 PSCIS crossings have another within 100 m (one road
  over a run of tributaries). Where PSCIS has no stream name ("Unnamed", "n/a", "unknown" are dropped as placeholders),
  the FISS record's gazetted name fills it.
- **What is drawn — options measured** (each layer tiled alone with the archive's top-band flags, z10–13, each obstacle
  tile appended to the live atlas db853e9556df81a6.16b3906b04bf tile and gzipped; T173's `measure.py`):

  | option | PSCIS marks | layer marks | added to the 168 MB atlas |
  |---|---:|---:|---:|
  | FISS + WRIS only (as first shipped) | 0 | 15,185 | +3.04 MB |
  | A: culverts, barrier + potential | 11,997 | 27,072 | +5.64 MB |
  | **B: A + culverts not yet rated (chosen)** | **12,192** | **27,267** | **+5.68 MB** (z10 +0.81, z11 +1.10, z12 +1.58, z13 +2.19) |
  | C: B + fords (unknown) | 13,630 | 28,705 | +5.89 MB |
  | D: every culvert, passable too | 13,066 | 28,141 | +5.83 MB |
  | E: every crossing (bridges + fords) | 19,905 | 34,980 | +6.76 MB |

  **Chosen: B** — barriers, potential barriers and the 195 culverts PSCIS has not rated (+41 kB; unknown is not
  passable). Passable culverts and bridges are noise on an obstacles map; fords are not culverts and have no rating.
  The layer goes from ~3.0 to ~5.7 MB (+3.4 % of the atlas, +1.3 % of the ~450 MB offline package); no tile-size gate
  exists. If it must shrink: minzoom 11 on PSCIS saves ~0.8 MB; dropping `road` (free text) saves a share.
- **Tile record** (layer attrs appended, contract regenerated): `obstacle_id` `pscis:<crossing id>`, `type` culvert,
  `water` (stream name), `source` pscis, `year` assessed, and PSCIS only: `barrier` (barrier · potential · unknown),
  `habitat` (high · medium · low — the assessor's upstream habitat value), `road`, `date` (assessed, ISO).
  Of the 12,192: a stream name 10,311, a road 11,404, a habitat value 11,193. FISS culverts left: 1,993.
- **App:** culvert icon (no new icon; `type` is culvert). Tap sheet (`obstacleSheet`): title "Culvert", "On <stream>",
  rows Fish passage (Barrier / Potential barrier / Passable / Not yet rated) · Habitat upstream (High/Medium/Low value) ·
  Road · Assessed ("Aug 17, 2008") · Source "Provincial Stream Crossing Inventory · PSCIS <id>"; the OGL-BC line as before.
- **Tests:** pytest `test_tile_obstacles.py` (shown = unpassable culverts only; FISS-at-a-crossing replaced / kept beyond
  50 m / name fill; the view's row → record; attrs + the app's barrier words equal the pipeline's), `test_tiles.py`
  inventory; vitest `obstacles.test.ts` (whole PSCIS sheets, every barrier word, dates).
- **Release lane:** the gpkg on this machine already has `pscis` (fetched 2026-10-28); the tiles stage key stamps the
  gpkg, so the next tile build re-exports. A gpkg without the `pscis` layer stops the export naming the fetch command.

## Left / follow-ups
- T162 / T164 (run timing, "upstream of the falls" locators) can read `obstacles.build()` — not wired here.
