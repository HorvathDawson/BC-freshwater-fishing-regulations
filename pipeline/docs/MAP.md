# The pipeline, as it is (map)

The one current map of `pipeline/` (T63, checked against the code 2026-10-24). How the book is *read* is
[`RULINGS.md`](RULINGS.md); the rules for working here are the root [`AGENTS.md`](../../AGENTS.md); commands are in
[`pipeline/README.md`](../README.md). When this map and the code disagree, the code wins — fix the map.

## The flow

```
data/source/  (fetched: FWA gpkg, HYDAT, synopsis PDF, stations …)  ─┐
data/curated/ (authored + reviewed: splits, names, areas, catalogue,  ├─▶ ATLAS ─▶ CLIM ─▶ REACH ─▶ TILES ─▶ DELIVER ─▶ data/release/r-*/
               gauge matches, steelhead list …)                      ─┘   (side      (gauge   (rules →   (PMTiles) (bundle → … →     + LATEST
                                                                           build,     envelope) sections)            release)
                                                                           promoted)
```

`python -m pipeline build` (`pipeline/build.py`) runs `clim → reach → tiles → deliver`, each stage skipped when its key
(a digest of everything it reads, its own code included) matches its last run in `data/generated/build-manifest.json`.
The atlas is built only on request (`--atlas`, into `<build>_next`) and adopted by `--promote`; a promoted atlas is
immutable (AGENTS 56). The run ends with the strict vintage check (`pipeline/common/vintage.py`): tiles and bundle from
one atlas and one registry. Heavy: one at a time through `scripts/heavy.sh`.

## Stages and where they live

| stage | code | reads | writes (`GENERATED.*`, `pipeline/common/curated.py`) |
|---|---|---|---|
| **atlas** | `atlas/build.py`; `atlas/graph/` (network, prune, tributaries, windows), `atlas/splits/` (sectionizer, anchors, area cuts, border, sliver gate), `atlas/registry/` (named waters, flowing-polygon fold, regions), `atlas/waters/` (added lakes/streams), `atlas/sidecars.py`, `atlas/promote.py` | FWA gpkg, `CURATED.splits`, names, areas | `data/generated/atlas/<build>/`: graph, `registry.json`, `section_handles.txt`, sidecars |
| **clim** | `gauges/feed/climatology.py` | HYDAT | `clim.json` (the gauge envelope) |
| **reach** | `atlas/reach/` (`cli.py`, `build.py`, `classify.py`, `extent.py`, `licensing.py`, `steelhead.py`, `cache.py`) | atlas, the catalogue corpus (`data/curated/regulations/entries/catalogue/`) | `data/generated/reaches/<run>/`: per-rule bindings, `report.json`, diagnostics |
| **tiles** | `deliver/tiles/` (`tippe.py`, `layers.py`, `names.py`, `basemap.py`) | atlas | `data/generated/tiles/`: `atlas.pmtiles` (z4–13, T85), `atlas.meta.json`, `layers/` + `layers/undrawn.json` (the sections left out on purpose, T64; the bundle's `bundle/undrawn.py` proves none was needed), never the basemap |
| **deliver** | `deliver/__main__.py` `all` | atlas, reach run, tiles, curated corpus, `clim.json` | `data/release/r-<id>/` + `LATEST` |

**Deliver, step by step** (`python -m pipeline.deliver all`; each step its own process, each reads only the previous
step's output; nothing is promoted unless every step and the vintage check pass):

1. `bundle` — `deliver/bundle/` (`build.py`, `schema.sql`; the reference reader `bundle/read/ladder.py
   effective_rules`, the rule tables `bundle/rules/`) → `bundle.sqlite`. Gauge tables from `gauges/consume/`.
2. `verdicts` — `deliver/verdicts/` → `verdicts.sqlite`: the reader run once per rule key × reading × fish × origin
   (never shipped; everything after looks answers up here, AGENTS 56).
3. `status_index` — `deliver/status_index.py` → `status_index.bin`.
4. `export` — `deliver/export/` (`model.build`, `codec`, `checks`) → `ui-rules-export.json` + `ui-rules-guide.json`.
5. `answers` — `deliver/answers/` (`model/`, `rows/`, `display/`; spec `ANSWERS-SPEC.md`) → the answers file.
6. `card_check` — `deliver/answers/card_check.py`: every number on every card proved (problems must be 0).
7. `store` — `deliver/store/` → `regs.sqlite` in staging only (P20: never kept; `--debug-json` keeps a copy).
8. `release` — `deliver/release/` (`build.py`, `paint.py`, `shards.py`, `manifest.py`) → the shipped set: paint,
   access, core, near, water, `regs-<region>` ×9, `regs-sid`, conditions, flow-area (gzip, sha-named, one manifest).
   Readers rebuild the store from those files (`deliver/store/release.py province_store`).

## Off the build (run on purpose, never by a stage)

| what | code | note |
|---|---|---|
| synopsis rows | `regs/extraction/extract_synopsis.py` | PDF → rows (only when the synopsis changes) |
| **the LLM parse** | `regs/parsing/` (`run_parse.sh`, `dispatch`) | **HUMAN-ONLY, spends credits** (AGENTS 1); writes the catalogue |
| catalogue model + gate | `regs/parsing/catalogue.py`, `validate_catalogue.py` | what ingest refuses |
| curation review | `regs/review/` + `curation-review/` (FastAPI + frontend) | marks only in review mode (AGENTS 65–67) |
| steelhead list | `regs/steelhead/known_waters.py` | generates a list a human copies to curated (AGENTS 54) |
| DFO salmon | `regs/dfo_salmon/` | see [`19-dfo-salmon-pipeline.md`](19-dfo-salmon-pipeline.md) |
| gauge matches | `gauges/generate/` (`match.py`, `promote.py`) | REVIEWED data: promote station by station (AGENTS 38) |
| live gauge feed | `gauges/feed/publish.py` (+ `forecast.py`, `change.py`, `notify.py`) | the feed the app polls; `scripts/dev-build.sh`, `scripts/update-hydat.sh` |
| generated app code | `tools/emit/` (`calendar`, `gauge_policy`, `pack`) + `deliver/answers/model --check` | `--check` in `pnpm check` and CI |
| audits | `tools/audit/` (`build_parity.py`, `check_curated.py`, …) | read-only reports |
| basemap | `deliver/tiles/basemap.py` | the OSM ground: clip, filter to what the style draws (T85), release as `data/release/basemap/<id>/` + `tiles/basemap.meta.json` (T75); the release names it |
| tile sizes | `tools/audit/tile_sizes.py` | bytes per zoom and layer of a pmtiles archive (the S2 size table) |
| PREPARED, NOT FED | `stocking/`, `bathymetry/`, `runtiming/` | kept on purpose (owner 2026-10-22); each README says what feeds it |

## Shared ground

- `common/curated.py` — `CURATED` / `GENERATED` / `SOURCE`: the only way to name a path (AGENTS 36, 41).
- `common/` — names key (`names.py`), tolerances, section handles, entry ids, synopsis page map, vintage, not-fishing
  water (`fishing_water.py`, T152: sewage basins / tailing ponds / dumps, stamped by the atlas, out of every binding).
- Data kinds: authored / reviewed / generated (AGENTS 35). Never regenerate a reviewed artifact to check it.

## Tests

`pipeline/tests/` (pytest; `pytest.ini` deselects `slow`). Tiers: Tier 0 the file you touched; Tier 1
`scripts/test-smoke.sh` (`pipeline/tests/smoke.txt`); Tier 2 `scripts/test-full.sh` through `scripts/heavy.sh`. A test
reading generated data carries a `needs_*` marker and fails when the data is missing (AGENTS 62).
