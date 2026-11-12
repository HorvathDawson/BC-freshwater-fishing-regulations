# The pipeline, as it is (map)

The one current map of `pipeline/` (T63). How the book is *read* is
[`RULINGS.md`](RULINGS.md); the rules for working here are the root [`AGENTS.md`](../../AGENTS.md); commands are in
[`pipeline/README.md`](../README.md). When this map and the code disagree, the code wins — fix the map.

## The flow

```
data/source/  (fetched: FWA gpkg, HYDAT, synopsis PDF, stations …)  ─┐
data/curated/ (authored + reviewed: cuts, names, areas, synopsis,    ├─▶ ATLAS ─▶ CLIM ─▶ REACH ─▶ TILES ─▶ DELIVER ─▶ data/release/r-*/
               gauge matches, steelhead list …)                      ─┘   (side      (gauge   (rules →   (PMTiles) (bundle → … →     + LATEST
                                                                           build,     envelope) sections)            release)
                                                                           promoted)
```

`python -m pipeline build` (`pipeline/build.py`) runs `clim → reach → tiles → deliver`, each stage skipped when its key
(a digest of everything it reads, its own code included) matches its last run in `data/generated/build-manifest.json`.
The atlas is built only on request (`--atlas`, into `<build>_next`) and adopted by `--promote`; a promoted atlas is
immutable (AGENTS 56). The run ends with the strict vintage check (`pipeline/common/vintage.py`): tiles and bundle from
one atlas and one registry. Heavy: one at a time through `scripts/heavy.sh`.

`python -m pipeline release --staging` (`pipeline/release.py`, T408) wraps it: preflight (committed code + curated) →
a copy of the live release's reach run → `pipeline build --hold-latest` via heavy.sh (the release waits in
`data/release/CANDIDATE`) → checks (manifest sanity, `deliver/release/app_gate.py`, `atlas/reach/binding_diff.py`
against the live release) → follows (T666, `pipeline/release_follows.py`: the in-season matches, the app's fixtures
re-cut and the tests that read the release, all on the candidate) → LATEST → `deploy/staging/upload.sh`. Preflight
lists every failure at once; `--dry-run` rehearses every stage's checks. Its ledger is `data/generated/release/ledger.json`;
`--prod <r-id>` (human-only) re-uploads that same release through `deliver/release/publish.py`.

## Stages and where they live

| stage | code | reads | writes (`GENERATED.*`, `pipeline/common/curated.py`) |
|---|---|---|---|
| **atlas** | `atlas/build.py`; `atlas/graph/` (network, prune, tributaries, windows), `atlas/splits/` (sectionizer, anchors, area cuts, border, sliver gate, the coast cut `splits/coast.py`: T678/T685 one rule (RULINGS E21) — a creek entering a tidal river is cut at the river's edge (where it leaves the FWA river polygon its mouth sits in), a water reaching the sea is fresh to the shoreline, the DFO polygon never cuts; `shore_cuts.json`, `tidal_sections.json` (the one tidal record), `coast_owner.json`), `atlas/registry/` (named waters, flowing-polygon fold, regions), `atlas/waters/` (added lakes/streams), `atlas/sidecars.py`, `atlas/promote.py`, `atlas/inputs.py` (T547: the digest of every curated input, in `graph.pkl.meta.json`; `pipeline release` refuses an atlas behind one) | FWA gpkg, `CURATED.splits`, names, areas | `data/generated/atlas/<build>/`: graph, `registry.json`, `section_handles.txt`, sidecars |
| **clim** | `gauges/feed/climatology.py` | HYDAT | `clim.json` (the gauge envelope) |
| **reach** | `atlas/reach/` (`cli.py`, `build.py`, `classify.py`, `extent.py`, `licensing.py`, `steelhead.py`, `cache.py`) | atlas, the catalogue corpus (`data/curated/regulations/synopsis/`) | `data/generated/reaches/<run>/`: per-rule bindings, `report.json`, diagnostics |
| **tiles** | `deliver/tiles/` (`tippe.py`, `layers.py`, `names.py`, `basemap.py`) | atlas | `data/generated/tiles/`: `atlas.pmtiles` (z4–13, T85), `atlas.meta.json`, `layers/` + `layers/undrawn.json` (the sections left out on purpose, T64; the bundle's `bundle/undrawn.py` proves none was needed), never the basemap |
| **deliver** | `deliver/__main__.py` `all` | atlas, reach run, tiles, curated corpus, `clim.json` | `data/release/r-<id>/` + `LATEST` (or `CANDIDATE` with `--hold-latest`) |

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
   access, core, near, water, `regs-<region>` ×9, `regs-sid`, conditions, flow-area, salmon (T211: DFO salmon per chain
   and day, `deliver/salmon.py`; placed by the reach run's `dfo_salmon.jsonl.gz`, `atlas/reach/dfo_salmon.py`) (gzip,
   sha-named, one manifest).
   Readers rebuild the store from those files (`deliver/store/release.py province_store`).

## Off the build (run on purpose, never by a stage)

| what | code | note |
|---|---|---|
| synopsis rows | `regs/extraction/extract_synopsis.py` | PDF → rows (only when the synopsis changes) |
| extractor change gate | `regs/extraction/baseline.py`, `baseline/<book>.json` + `accepted_diffs.json` | `extract_synopsis --check-against`: the current book re-read row by row (T426) |
| a new synopsis | `tools/synopsis_book.py` (carry / replay), `tools/synopsis_diff.py` | the annual update: carry unchanged rows, diff two books; runbook `handoff/PLAN/runbooks/annual-synopsis.md` (T239) |
| **the LLM parse** | `regs/parsing/` (`run_parse.sh`, `dispatch`) | **HUMAN-ONLY, spends credits** (AGENTS 1); writes the catalogue |
| catalogue model + gate | `regs/parsing/catalogue.py`, `validate_catalogue.py` | what ingest refuses |
| curation review | `regs/review/` + `curation-review/` (FastAPI + frontend) | marks only in review mode (AGENTS 65–67) |
| steelhead list | `regs/steelhead/known_waters.py`, `regs/steelhead/accept.py` | generates the list; `accept` prints added / dropped and `--write`s `data/curated/waters/steelhead_waters.json` (AGENTS 54, T324) |
| DFO salmon | `regs/dfo_salmon/` | see [`19-dfo-salmon-pipeline.md`](19-dfo-salmon-pipeline.md) |
| gauge matches | `gauges/generate/` (`match.py`, `promote.py`) | REVIEWED data: promote station by station (AGENTS 38) |
| live gauge feed | `gauges/feed/publish.py` (+ `forecast.py`, `change.py`, `notify.py`) | the feed the app polls; `scripts/dev-build.sh`, `scripts/update-hydat.sh` |
| generated app code | `tools/emit/` (`calendar`, `gauge_policy`, `pack`) + `deliver/answers/model --check` | `--check` in `pnpm check` and CI |
| audits | `tools/audit/` (`build_parity.py`, `check_curated.py` — `--waters` is the fast hand-edit check, the build's first step, …) | read-only reports |
| basemap | `deliver/tiles/basemap.py` (+ `basemap_roads.py`) | the OSM ground: clip, filter to what the style draws (T85, T202), cap at z13 with the z13 roads rebuilt from z14 so trail/FSR names survive (T202; zoom audit gates the build), release as `data/release/basemap/<id>/` + `tiles/basemap.meta.json` (T75); the release names it |
| split binding | `tools/audit/audit_split_binding.py` | which curated splits a rule can bind to, and which no item carries (T67) |
| tile sizes | `tools/audit/tile_sizes.py` | bytes per zoom and layer of a pmtiles archive (the S2 size table) |
| PREPARED, NOT FED | `stocking/`, `bathymetry/`, `runtiming/` | kept on purpose (owner 2026-10-22); each README says what feeds it (the two `generate/audit.py` were deleted in T436, 971f40575: restore from git). T634 first pass: `bathymetry/` holds the chart -> lake proposer (`propose`), the owner-only `accept`, and the 2001 vector-file fetch / `.e00` reader / placing fit (`fetch_vectors`, `e00`, `place`; files kept outside git); second pass: `compress` (chart PDFs -> 200 dpi 1-bit, outside git) |

## Shared ground

- `common/curated.py` — `CURATED` / `GENERATED` / `SOURCE`: the only way to name a path (AGENTS 36, 41).
- `common/` — names key (`names.py`), tolerances, section handles, entry ids, synopsis page map, vintage, not-fishing
  water (`fishing_water.py`, T152: sewage basins / tailing ponds / dumps, stamped by the atlas, out of every binding).
- Data kinds: authored / reviewed / generated (AGENTS 35). Never regenerate a reviewed artifact to check it.

## Tests

`pipeline/tests/` (pytest; `pytest.ini` deselects `slow`). Tiers: Tier 0 the file you touched; Tier 1
`scripts/test-smoke.sh` (`pipeline/tests/smoke.txt`); Tier 2 `scripts/test-full.sh` through `scripts/heavy.sh`. A test
reading generated data carries a `needs_*` marker and fails when the data is missing (AGENTS 62).
