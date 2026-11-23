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
(T998) `python -m pipeline release --cut` computes the app version from the Conventional Commits since the last `v*` tag
(`pipeline/release_version.py`: only the app's commits bump it; preflight's `version` check holds app.json to it) and
makes the release branch with its version commit; `--cut --data-only` makes `release/data-<date>`.
`--prod <r-id>` (human-only) re-uploads that same release through `deploy/prod/upload.sh` (T404: it promotes what staging
serves). `deliver/release/publish.py` is the upload's PLAN only (`plan`, the order; T994 #1: it sends nothing, `--yes` is
refused), and `scripts/update-hydat.sh` uploads nothing either. After the production pointer moves,
`deploy/prod/upload.sh` prunes the bucket (T865, `deliver/release/prod_prune.py`): release-scoped objects neither pointer
names (the v1 layout only by `deploy/prod/drop-v1.sh`, at the switch).

## Stages and where they live

| stage | code | reads | writes (`GENERATED.*`, `pipeline/common/curated.py`) |
|---|---|---|---|
| **atlas** | `atlas/build.py`; `atlas/graph/` (network, prune, tributaries, windows), `atlas/splits/` (sectionizer, anchors (every cut it cannot place listed in `split_unplaced.json`, T743), area cuts, border, sliver gate, the coast cut `splits/coast.py`: T678/T685 one rule (RULINGS E21) — a creek entering a tidal river is cut at the river's edge (where it leaves the FWA river polygon its mouth sits in), a water reaching the sea is fresh to the shoreline, the DFO polygon never cuts; `shore_cuts.json`, `tidal_sections.json` (the one tidal record; T678 plan 5: each tidal node carries its owner as `StreamNode.tidal`, read by every reader through `coast.read_tidal`; rank 1 resolved in `splits/tidal.py`), `coast_owner.json`, `step10_report.json` (T679 #1/#6 measured)), `atlas/registry/` (named waters, flowing-polygon fold, lake loops `registry/lake_loops.py` (T753: a nameless line out of a lake and back into it is the lake's, unless that would divide the lake by a cut area; `lake_loop_report.json`), regions), `atlas/waters/` (added lakes/streams), `atlas/sidecars.py`, `atlas/promote.py`, `atlas/inputs.py` (T547: the digest of every curated input, in `graph.pkl.meta.json`; `pipeline release` refuses an atlas behind one) | FWA gpkg, `CURATED.splits`, names, areas | `data/generated/atlas/<build>/`: graph, `registry.json`, `section_handles.txt`, sidecars |
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
   and day, `deliver/salmon.py`; placed by the reach run's `dfo_salmon.jsonl.gz`, `atlas/reach/dfo_salmon.py`; T573: a place's closed areas are
   locations of their own, `regs/dfo_salmon/closures.build_pieces` / `resolve_closures`; T761: a "rest of" place —
   its bounds and listed places `regs/dfo_salmon/entries.rest_listing` / `answers_before`, bound by `atlas/reach/build._build_rest`,
   answering after its listed places on their sections `deliver/salmon.rests_behind` / `rest_after`; T807: a named-water
   place's side streams are DFO's words for it, `regs/dfo_salmon/side_streams.effective`, read by `entries.to_reach_input`) (gzip,
   sha-named, one manifest).
   Readers rebuild the store from those files (`deliver/store/release.py province_store`).

## Off the build (run on purpose, never by a stage)

| what | code | note |
|---|---|---|
| synopsis rows | `regs/extraction/extract_synopsis.py` | PDF → rows (only when the synopsis changes) |
| region / province highlight boxes | `regs/extraction/passage_boxes.py` | T1032: a zone, province or area rule's `book_box`, its words located in the release's PDF at answers build (`display.decided.book_boxes`; exact, once, on word bounds, else no box); `python -m pipeline.regs.extraction.passage_boxes` reports coverage per page and every rule not located |
| extractor change gate | `regs/extraction/baseline.py`, `baseline/<book>.json` + `accepted_diffs.json` | `extract_synopsis --check-against`: the current book re-read row by row (T426) |
| a new synopsis | `tools/synopsis_book.py` (carry / replay), `tools/synopsis_diff.py` | the annual update: carry unchanged rows, diff two books; runbook `handoff/PLAN/runbooks/annual-synopsis.md` (T239) |
| **the LLM parse** | `regs/parsing/` (`run_parse.sh`, `dispatch`) | **HUMAN-ONLY, spends credits** (AGENTS 1); writes the catalogue |
| catalogue model + gate | `regs/parsing/catalogue.py`, `validate_catalogue.py` | what ingest refuses |
| curation review | `regs/review/` + `curation-review/` (FastAPI + frontend) | marks only in review mode (AGENTS 65–67) |
| classified waters gate | `regs/classified_waters.py` (`BOOK`: the book's 73 classified rows with each designation's printed period, place, carve-outs, sleep; `NAMED`: the Li5/Li5a waters; `classified_pins.json`: each designation's measured stretch and walk) | T841: `--check` holds the corpus to the book (fast-fail, release preflight); `--bundle` holds the candidate: the Classified Waters Licence on every section a designation binds, `holds.considered` naming every designation (the card's Class badge), the named waters, the pins (release checks stage); `--write-pins BUNDLE` re-cuts the pins after a reviewed change |
| steelhead list | `regs/steelhead/known_waters.py`, `regs/steelhead/accept.py` | generates the list; `accept` prints added / dropped and `--write`s `data/curated/waters/steelhead_waters.json` (AGENTS 54, T324) |
| DFO salmon | `regs/dfo_salmon/` | see [`19-dfo-salmon-pipeline.md`](19-dfo-salmon-pipeline.md) |
| gauge matches | `gauges/generate/` (`match.py`, `promote.py`) | REVIEWED data: promote station by station (AGENTS 38) |
| live gauge feed | `gauges/feed/publish.py` (+ `forecast.py`, `change.py`, `notify.py`) | the feed the app polls; `scripts/dev-build.sh`, `scripts/update-hydat.sh` |
| generated app code | `tools/emit/` (`calendar`, `gauge_policy`, `pack`) + `deliver/answers/model --check` | `--check` in `pnpm check` and CI |
| the mini release | `tools/mini_release.py` (+ `fixtures/mini/`, `deliver/export/claims.py`) | T667 slice 8: the real chain atlas → release on an invented world, no live data; `--check` (CI) / `--write` keep `app/packages/data/dev/mini-release/`; the export held to the mini corpus's own claims (`--claims`) |
| water rules map | `tools/water_map/` (`python -m pipeline.tools.water_map "<water>" [--mode salmon\|freshwater\|both] [--date] [--release] [--focus KM-KM]`) | T853: the Nass-style labelled PNG of any water's system from a release on disk (answers via `province_store` + `encode.tap`, salmon via `decode_files`, lines from the tile layers); read-only, writes PNGs to `--out` |
| audits | `tools/audit/` (`build_parity.py`, `check_curated.py` — `--waters` is the fast hand-edit check, the build's first step, …) | read-only reports |
| basemap | `deliver/tiles/basemap.py` (+ `basemap_roads.py`) | the OSM ground: clip, filter to what the style draws (T85, T202), cap at z13 with the z13 roads rebuilt from z14 so trail/FSR names survive (T202; zoom audit gates the build), release as `data/release/basemap/<id>/` + `tiles/basemap.meta.json` (T75); the release names it |
| near B.C. | `deliver/tiles/near_bc.py` | by hand, when `data/source/bc_boundary.geojson` changes: B.C. grown by 50 km, simplified to 5 km, written as `app/packages/core/src/nearBcOutline.ts` (70 points) for the app's "far from B.C." location check (T864, `nearBc.ts`) |
| split binding | `tools/audit/audit_split_binding.py` | which curated splits a rule can bind to, and which no item carries (T67) |
| tile sizes | `tools/audit/tile_sizes.py` | bytes per zoom and layer of a pmtiles archive (the S2 size table) |
| PREPARED, NOT FED | `stocking/`, `bathymetry/`, `runtiming/` | kept on purpose (owner 2026-10-22); each README says what feeds it (their two generate/audit.py were deleted in T436, 971f40575: restore from git). T634 first pass: `bathymetry/` holds the chart -> lake proposer (`propose`), the owner-only `accept`, and the 2001 vector-file fetch / `.e00` reader / placing fit (`fetch_vectors`, `e00`, `place`; files kept outside git); second pass: `compress` (chart PDFs -> 200 dpi 1-bit, outside git) |

## Shared ground

- `common/curated.py` — `CURATED` / `GENERATED` / `SOURCE`: the only way to name a path (AGENTS 36, 41).
- `common/` — names key (`names.py`), tolerances, section handles, entry ids, synopsis page map, vintage, not-fishing
  water (`fishing_water.py`, T152: sewage basins / tailing ponds / dumps, stamped by the atlas, out of every binding).
- Data kinds: authored / reviewed / generated (AGENTS 35). Never regenerate a reviewed artifact to check it.

## Tests

`pipeline/tests/` (pytest; `pytest.ini` deselects `slow`). Tiers: Tier 0 the file you touched; Tier 1
`scripts/test-smoke.sh` (the smoke set by rule, `pipeline/tests/smoke.py`); Tier 2 `scripts/test-full.sh` through `scripts/heavy.sh`. A test
reading generated data carries a `needs_*` marker and fails when the data is missing (AGENTS 62).
