# T599 findings — one structure for the pipeline's data (Zarr or other)

Study only, 2026-11-10. Nothing was moved or built. Sizes are `du` on this Mac today.

## Summary

- **Recommendation: do not switch formats.** Keep JSON for curated data, SQLite / PMTiles / packed binaries for what ships.
  Instead: make the four kinds of data (fetched, curated, built, released) strict, with one writer for curated data.
- **Why:** our data is tables, a graph, geometry, tiles and small JSON; Zarr is for big N-d arrays and ours are tiny
  (clim.json 8.5 MB). None of the real pains came from formats: they came from stale pairs, unchecked inputs,
  hand-made copies of one reader/writer, and programs rewriting curated files.
- **Most of the "manifest" idea already exists:** `build-manifest.json` stage keys, a content-addressed release with
  sha256 + format per file, `pins.json` for big inputs, `CURATED`/`GENERATED` paths. The gaps are specific (below).
- **First step (about 1 day):** a curated guard. (a) the atlas writes a digest of every curated file it read (T547);
  `pipeline release` refuses an atlas older than its curated inputs. (b) a test runs each build stage against a
  read-only copy of `data/curated/` and fails if any byte changed.
- **What it costs:** no format migration and no app change. Steps 2–4 are small tickets, mostly ones already open
  (T547, T555, T557, T560). Step 5 (Parquet for 2–3 big intermediates) is optional, only if a measured stage is slow.

## 1. Inventory

Class: **F** fetched/raw · **C** curated (human time inside) · **B** built/intermediate · **R** release (app/worker read).
"Git" = tracked in the repo.

| path | format | size | writer | main readers | rewritten | class | git |
|---|---|---|---|---|---|---|---|
| `source/bc_fisheries_data.gpkg` | GeoPackage | 9.5 GB | `data/fetch_data.py` | atlas build | by hand, rarely | F | no (pinned in `pins.json`) |
| `source/bc.pmtiles`, `basemap.pmtiles` | PMTiles | 4.1 GB + 0.2 GB | fetch; basemap is BUILT by `deliver.tiles.basemap` | tiles, release | rarely | F (basemap is really B) | no |
| `source/hydat.sqlite3` + `hydat.20260717.sqlite3` | SQLite (ECCC) | 1.2 GB each | `update-hydat.sh` | clim stage, hydat_daily | each HYDAT | F | no |
| `source/runtiming/timing.json` | JSON | small | `pipeline.runtiming.parse` (derived) | runtiming | per fetch | B stored as F | yes |
| `source/bathymetry_pdfs`, `fish_obstacles`, `dfo_tidal_areas`, … | PDF/JSON/HTML | ~0.8 GB | fetchers/scrapes | various | rarely | F | partly |
| `curated/regulations/synopsis/region-*.json` | JSON | 2.4 MB | parser (human-run) + review write-through + 6 tools | reach, bundle | per review save | C (two writers) | yes |
| `curated/waters/*` (cuts, names, areas, tidal, added_*) | JSON/GeoJSON | 8.7 MB | humans, review write-through, `mined_names`, `tidal_limits` | atlas build | weekly | C | yes |
| `curated/gauges/matches.json` (+ calls, stations) | JSON | 1.3 MB | `gauges.generate.promote` only | atlas, bundle | rarely | C (promoted) | yes |
| `curated/dfo_salmon/places/region-*.json` | JSON | 0.65 MB | review write-through, `dfo_salmon.live queue` | reach, feeds worker (synced) | 30 commits to region-2 in a month | C | yes |
| `curated/review/marks.jsonl` | JSONL, append-only | 1.8 MB | review app | apply paths | every save | C | yes |
| `curated/inseason`, `runtiming` | JSON | small | review app / hand | inseason publish, runs | weekly | C | yes |
| `generated/atlas/full/` | pickle + JSON + gpkg | 3.5 GB | `pipeline build --atlas` | reach, tiles, deliver, review app | few times a year | B | no |
| `generated/reaches/full{,_next,_prev}` | JSON tables | 0.74 GB × 3 | `atlas.reach.cli` | deliver | per build | B | no |
| `generated/tiles/` (`atlas.pmtiles`, `basemap.pmtiles`, `layers/*.geojsonl`) | PMTiles + GeoJSONSeq | 3.9 GB | `deliver.tiles` (+ basemap) | release, slow tests | per atlas | B | no |
| `generated/bundle/` (`bundle`, `regs`, `verdicts` .sqlite, `status_index.bin`) | SQLite + binary | 214 MB | `pipeline.deliver` | release builder, tools | per build | B | no |
| `generated/deliver-cache/` | step cache | 266 MB | `pipeline.common.stepcache` | deliver | per build | B (cache) | no |
| `generated/runtiming/` | CSV, GeoJSON, `.bin` cubes, `feed/base-<handles>.json.gz` ×7, `work`,`work2`,`work3` | 967 MB | `pipeline.runtiming.*` | `deliver runs`, runtiming feed | per fetch | B (+ ad-hoc) | no |
| `generated/gauges/feeds/` (`clim.json`, `index.json`, 438 stations), `hydat_daily/` | JSON | 49 MB | Python publisher (local), clim stage | upload.sh → R2, dev server | per HYDAT / by hand | B | no |
| `generated/gauges/candidates.json`, `match.json` (+ `match.kilgard.json`, `match.t159.json`) | JSON | 1.8 MB | matcher / promote | review, promote | rarely | B | candidates only |
| `generated/review/`, `generated/sloughs/` | JSON(.gz) | 24 MB | many one-off tools | review app | per tool run | B | **yes** |
| `generated/regs/extraction`, `parse`, `dfo_salmon` | JSON, PNG | 0.6 GB | extraction, parser, DFO scrape | parser, reach | per parse / scrape | B (+ F scrape) | no |
| `generated/regs/entries_backup/` | JSON copies | 517 MB | `reparse_candidates --apply` | nobody | rarely | backups of C | no |
| `generated/scratch/`, `owner1028/`, root `*.log` | anything | 2.4 GB | one-off audits, sessions | nobody | ad hoc | junk | no |
| `generated/build-manifest.json`, `build-logs/` | JSON, logs | 0.5 MB | `pipeline build` | `build --status`, `--dry-run` | per build | B (meta) | no |
| `release/r-<id>/` + `LATEST`; `release/tiles/<ver>/`; `release/basemap/` | gz SQLite + packed `.bin` + `manifest.json` (sha256, format, vintage) | 729 MB (4 releases, 3 tile sets) | `pipeline.deliver` / `pipeline release` | app (`packages/data`), staging/prod upload | per release | R | no |
| R2 `feeds/live/`, `feeds/dfo/`, `feeds/inseason/`, `private/feeds/…` | JSON | — | feeds worker; GH workflow; `sync_inputs.py`; `upload.sh` | app, worker | hourly to daily | R (+ C synced) | no |
| `data/sourcemaps/` | app sourcemaps, 34 builds | 450 MB | APK builds | crash reports | per APK | app build output | no |

### The same fact in two places

- `clim.json`: two writers into R2 `feeds/live/` (GH workflow and `upload.sh`); an upload can put an older HYDAT back (T554 §4).
- Gauge feed: two full publishers, Python (local) and TS (worker, served) that have drifted (T557 §1–2).
- Which water a book row means: in the entry's `matched` and in the matcher; `backfill_matched` overwrites one with the other (T546 §5).
- `found_names.json` (curated) is recomputed from a generated candidates file's auto-accept flag (T545 §6).
- Tiles: `generated/tiles/` and `release/tiles/<ver>/`; promoting means copying file by file (memory `tiles-dir-holds-basemap`).
- Reaches kept three times (`full`, `_next`, `_prev`, 2.2 GB); run-timing base kept once per atlas vintage (7 files).
- Curated backups: `generated/regs/entries_backup/` (517 MB) beside git history.

### Where a reader guesses a path or a vintage

- "Which release is live" is read seven ways; two fall back to the newest folder by mtime; some skip the manifest check (T560 §6).
- `vintage.tile_vintage()` returns None on a broken sidecar instead of raising (`pipeline/common/vintage.py:26-34`).
- `hydat_daily` takes its station list from whatever is in a local folder (T557 §1).
- `feeds/inseason/matches.json` and `runtiming/index.json` sit at one unversioned key bound to one release (T555 §2).
- The gauge feed has no format tag; the app accepts any JSON (T557 §3–4).

## 2. The real pains (with evidence)

| # | pain | evidence | status |
|---|---|---|---|
| 1 | **Stale pair: tiles and bundle from different atlases.** 339 gauged sections had no tile; 16 of 20 Fraser mainstem sections painted wrong. Nothing errored. | memory `tiles-bundle-same-vintage` (2026-09-02); `pipeline/common/vintage.py` docstring | guarded since (strict vintage check at end of build, `useVintage` in app) |
| 2 | **Atlas does not record its curated inputs.** `graph.pkl.meta.json` has `"input_hashes": {}` today. A release can ship cuts, names, sloughs or added waters edited after the atlas was built. | T547 §1–2; checked: `data/generated/atlas/full/graph.pkl.meta.json` | open |
| 3 | **Half-written atlas can be promoted**; two bare renames, no COMPLETE marker, `_next` reused without clearing. | T547 §4 (`pipeline/atlas/promote.py`) | open |
| 4 | **Programs rewriting curated data.** A full gauge re-match demoted 25 reviewed stations and rewrote 1,042 rows; a DFO `live queue` re-emitted a whole region and made 6 owner marks "rule changed"; `found_names.json` follows a generated flag. | memory `gauge-regenerate-drifts`, `dfo-queue-stales-marks`; T545 §4, §6 | partly guarded (promote refuses demotions; shrink limit) |
| 5 | **Feeds and release have no shared vintage.** A DFO place curated after a release is in the feed but no release chain, so it shows nowhere. Release-bound feeds at one key orphan older devices. | T549 §1–3; T555 §2 | open |
| 6 | **Two writers / two publishers for one file.** clim.json (T554 §4); gauge feed Python vs TS (T557). | tickets | open |
| 7 | **Hand-made copies of one helper.** Atomic write re-done six times; some curated/published writers skip it. Live-release reader seven ways. | T560 §6–7 | open |
| 8 | **Moving folders by hand loses things.** Basemap (0.9 GB) lost when the tiles dir was replaced; `rtk ls` filter deleted the live bundle; promote copied only the pmtiles and left old `layers/`. | memory `tiles-dir-holds-basemap`, `rtk-ls-adds-sizes` | rules written; no tool does the promote file-by-file for you |
| 9 | **Clutter in the build tree.** ~4.8 GB of scratch, session folders, `_prev/_next` copies, `work2/work3`, backups; generated root holds 12 loose logs. | `du` today | open |
| 10 | **Classes mixed.** Built data in `source/` (`basemap.pmtiles`, `runtiming/timing.json`, `bc_boundary.exact.wkb`); machine output in git under `generated/review` and `sloughs`; app sourcemaps under `data/`; Python scripts in `data/`. | listings above; `data/.gitignore` | open |
| 11 | **Slow builds.** reach 11m44s, deliver 11m49s, ~11 GB RAM each (`build --status`, 2026-11-09). | build manifest | not a format problem: the step cache (T522) already skips unchanged steps |

None of the eleven pains is "the format is too slow or too big". Ten are about **which version, who wrote it, and
was it finished**.

## 3. Options compared

What kind of data is ours? Mostly: rule/section **tables**, one **graph** (pickle), **geometry** (gpkg, geojsonl),
**vector tiles** (PMTiles), and **small JSON**. Real N-dimensional arrays exist but are small:
- `clim.json`: station × variable × 73 pentads × 5 percentiles, 8.5 MB (plus recent years per station).
- `runtiming/run_cube.bin`, `passage_cube.bin`: profiles × species × day, 0.25 and 0.6 MB.
- HYDAT daily series: station × day, but it is ECCC's own SQLite (a source; we do not own its format).
- Forecast runs: `gauges/feeds.runs/*.jsonl`, 268 KB.

| option | made for | fit with ours | gets better | gets worse | cost to move | git diffs of curated | app release files | owner's tools |
|---|---|---|---|---|---|---|---|---|
| **Change nothing** | — | it works today | nothing | the open pains stay: pains 2, 3, 5, 6, 7 each can ship a wrong answer quietly. Sep–Nov saw a data-handling incident about every 2–3 weeks (pains 1, 4, 8), each costing an evening to a session to find and repair; most were then guarded one by one | 0 | stay good | unchanged | unchanged |
| **Zarr** | big chunked N-d arrays (climate grids, satellite cubes) read in parallel from cloud storage | **poor**: our arrays are < 10 MB and read whole; the readers are a TS worker and a phone app with no Zarr library | nothing measurable | new dependency in Python, worker and app; binary chunks in folders; harder to look at | high (new readers in 3 runtimes) | n/a (not for curated) | would have to change: not allowed | none of them read Zarr |
| **Parquet / GeoParquet + DuckDB** | big column tables, analytics queries | **partial**: good for a few big intermediates (`section_run.csv` 101 MB, `section_cu.csv` 95 MB, reaches 0.74 GB, `tiles/layers` 3.5 GB) | smaller files, typed columns, quick ad-hoc SQL on intermediates | binary: no `grep`, no line diffs; tests that grep `layers/` need rewriting; DuckDB new dependency (pyarrow already in) | medium, per intermediate | **bad** if used for curated (unreadable diffs) | unchanged if kept to intermediates | DuckDB CLI is easy, but one more tool |
| **One SQLite / GeoPackage per stage** | single-writer local databases | **fair**: we already ship SQLite and read gpkg | one file per stage = atomic swap, schema in the file | binary diffs; locking if two stages write; another refactor of every writer | medium–high | **bad** for curated | unchanged | owner already uses sqlite3 |
| **Keep formats + one layout + per-build manifest + one reader/writer + schema checks** | exactly our problem: provenance and vintage | **good**: most pieces exist (`build-manifest.json`, release `manifest.json` with sha256/format/vintage, `pins.json`, `CURATED`/`GENERATED`, stepcache) | targets pains 2–10 directly | nothing gets slower; a few more checks at build/release | low: small tickets, mostly already open | stay JSON, stay diffable | unchanged | unchanged |
| **DVC** (data version control) | teams versioning big files and pipelines beside git | **weak**: `pins.json` + R2 already pins big inputs by sha256; `build.py` already keys stages | a standard tool | second cache, second command set, a remote to keep in sync; duplicates what exists | medium | unchanged | unchanged | new tool to learn |
| **Delta Lake / Iceberg** | many writers on huge tables in a data lake, time travel | **none**: one person, one Mac, GB not TB | — | Spark/Java-style tooling | high | n/a | n/a | none |
| **One DuckDB file for everything** | one analytical database | **poor**: one file for curated + built means one blast radius; parallel stages fight the lock | single place to query | binary curated data; a corrupt file loses everything at once | high | **bad** | unchanged | DuckDB CLI |
| **FlatGeobuf for geometry** | fast streaming of geometry with a spatial index | **small, optional**: could replace `tiles/layers/*.geojsonl` (3.5 GB) as tippecanoe input | smaller, faster layer reads | binary; tests that grep layers need a reader | low–medium, one stage | n/a | unchanged | QGIS reads it |
| **Mix: the recommended one** | — | keep JSON for curated, SQLite/PMTiles/bin for release, today's formats for built; add the strict layout + manifest + one writer; Parquet or FlatGeobuf only for an intermediate that a measured stage shows is slow | pains 2–10 | — | low | stay good | unchanged | unchanged |

**Does any option help the app or the worker?** No. Release files stay as the app reads them (gz SQLite + packed
`.bin` + `manifest.json`, already content-named and sha-checked). The feeds' gaps (T555, T557) are missing version
tags, not format.

## 4. The split by source type (and stricter curated data)

Whatever the format, the four classes should never share a folder or a file.

| class | lives today | mixed with | who may write | in git | protected by |
|---|---|---|---|---|---|
| **1. Fetched / raw** (government downloads, scrapes, caches) | `data/source/` | built files: `basemap.pmtiles`, `runtiming/timing.json`, `bc_boundary.exact.wkb`; scrapes also in `generated/regs/dfo_salmon`, `generated/runtiming/feed` | fetchers only | small ones; big ones pinned in `pins.json` + R2 | sha256 pins, `.gitignore` |
| **2. Curated** (human decisions: marks, matches, cuts, overrides, corpus) | `data/curated/` | parser output in `synopsis/region-*.json` (parser + review + tools write one file); backups in `generated/regs/entries_backup` | humans, review write-through, **and at least ~10 pipeline tools** (promote, `mined_names`, `tidal_limits`, `dfo_salmon live queue`, `steelhead.accept`, `backfill_matched`, `ingest_catalogue`, `reparse_candidates`, `remap_boundaries`, `drop_on_parts`) | yes | `CURATED` paths, loaders raise on missing, `check_curated`, SHRINK_LIMIT, promote refuses demotions, AGENTS rule 2 (back up by hand) |
| **3. Built / intermediate** | `data/generated/` | machine review inputs **in git** (`generated/review`, `sloughs`, `gauges/candidates.json`); scratch, sessions, logs, backups | pipeline steps | mostly no | stage keys, step cache, `require_build()` |
| **4. Release** (app and worker read) | `data/release/` + R2 `feeds/…` | R2 also holds curated copies (`private/feeds/dfo/entries`, wording) synced from the working tree with no release check (T549) | `pipeline release` / deliver; nobody after build | no | content-named files, sha256 + format in manifest, LATEST pointer |

**Stricter curated data — concrete proposal:**
1. **One home.** Everything a human decided is under `data/curated/` and nowhere else. Move curated backups out of
   `generated/` into `data/backups/curated/<date>/` (or rely on git; pick one). Parser output that is not yet
   reviewed stays a proposal in `generated/` (memory `everything-added-is-a-proposal`).
2. **One writer path.** `pipeline/common/curated_write.py`: `write(key, data, by=, source=)` does validate (the
   pydantic/schema check the reader already does) → back up the old file → atomic replace → append one line to a
   write log (`curated/_log.jsonl`: file, who, when, tool, old/new sha). Every tool above calls it; the six atomic
   copies (T560 §7) go away. A grep test refuses `write_text`/`json.dump` on a `CURATED.` path anywhere else.
3. **Provenance on each record.** New and changed records carry `by`, `at`, `source` (marks already do:
   `reviewer`, `date`; `matches.json` rows have `resolved_by` but no who/when). Add on write, never backfill guesses.
4. **No build step may rewrite it.** A test runs the reach, tiles and deliver steps (a small fixture) with
   `CURATED_ROOT` pointed at a read-only copy and asserts the tree's digest is unchanged. The atlas records the
   digest it read (T547); release refuses a mismatch.
5. **Regenerators write proposals, never the file.** A re-match or re-mine writes to `generated/` and the review app;
   only a mark or an explicit `--only <ids>` promote moves rows into curated (memory `gauge-regenerate-drifts`,
   `lock-reviewed-domains`: write-through on save stays the only everyday path).
6. **Backups.** The writer keeps the previous version of a file it changes (last N per file) before every write, so
   AGENTS rule 2's manual `cp` becomes automatic.
7. **Readers.** No reader takes a curated file at run time once a bundle exists (memory `bundle-is-the-only-source`);
   keep that test.

## 5. Recommendation and staged plan

**Do not switch formats.** Adopt the "mix": today's formats, a strict four-class layout, a manifest that records
inputs at every stage (the atlas included), one writer for curated data, one reader for "the live release".

| step | what | pays for itself because | size | tickets |
|---|---|---|---|---|
| **1 (first)** | **Curated guard.** Atlas writes a digest per curated file it read into `graph.pkl.meta.json` (`input_hashes`, already supported by `pipeline/common/io/serialize.py`); `pipeline release` refuses an atlas older than its curated inputs. A test proves no build step changes the curated tree (read-only `CURATED_ROOT` copy). | closes pain 2 (ships wrong answers today, silently) and pins pain 4 with a test | ~1 day | T547 §1–2 |
| 2 | **One curated writer** (`curated_write.py`: validate, backup, atomic, write log, provenance) and move the ~10 writing tools onto it; grep test. | removes the six atomic copies, makes backups automatic, makes every curated change traceable | 2–3 days | T560 §7, T545 §4/§6 |
| 3 | **Finished-or-not and which-version.** Atlas COMPLETE marker + safe promote; one `latest()` for every Python/JS reader, no mtime fallback; format tag on the gauge feed; feeds name the release they belong to; one clim.json writer. | pains 3, 5, 6, 7 | 2–4 days, split | T547 §4, T560 §6, T557 §3–4, T555 §2, T554 §4 |
| 4 | **Tidy the layout.** Move built files out of `source/`; give machine review inputs their own folder (still in git) apart from build output; one `generated/tmp/` for scratch/sessions/logs that is safe to delete; prune `_prev/_next`, `work2/3`, old runtiming bases (≈4.8 GB); sourcemaps out of `data/`; a `promote` command that copies tiles file by file. | stops hand moves losing files (pain 8) and makes the tree one honest listing | 1–2 days | new ticket |
| 5 (optional) | Parquet (or FlatGeobuf) for one intermediate **only if** `build --status` shows a stage dominated by reading it. | only when measured | per file | — |

**Do not move:** curated JSON (it must stay diffable); release files (the app reads them as they are); PMTiles;
`bundle.sqlite`; HYDAT's own SQLite; `graph.pkl` (fine while the build is the only reader; note pickle ties it to the
Python version); the small arrays (`clim.json`, run cubes) — JSON/bin is right at their size and readable by the worker.

**Not verified here:** I did not time any reader or measure Parquet sizes; the "about every 2–3 weeks" incident rate
is counted from memory notes and STATUS, not a log. The list of curated-writing tools comes from a grep of modules
that use `CURATED` and also write files (34 modules), read down by hand to those that write curated paths; step 2
should confirm it.
