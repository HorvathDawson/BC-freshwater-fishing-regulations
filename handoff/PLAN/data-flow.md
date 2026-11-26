# Data flow: every stream, end to end

The map of how data gets from the sources and the owner's decisions to the app. Detail lives in the code named here;
`pipeline/build.py` (its docstring) is the build's own description, `pipeline/README.md` says which edit reruns what.
Each curated folder has its own `README.md` (`data/curated/*/README.md`). The owner's choices behind this map are in
`app-integration/decisions.md`, section 12.

## Three kinds of data, one folder each

- `data/source/`: fetched (costs bandwidth): the FWA gpkg, the synopsis PDF, HYDAT, PSF and NuSEDS pages, OSM and city layers.
- `data/generated/`: computed (costs CPU): the atlas, reach run, tiles, release candidates, and Claude's proposal files
  (`review/`) and queues. Rebuildable; never the owner's decision.
- `data/curated/`: the owner's decisions and hand-made data (costs human hours). Curated holds only files the build reads.
  Names: `*_calls.json` are his per-item decisions, `*_waters.json` a list of waters, `*_stations.json` a list of gauges.

## A review decision makes one hop

The owner saves a mark in the review app. The mark is appended to `data/curated/review/marks.jsonl` (the log, only the
review app appends, AGENTS A8) and the server writes it straight into its domain's final file. The build reads only the final files.

```
review app -> review/marks.jsonl -> write-through (apply) -> final curated file -> build -> release -> app
                (the log)           cuts.json, synopsis/*, dfo_salmon/places/, tidal_limits.json,
                                    slough_calls.json, found_names.json
```

`python -m pipeline.regs.review.apply [domain] [--dry-run]` is the same apply by hand. There is no `decided/` copy.

## The map

```
source (fetched) --+                    curated (decisions) ---------------+
 FWA, PDF, HYDAT,  |                     waters/ regulations/ dfo_salmon/    |
 PSF, OSM          |                     gauges/ runtiming/ inseason/        |
                   v                                                         v
                 atlas (only with --atlas, then promote) -> clim, reach, tiles -> deliver -> data/release/r-*
                                                                                     |  pipeline release
live web pages: DFO salmon, gov in-season, ECCC realtime --> feeds Worker <-- R2 <---+  (staging / prod)
                                                                |                 |
                                                                +----> R2 -----> app
```

The feeds Worker (`app/workers/feeds/`) reads R2 and the live pages and writes the live feeds back to R2; the app reads R2.

## The build and the release

- `python -m pipeline release --staging` ships: build with `--hold-latest`, the checks, promote `CANDIDATE` to `LATEST`,
  upload to staging; stops at the first failure and resumes. `--prod <r-id>` re-uploads the same checked bytes to
  production and is the owner's (`CONFIRM_PROD`, AGENTS A7). `--dry-run` prints the plan. Code: `pipeline/release.py`.
- `python -m pipeline build` runs, in order: the fast curated check (`python -m pipeline.tools.audit.check_curated
  --waters`: shape and references of the hand-edited waters files, under a second), the preflight (sources present,
  promoted artifacts not stale, tippecanoe), then each stage whose key moved: `clim`, `reach`, `tiles`, `deliver`.
  `python -m pipeline build --graph` prints what each stage reads and writes.
- A stage key is a digest of the bytes it reads (content, not file names); marks are in no key, because a mark
  changes the build only through the final file it writes.
- The atlas is built only with `--atlas` (then `--promote`), into `<build>_next`; a promoted atlas is immutable
  (`pipeline.atlas.promote`). Everything downstream reads the atlas's sidecars, never a curated waters file
  directly: AGENTS rule 56, pinned by `test_one_data_flow.py` and `test_dataflow_gates.py`.
- `pipeline-ci.yml` runs the cheap guards on every push: every curated path resolves, `check_curated --ci` and
  `--waters`, generated files current, the no-data test tier, no hard-coded curated paths.

## The streams (file, written by, read by)

### Waters: `data/curated/waters/` (the shape of the map; a change reaches the map at the next atlas build)

| file | written by | read by |
|---|---|---|
| `cuts.json` | hand; write-through (split, entry, DFO marks) | atlas sectionizer, review app |
| `name_variants.json`, `area_closures.json`, `added_areas.geojson`, `added_lakes.geojson` | hand | atlas |
| `added_streams.json` | the added-streams resolver, promoted (never rerun: negative ids are an ABI) | atlas |
| `found_names.json`, `found_name_calls.json` | `tools.mined_names`; write-through (Names marks); hand | atlas registry |
| `continuations.json` | hand (reviewed or refused) | bundle (`water_continue`) |
| `tidal_limits.json`, `tidal_split_authority.json`, `shore_held.json` | `tools.tidal_limits`; write-through (Tidal marks); hand | atlas, then reach and tiles |
| `slough_calls.json`, `slough_page_calls.json` | `tools.slough_calls`; write-through; frozen old notes | atlas registry |
| `steelhead_waters.json` | `steelhead.accept --write` | reach run |
| `unmapped_waters.json`, `pond_calls.json`, `unplaced_piece_calls.json` | hand | `check_curated --waters`, atlas, reach |

### Regulations: `data/curated/regulations/`

| file | written by | read by |
|---|---|---|
| `synopsis/region-*.json` | the parser (human only, spends credits, AGENTS A1); write-through (entry and rule marks); hand (back up first, A2) | reach, deliver, review app |
| `book_name_links.json`, `under_closure_calls.json`, `word_reads.json` | hand | matcher, reach, review app |
| `reference/*.md` | hand transcription of the printed book | answers glossary |

Steelhead list: `python -m pipeline.regs.steelhead.known_waters` writes `data/generated/steelhead/steelhead_waters.json`;
`python -m pipeline.regs.steelhead.accept` prints the waters added and dropped; `--write` writes the curated list. A
rerun of the generator never changes the curated list by itself.

### DFO salmon: `data/curated/dfo_salmon/`

| file | written by | read by |
|---|---|---|
| `places/region-*.json` | DFO review (write-through) and placing | reach, deliver, review app; the Worker via R2 `private/feeds/dfo/entries/` |
| `wording.json`, `closed_areas.json`, `scope_places.json`, `tidal_calls.json` | `dfo_salmon.wording build` and hand | Python and the Worker (wording: one copy in R2 `private/feeds/dfo/wording.json`; missing or invalid fails the DFO job and reports, the app keeps the last good feed) |

The Worker reads the DFO pages; `dfo_salmon.live` writes the typed result the bundle reads. Every saved DFO match is
checked by the build against that build's registry. A DFO name with no match is held in `unplaced.json` and the owner
gets an "N rules held unplaced" email.

### Gauges: `data/curated/gauges/`

| file | written by | read by |
|---|---|---|
| `matches.json` | `gauges.generate.promote` only, one station at a time | atlas, bundle |
| `match_calls.json` | hand (his calls, an input to matching) | match, promote |
| `tidal_stations.json`, `backwater_stations.json`, `diversion_stations.json`, `datum_change_stations.json` | hand | bundle |

Generated side: `data/generated/gauges/match.json` and the review queue `candidates.json`. HYDAT comes in with
`scripts/update-hydat.sh`; a newer HYDAT starts the `feeds-hydat` workflow from the Worker (never a GitHub schedule).

### In-season, run timing

- In-season (`inseason/notice_waters.json`): written by the review app's In-season tab or by hand; `inseason.publish`
  puts it in R2 `feeds/inseason/`. Informational: a banner on the water, never an answer change.
- Run timing (`runtiming/run_calls.json`, hand): `runtiming.parse` writes `data/source/runtiming/timing.json` for
  deliver; the `feeds-runtiming` workflow (a "slow feed": own R2 folder and `index.json`, started by a button or
  the Worker, publishes only on a real change) writes R2 `runtiming/`.

### Worker inputs to R2

`python -m pipeline.deliver.sync_inputs` is the one list of what the Worker reads from R2 beside a release (DFO
places, the wording registry, the station roster, the in-season links). When the review app saves one of them it runs
`deploy/staging/sync-inputs.sh`, which puts what changed into staging R2. Production is a manual run at the prod switch.

### Release to app

`data/release/<r-id>/` (deliver) -> `data/release/CANDIDATE`, then `LATEST` once every check passes -> R2 `release/`
(staging: Claude may; production: the owner) -> the app boots from the manifest, installs, paints.

## Where the owner's time goes

| when | what | command or place |
|---|---|---|
| reviewing | save marks; they land in the final file at once | review app |
| a rare hand edit | edit the file; the build's first step checks it | `check_curated --waters` |
| a new synopsis | run the parser (credits) | `pipeline/regs/parsing/run_parse.sh` |
| a new steelhead list | read the diff, accept | `pipeline.regs.steelhead.accept [--write]` |
| production | promote the checked staging release | `CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>` |

Open work on this flow is in tickets (lake splits, T281, deferred).
