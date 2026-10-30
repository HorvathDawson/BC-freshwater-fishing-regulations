# Build pass 2026-10-31 — T266 + T295 + T297 (overnight, owner pre-approved)

Nothing was promoted, uploaded or shipped: LATEST still names r-51d80e68f056. The proof ran as a SIDE run.
The owner reviews first.

## What changed (commits on redesign/stream-sections)

| commit | what |
|---|---|
| b1f58097f | `scripts/heavy.sh` is re-entrant: a heavy.sh run inside a heavy job (`HEAVY_HELD` names the lock owner) runs straight away under the outer lock. This fixes the build-apk.sh deadlock. T303's stale "Phone: UserLocation" line now names the own-dot implementation (d51ca02c8). |
| ec2d60bdd | **T266** `python -m pipeline.regs.review.export [--check]`: the marks in force become `data/curated/decided/{tidal,dfo,entries,splits,sloughs,ponds,names,proposed_splits}.json`, one record per line. Each record has provenance: the mark's line sha, line number, reviewer and date. Cleared marks are respected, conflicting tidal cuts are refused, and the owner's pre-Tab tidal calls are folded in. The apply paths now read only the export: `proposals`/`apply_proposals` (which refuse while the export is behind the marks), `tools.mined_names`, `dfo_salmon.suggest` and `tools.audit.tidal_preview`. `CURATED.decided` is new. `review.marks` no longer has to exist. The 1,144 proposals and the mined names are byte-equal before and after. |
| ea9cade2a | **T295/T297** `pipeline build`: one `STAGE_GRAPH` (`--graph`). Independent stages run in parallel (`--jobs`, default 2, max 4; each stage logs to `build-logs/<run>/<stage>.log`). The manifest records per-stage seconds, peak tree RSS and per-input digests, so the plan prints *why* a stage runs ("inputs changed: code, curated"). There is a timings table (`--status`). `--side DIR` runs every stage into DIR and never touches the live outputs or LATEST. Deliver times each step. The marks and decided/ are in no stage key, even through `curated/base`. `scripts/build-progress.sh` is removed (replaced by `--status`). |
| ae5cb8e6a | docs: pipeline/README.md and AGENTS 62. |

Tests: `test_review_export.py` (new, 7), `test_build_command.py` (+6: parallel order, a failure stops launches, reasons, side
paths, marks in no key with the digest never opening them, the curated tree loading without marks.jsonl),
`test_review_proposals.py` (apply reads the export).

## The proof: ONE full rebuild (side run)

```
scripts/heavy.sh build-pass 70 -- sandbox-exec -p '(deny file-read-data marks.jsonl, data/curated/decided/)' \
  .venv/bin/python -m pipeline build --side data/generated/side/build-pass-1031 --atlas --jobs 2
```

The build ran **with marks.jsonl and decided/ unreadable**: macOS sandbox, denied at the kernel. It passed. This is the T266
acceptance ("a build with marks.jsonl absent produces the same release"), and it is stronger than moving the file. The log is
at `data/generated/side/build-pass-1031.log`.

### Timings (T295)

| stage | before (10-29 runs, one after another) | this run |
|---|---|---|
| atlas | 29m21s | 26m20s |
| clim | 0m24s | 0m24s (alongside reach) |
| reach | 10m34s – 12m21s | 10m57s |
| tiles | 7m52s – 8m13s | 9m00s, **alongside reach** |
| deliver | 10m03s – 11m00s | 10m08s |
| **downstream wall time** | ~30 min (reach + tiles + deliver) | **~21 min** (tiles hidden under reach) |
| **total** | ~59 min | **47m30s** |

The peak-memory column reads 0.0 GB for this run only. The sampler runs `ps`, which the sandbox blocked. In a normal run
it reads correctly (checked). The machine had 90 % free memory at the start and 79 % at the end, and the heavy.sh guard
never fired.

Cached rerun: `pipeline build --side … --dry-run` right after the run reports all four stages **up to date**, so a stage
whose inputs did not change is skipped.

### Atlas
The side atlas (`full_next`, not promoted) against the live atlas: **IDENTICAL** — items 21,311 / 21,311, sections
4,211,558 (+0), boundaries 27,041 (+0), 0 changed. This run showed none of the old-vs-old noise the memory note warns
about (about 50 blocks), so there was nothing to subtract. Downstream stages read the promoted atlas, so the release is
comparable byte for byte.

### Release vs r-51d80e68f056 — every difference explained
The side release is `r-6474d28c0448`. It has the same 52 files with the same names.

- **All 51 data files are byte-identical**: water*, regs*, core*, salmon*, near, down, geo, method, paint, access,
  climate, conditions, flow-area and the synopsis. The bundle, verdicts, status index, ui-rules export/guide/answers and
  card-check stamp are byte-identical to the live homes. Reach `rule_section.jsonl.gz` and `dfo_salmon.jsonl.gz` are
  identical. `clim.json` is identical. The tile `layers/` (20 files) are identical.
- **manifest.json** differs in three ways, each explained:
  1. `published` and `minAppVersion` exist only on the live copy. They are publish stamps added at staging/publish time
     (`release/stamp.py`) and sit outside every hash.
  2. `tiles.current.*` differs, and with it `data_release`, because the release id hashes the tiles. **The cause is a
     determinism defect that predates this pass:** tippecanoe writes its own command line into the pmtiles metadata
     (`generator_options`). That command line includes a *random temp dir* (`/var/folders/…/tiles-lowband-<random>/low.mbtiles`),
     so every tile build gets a new `tile_version` even when every tile is the same. It is the only metadata difference,
     and it accounts for the 19-byte size change.
- Reach `report.json` differs only in `seconds`, the reach run's own wall time.

**Recommendation (not done, it needs a tile build to prove):** make the tippecanoe temp dir deterministic, or strip
`generator_options` before versioning. Then a tile rebuild with unchanged inputs keeps its tile_version, and the
release id stays the same. A follow-up ticket is proposed below.

## Tests (one full unfiltered run at the end, through heavy.sh)
Fast suite: **6193 passed**. Slow suite: **262 passed, 8 failed**, all on LIVE data the pass never touched, so none
were caused by this pass (the side rebuild matches the live bundle byte for byte). They are listed in **T306**. The
most important: `test_status_index` reports 198 of 6000 section-days where the index disagrees with effective_rules.

## Found on the way (not changed)
- **`data/curated/waters/mined_names.json` is behind its generator by one entry.** "Sheldens Creek" (blk 360846083) was
  accepted by the owner on 10-29 (`names.OWNER`, 0cb8ee2ca) but was never written. `python -m pipeline.tools.mined_names`
  would add it. That changes the atlas registry names, so it needs an atlas rebuild. It is left for the owner's next data
  release.
- Slough (811) and pond/Q154 (80) decisions now sit in `decided/`. Nothing applies them yet: the T55 fold and Q154 are
  still open.
- The review app still reads marks.jsonl for review state (held/landed/stale), as the ticket intends. Its "landed in
  data" view is unchanged.

## Not done / left for the owner
- **Confirm the file split.** Defaults taken: one directory, `data/curated/decided/`, one file per domain. The ticket
  suggested per-domain paths under waters/ and dfo/; one directory keeps "the exporter's output" in one place.
- Promote nothing yet. If this is accepted, the live pipeline needs no rebuild for these changes, since outputs are
  identical. The next `pipeline build` reruns reach, tiles and deliver once, because their keys now include new code and
  per-input records.
- Proposed ticket: tile_version determinism (tippecanoe temp path in `generator_options`).
- Atlas DEM/FWA memoisation (T295 item 2) was not attempted. The atlas is now the largest single cost, at 26 min.
