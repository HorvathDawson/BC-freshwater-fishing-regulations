# T295 — Faster full rebuild (do it the next time a full run is needed)

Status: IN REVIEW (2026-11-05, T408 re-scope, 9bc734bfa) — `python -m pipeline release --staging` (pipeline/release.py): preflight → baseline → `pipeline build --hold-latest` via heavy.sh (cached stages) → checks → promote LATEST → staging upload; stops at the first failure with a plain message, resumes from the ledger; `--dry-run` plan; tests `test_release_command.py` (17). Not run end to end (no build/upload by the agent). Runbook `handoff/PLAN/runbooks/release.md`. Earlier: 2026-10-31, ea9cade2a — stage timings + peak RSS + per-input reasons, parallel stages, cached rerun skips; 59 → 47.5 min. Not done: atlas DEM/FWA memoisation; tile_version determinism.

Was: OPEN (owner 2026-10-29: "maybe there are obvious ways to speed up the full rebuild for future? should be done the
next time a full run is needed").

Source: the 10-29 data rebuild took ~3.5 h: a full atlas rebuild (added-streams topology change) then reach, tiles, deliver;
it ran twice (tiles had started before a tile-attribute commit) and queued behind other heavy jobs.

## Work (measure first, then pick the obvious wins)
1. Time every stage of `python -m pipeline build` on the next full run (per-stage timings in the log).
2. Candidates: cache stage outputs keyed by their inputs' hashes so an unchanged stage is skipped (added streams, registry,
   graph, reach, tiles); parallelise independent stages and per-region work within the ≤4-worker rule; make tiles depend on
   code too (a code change after tiles started forces only tiles to rerun, not the atlas); avoid recomputing the DEM / FWA
   loads (memoised parquet); a "reach-only / tiles-only" decision printed up front from what changed.
3. Keep determinism (atlas-build-not-deterministic memory: subtract old-vs-old noise) and the binding-diff gate.

## Acceptance
Before/after stage timings on a full run; an unchanged-input rerun skips cached stages; outputs byte-identical (or explained).

Depends: none. Size: M. Owner: no.

## T408 re-scope (owner walkthrough 2026-11-05, `handoff/PLAN/data-flow-decisions.md`)
Part of one command, `pipeline release --staging`: build → checks → promote LATEST → upload staging; stop at the
first failure; **resumable** (finished stages cached); stage-by-stage progress; a plain failure message.
