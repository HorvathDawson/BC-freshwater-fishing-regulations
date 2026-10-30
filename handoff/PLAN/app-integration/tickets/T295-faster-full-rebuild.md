# T295 — Faster full rebuild (do it the next time a full run is needed)

Status: OPEN (owner 2026-10-29: "maybe there are obvious ways to speed up the full rebuild for future? should be done the
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
