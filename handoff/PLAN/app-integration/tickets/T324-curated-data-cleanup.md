# T324 — One clean data flow: document every stream, then lock reviewed data as final curated files

Status: DONE — 73c04dbc2 (naming pass, out of curated, check_curated --waters, steelhead accept), e064ab2ea (worker: one wording copy, fail loud; not deployed), f90fbb988 (sync inputs on push, staging); data-flow.md rewritten in the ticket commit. Earlier: plan cbfc0adc7, write-through T325.
the flow of all the data streams too. … after we finish doing the review … lock all the data in curated in the format
they will be useful like sloughs etc. … convert marks into that format for all the data types … Right now the whole flow
feels really shitty and awkward.")

## Today (the awkward part)
- Marks (`data/curated/regulations/review/marks.jsonl`) are review STATE. T266's exporter turns them into
  `data/curated/decided/*.json` (sloughs, tidal, dfo, ponds, names, entries, splits…) — but each domain is read
  differently: some by the build, some not at all yet (sloughs → T322), some files live outside data/curated
  (slough page verdicts in handoff/PLAN, moving in T322).
- Other curated inputs sit beside them in their own shapes (gauges/matches.json, dfo_salmon/, inseason/, runtiming/,
  regulations/entries/).

## Work
1. **Data-flow map** (`handoff/PLAN/data-flow.md`): every stream end to end — source (FWA, synopsis PDF, DFO pages,
   HYDAT, ECCC, PSF, OSM, owner review) → raw/cache → curated (human) → generated (build stages) → release files →
   R2 / feeds worker → app. Per stream: files, the tool that writes each, who reads it, how it is refreshed, where
   human time is embedded. Flag every place a reader skips the curated file, two files say the same thing, or human
   work lives outside data/curated.
2. **End state per domain:** one final curated file per data type (sloughs, tidal, DFO extents, ponds, names, splits,
   gauges, in-season, run timing…), in the shape the build uses directly (no marks, no review fields), documented
   schema, read by exactly one stage.
3. **Write-through (T325, replaces the lock step):** a saved mark updates its domain's final curated file directly;
   marks.jsonl stays history and is never edited. A check proves the final file reproduces today's build.
4. **Cleanup list:** stray files, duplicate paths, dead readers — ticket each.

Depends: T266, T322. Size: M (doc + plan), then per-domain lock tickets. Owner: approve the end-state shapes.

## T408 re-scope (owner walkthrough 2026-11-05, `handoff/PLAN/data-flow-decisions.md`)
Now the cleanup change set for the curated layer:
1. **Naming pass** (the decisions table, "Naming pass"): `git mv` every file once; code, config, docs, tests together;
   every JSON keeps an `_about` header, every curated folder a short README. Conventions: `*_calls` / `*_waters` / `*_stations`.
2. **Out of curated:** `decided/` (with T325), `waters/sub_lake_areas.json` → the deferred lake-splits ticket,
   `review/dfo_fix_proposals.json` → `data/generated/review/`, `gauges/candidates.json` → `data/generated/gauges/`.
3. **Fast curated check** (schema + references) for the hand-edited waters files, on commit and as the build's first step.
4. **Steelhead accept command:** prints the list diff (added / dropped) and writes the curated file; no hand copy.
5. **Worker wording: one copy** — drop the compiled-in `wording.json`; missing/invalid R2 copy fails the DFO job loudly.
6. **Sync inputs on push to STAGING** (`feeds-sync-inputs.yml` push trigger); prod stays manual.
7. Rewrite `data-flow.md` to match the result.
- (from T266/T325, 11-05) `pipeline/tools/slough_calls.py` ABOUT still says "applied from data/curated/decided/sloughs.json";
  fix it in the rename change (it is written into `slough_calls.json`, so it changes a build input once).

## Done (2026-11-05)
- Naming pass + moves as the table; `marks.jsonl` moved, bytes unchanged. Build keys by content: the renames rerun
  nothing (`build --dry-run` before/after: clim + tiles up to date both times; reach + deliver were already due).
- Left for later: config attributes `waters.tidal` / `waters.areas` keep their old names, and the `_about` of
  `tidal_limits.json` / `area_closures.json` still says "splits.json" — both live in tile-stage code / a tile key, so
  changing them rebuilds and re-ships the tiles; do it with the next tile change. `guide_words.py` STEELHEAD_TEXT still
  names the old steelhead path (shipped export text; change with the next release).
- Restart the review app after pulling: its curated paths are read from config at start.
