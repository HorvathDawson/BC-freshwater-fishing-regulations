# T324 — One clean data flow: document every stream, then lock reviewed data as final curated files

Status: PARTIAL — plan done (data-flow.md, cbfc0adc7); owner approved the design 2026-10-30; the lock step was replaced by write-through (T325: a saved mark updates its final curated file; lock.py deleted); work in T325–T331. (owner 2026-10-30: "I thought the marks got converted into our real curated data type? We need to document
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
