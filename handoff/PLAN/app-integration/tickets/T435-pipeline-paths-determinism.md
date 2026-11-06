# T435 — Pipeline: hard-coded data paths, mtime staleness, two release-id formulas

Status: OPEN — from the audit (T333–T335, 2026-11-05). Severity: **medium**. Verified by a second reader.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: pipeline 3, 5, 6, 8, 9, 11, 15, 16, V1; `*.verified.md` has the evidence per finding).

## What happens
Hard-coded paths outside `CURATED`/`GENERATED`: `preverify.py:598/611/622`, `tidal_pass2.py:752`,
`check_curated.py:152/160` (an `or ROOT/…` fallback), `slough.py:28-30`, `review/dfo.py:291` (`cache/`); the CI
path gate misses the one-string form `ROOT / "data/curated/…"`. `gauges/matches.py:276-301` judges staleness by
file mtimes vs the atlas `build_ts` (a copied atlas refuses falsely; a touched file passes). The release id is
computed in `release/build.py:609-614` and `rebase.py:25-41` (a drift makes every rebase refuse).
`bundle/build.py:124-149` `_reach_run` still pairs by folder NAME when no digest matches. `common/vintage.py:290-299`
skips the run-timing tile check silently if `atlas.meta.json` is unreadable. `project_config.get_path` returns
`Path()` (cwd) for a missing key. `dfo_salmon/entries.py:675-679` `save()` writes curated places without temp+replace
or re-validation.

## Fix
Read through `CURATED`/`GENERATED` + a CI pattern for the one-string form; content hashes instead of mtimes; one
release-id helper + a test that a built release recomputes to its own id; `_reach_run` digest-only (refuse
otherwise); vintage refuses an unreadable meta; `get_path` raises; `save()` atomic + validated.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: M. Owner: no (picks the order).
