# T435 — Pipeline: hard-coded data paths, mtime staleness, two release-id formulas

Status: DONE 2026-11-06 (chat 3, HASH) — from the audit (T333–T335, 2026-11-05). Severity: **medium**. Verified by a second reader.

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

## Result (2026-11-06)
- Paths: 23 one-string `ROOT / "data/…"` literals now read `CURATED` / `GENERATED` / `SOURCE` (new `generated.sloughs`);
  `check_curated`'s three `or ROOT/…` fallbacks dropped (an undeclared file says "not declared"). CI gate: a new
  one-string pattern (tests exempt; mutation: HEAD's slough.py → 2 hits). **No** for `proposals.side_dir` (a printed,
  repo-relative command path), `tidal.DEFAULT_CACHE` (a citation label, never opened), `review/dfo.py`'s `cache/`
  (the DFO scrape cache, outside the data trees like `fetch.DEFAULT_CACHE`).
- Gauge staleness: the gate (`check_record`) was already content-only; the two mtime tools (`--stamp-atlas`,
  `--record-cuts`, for atlases older than the record) are deleted — the live atlas carries a build-time record. Test:
  no `st_mtime` / `build_ts` left in `gauges/matches.py`.
- Release id: one formula, `rebase.id_of`, used by `build.ship` and `rebase.release_id`; the built-release recompute
  test already existed (`test_release_basemap.py`); the live release recomputes to its own id.
- `_reach_run`: digest only; an unreadable report is refused (every run on disk has a digest).
- `vintage`: an unreadable tiles `atlas.meta.json` is refused (strict) / said "not checked" — no silent skip.
- `get_path` raises `KeyError`; DFO `save()` validates (reads back as itself) and writes atomically (live files
  round-trip byte-identical); `page-defaults` refuses a missing scrape / preamble; slough `_page_notes` raises.
  Not changed: `presence.py` (fails loudly already, per the second reader).
Tests: `test_pipeline_refusals_t435.py`, `test_reach_run_pairing.py`, `test_gauge_matches_record.py`.
