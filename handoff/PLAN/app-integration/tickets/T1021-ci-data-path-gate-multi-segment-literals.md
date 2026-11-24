# T1021 — The CI "no hard-coded data path" gate misses every multi-segment literal (T994 finding 7)

Status: DONE — 6e1289e3a, 0399ce248, 19a3704de

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1021 | The CI "no hard-coded data path" gate misses every multi-segment literal (T994 finding 7; ~10 live) | DONE | 6e1289e3a, 0399ce248, 19a3704de | — |

Source: T994 audit finding 7 (`handoff/PLAN/audit-2026-11-21/t994-audit.md`), checked still true 2026-11-22 (lane L13):
`.github/workflows/pipeline-ci.yml` "No hardcoded curated or source paths" matches `"data/<one segment>.<ext>"` only, so a
literal like `Path("data/source/hydat.sqlite3")` passes. Still in the tree: `pipeline/atlas/build.py:338,343` (source_ref
strings), `gauges/feed/publish.py:194` (`ROSTER_PATH`), `regs/dfo_salmon/churn.py:55`, `splitwork.py:35-36`, `entries.py:1204`,
`validate.py:202` (`cache/dfo_salmon/...`), `closure_mouths.py:164`, `regs/review/tidal.py:146`,
`regs/steelhead/known_waters.py:731`. (`hydat_daily.py` and `ingest_catalogue.py` were moved onto the config keys by
T993 G1, 8a89a73a9.) Each is relative to the cwd: a run from another directory reads or prunes a second tree silently
(W46); the DFO cache is named two ways.
## Work
1. Widen the gate to `"(data|cache)/[a-z_]+/` (comments and backtick docstring lines stay exempt, as now).
2. Add `SOURCE` / `GENERATED` / cache keys in `project_config.py` for the roster, the fisheries gpkg, the tidal definitions
   cache and the DFO cache, and move the literals onto them. The `source_ref` strings in `atlas/build.py` are labels, not
   paths read: say so in the gate's exemption, or build them from the keys.
## Acceptance
- The widened gate passes on the tree and fails on a planted `Path("data/source/x.json")` (a mutation test).
Depends: —. Size: M. Owner: no.

## Done (2026-11-24, lane L4)
- The gate (`pipeline-ci.yml` "No hardcoded curated or source paths", also fast-fail's step): any quoted string (f / r
  too) starting `data/<seg>` or `cache/<seg>` in pipeline / curation-review / project_config code; tests exempt; NO file
  allowlisted (the T993 F2 allowlist is gone). A string that only names a path carries `# path-label: <why>` (10+
  characters) on its line: 16 lines (provenance in outputs, message / help words, a docstring example, the roster name
  a notice prints, the T185 side dir's repo-relative name, git pathspecs). `pipeline/tests/test_data_path_gate.py` runs
  the step itself on planted trees (8 planted forms refused; comments, docs, tests, labels, other words pass) and on
  this tree; mutation-proved (old gate: 3 fail; reason not required: 2; a file allowlisted: 8; pattern narrowed: 3).
- Moved onto keys (each proved to resolve to the same file as the old literal from the repo root, and now from any
  cwd): the DFO cache (`config.yaml` `cache.dfo_salmon`, `get_config().dfo_salmon_cache`) for `fetch.DEFAULT_CACHE`,
  `churn._HISTORY`, `splitwork.HISTORY` / `RAW`, the `entries` / `validate` `--history-dir` and `untangle --cache-dir`
  defaults, `wording.saved_pages`; `closure_mouths --source` (`data_accessor.gpkg_path`); `pieces_gate --releases`
  (`generated.release`, not caught by the old pattern).
- THE DFO CACHE WAS NAMED TWO WAYS, ONE DIRECTORY: `Path("cache/dfo_salmon")` (the cwd's) in the scrape, and
  `parents[N] / "cache" / "dfo_salmon"` in `curation-review/backend/region_cascade.py` and `regs/review/dfo.py`. From
  the repo root both are `<root>/cache/dfo_salmon`; all now read the one key (`test_the_dfo_cache_has_one_name`).
  `GENERATED.regs.dfo_salmon` (`data/generated/regs/dfo_salmon`) is the scrape's OUTPUT, a different directory.
- Kept as it was: the DFO fetch's manifest records a default-cache page repo-relative (`cache/dfo_salmon/raw/…`), as
  the cwd-relative default wrote it.
- Review fixes (#97, 0399ce248): the gate is `scripts/check_data_paths.py`, by the AST. It now catches
  `os.path.join("data", …)`, `Path("data")`, `"./data/…"`, `"data/" + "x"` and any case. A code line holding a
  backtick is no longer exempt; only docstrings and comments are. The labels it lets through (15) are printed and
  pinned in the test, and a stale label or one with no reason is refused. Two more live literals were anchored:
  `extract_synopsis --page`'s PDF (`synopsis_pdf_path`) and `gauges/feed/publish.py`'s fetch_data dir (REPO_ROOT).
  `proposals.side_dir` now uses a new `generated.side` key and gives the same string as before. The planted forms
  are also tested in nested folders.
