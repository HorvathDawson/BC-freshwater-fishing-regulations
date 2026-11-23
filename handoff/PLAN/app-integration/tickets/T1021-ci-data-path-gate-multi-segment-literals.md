# T1021 — The CI "no hard-coded data path" gate misses every multi-segment literal (T994 finding 7)

Status: OPEN.

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
