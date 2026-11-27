# T1192 — config.yaml clean-up: paths only, the file list in the schema (Option A)

Status: IN PROGRESS — lane: the repo clean chat; PR 1 (comments into the schema's field notes) first.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1192 | config.yaml clean-up: paths only, the file list in the schema (Option A) | IN PROGRESS — the repo clean chat | — | PR 1 (comments move into curated.py, config.yaml stripped), then PRs 2-7 of the plan below, in order |

Source: the owner, 2026-11-26 (planning chat), verbatim: "the config.yaml i feel has a lot of stuff in it that is noit
needed too? like there is a lot of bloat because of the way stuff is formatted could we audit this and maybe use it as
a way to spin off more restructuring of ourr data etc? we want everything to be extremely clean". The audit:
`~/.claude/projects/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/night-notes-2026-11-26/config-audit.md`
(244 lines, 75 keys, 74% comments; 3 dead keys, 2 null placeholders, 4 duplicates, 1 real setting).

## Owner's decisions (2026-11-26, verbatim picks)
1. Shape: "Option A: about 12 lines (Recommended)": config.yaml keeps the roots and the one setting; the file list
   lives only in the schema (`pipeline/common/curated.py`).
2. Timing: "Yes, before the re-cut build (Recommended)": the PRs that change build keys ride the 0.10.0 re-cut rebuild.
3. Comments: "Move each unique fact into the schema's field notes, then strip (Recommended)". Ticket ids and dates stay
   in git history only.
4. Spin-offs: "A dedicated clean-up chat" (this lane; T1193 is its other ticket).

## Work (the audit's plan, one PR each, in this order)
1. Comments: each unique fact moves into its field's `#:` note in `curated.py` (the `data` / `data_accessor` / `cache`
   blocks: the `project_config.py` accessor); duplicates and stale notes are dropped; config.yaml keeps keys, values
   and a short header. No key or value changes.
2. Loud validation: `extra="forbid"` at every level, a top-level schema, no defaults, no `null`; delete `data.base`
   and the two null `match_calls`; an unknown or missing key fails naming its dotted key.
3. One `source:` block (the FWA gpkg has three names today); the fetcher writes through it.
4. Relative paths under `roots`; rename `waters.areas` → `area_closures`, `waters.tidal` → `tidal_limits`; declare the
   unlisted curated files (`closed_areas.json`, `owner_carries.json`, `recheck.json`, bathymetry, stocking) and the
   generated folders; `release` becomes a root.
5. Per-leaf C lines (a stage keys on the config leaves it reads, not the whole block).
6. Review app: one whole-tree generated override; the per-file `CURATION_*` paths refused.
7. Option A: the file list moves into the schema; config.yaml about 12 lines.

## Acceptance
Each PR proves, in its body: every resolved path (`CURATED`, `GENERATED`, `SOURCE`, every `get_config()` accessor)
identical before and after apart from the renames it lists; `curated_digest()` and the atlas input digests
(`python -m pipeline.atlas.inputs`) identical per file; a mini release (`pipeline/tools/mini_release.py`) with identical
bytes. PR 1 proves `yaml.safe_load` of the old and new config.yaml equal, and lists where every removed sentence lives.

Depends: none. Size: M. Owner: no (decided above). Model: Opus.
