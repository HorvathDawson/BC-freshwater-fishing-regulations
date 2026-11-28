# T1229 — Every curated file a stage reads has its content in that stage's key

Status: IN PROGRESS — lane `fix/T1229-content-in-every-stage-key` (stacked on #353); land before the 0.10.0 re-cut build

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1229 | Every curated file a stage reads has its content in that stage's key | IN PROGRESS — lane fix/T1229-content-in-every-stage-key | — | PR after #353 merges; land before the 0.10.0 re-cut build |

Source: the independent review of T1192 PR 5 (#353), 2026-11-27. A stage key holds the config KEY of each curated file a
stage reads (where it is); only a content part says what is IN it. Two reads had none; the guard found a third.

## The holes
1. tiles: `tiles.export` draws the `sign_zones` areas from `added_areas.geojson` (`area_closures.json` def
   `file: added_areas`, `area_splits._resolve_area_file`); `build.tiles_key` hashed only `area_closures.json`.
2. atlas: `registry.build.pinned_by_override` makes and names items from `book_name_links.json`; it was in neither
   `ATLAS_INPUTS` nor `NOT_ATLAS_INPUTS` (the coverage test skipped `pipeline/atlas/reach/`).
3. (found by the guard, net 2 over the live manifest's reach modules) reach: a confirmed closed area's new end binds
   once landed in `cuts.json` (`dfo_salmon.landed_resolver` -> `proposals.landed_cut`); the reach key held no `cuts`.

## Work
1. `build.curated_parts(stage)`: the content parts of each stage key, in one place; `tiles` gains `area_files`, every
   file an areas def names by `file` (through `_resolve_area_file`); `reach` gains `cuts` (`REACH_FILES`).
2. `curated:regulations.book_name_links` joins `ATLAS_INPUTS`; an atlas recorded before reads "recorded before this
   input was listed".
3. The guard, `test_stage_content_keys.py`: net 1, each curated leaf a stage records reading in the traced mini chain
   or its flag parse; net 2, each `CURATED.<a>.<b>` in the modules it imported. Each is content-keyed (the files
   `curated_parts` opens, traced; the atlas's `ATLAS_INPUTS`; a dependency's) or in `build.NOT_CONTENT_KEYED` /
   `NOT_ATLAS_INPUTS` with its reason.
4. #353 review LOWs: the PR 5 guard refuses `parse_config(` outside the loaders and `vars(` / `.__dict__` /
   `TypeAdapter` on the trees.

## Acceptance
The two holes pinned by mutation; the guard fails on a read neither keyed nor exempt; `curated_digest` and the path dump
unchanged; the mini release `--check` matches.

## Left
- `_trace.config_read` digests every access before asking for a recorder; the check cannot go first (a read before
  `install` is still written, `test_l1_…read_before_the_recorder…`). An identity memo per key would skip it: not done.
- Unsure, not checked one by one: the static import closure (function-level imports; an overapproximation, no gate)
  names more leaves for atlas / reach / tiles through review and bundle modules no recorded run imported.

## Not seen by the guard
- A read on a path neither the mini chain nor a flag parse takes, through a module neither imports.
- `generated.*` and `source.*` reads, and the `inseason` stage (made when missing, never by its key).

Depends: #353. Size: M. Owner: no.
