# T1192 — config.yaml clean-up: paths only, the file list in the schema (Option A)

Status: IN PROGRESS — lane: the repo clean chat; PRs 1-5 and 7-9 merged; PR 6 up (#367, under review).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1192 | config.yaml clean-up: paths only, the file list in the schema (Option A) | IN PROGRESS — the repo clean chat | — | left (Claude): PR 6's merge (#367: one whole-tree GENERATED_ROOT, the per-file CURATION_* paths refused); then only the owner lines and the open [ ] finds |

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
   PR 2 (merged): `curated.Config` covers all six blocks, `parse_config` is the one load path (`project_config` a view
   on it, `get_path(default=)` gone); the `null` `stocking` / `bathymetry` blocks deleted (PR 4 declares them).
3. One `source:` block (the FWA gpkg has three names today); the fetcher writes through it.
   PR 3 (merged): one `source:` block (`fwa_gpkg`, `coast_gpkg`, `synopsis_pdf`, `fetch_temp`; `curated.Source`) read as
   `SOURCE.<name>` by every reader and the fetcher; the five `project_config` accessors retired; `check_data_paths.py`
   refuses the gpkg names in code. A read records `source.<name>` + its root, per leaf.
4. Relative paths under `roots`; rename `waters.areas` → `area_closures`, `waters.tidal` → `tidal_limits`; declare the
   unlisted curated files (`closed_areas.json`, `owner_carries.json`, `recheck.json`, bathymetry, stocking) and the
   generated folders; `release` becomes a root. [x] (PR 2's review) `check_curated`'s stocking / bathymetry entries
   read `pipeline/bathymetry/accept.py`'s frozen paths (`frozen_path`), not `None`.
   PR 4 (merged): one `roots:` block (source, curated, generated, release, cache); every other path is a
   name under the root of its block (`curated.TREES`), refused when absolute or climbing out; `data_tree`,
   `curated.base`, `generated.base`, `generated.release` and their cross-check gone; `CURATED_ROOT` moves the one
   curated root. `waters.areas` -> `area_closures`, `waters.tidal` -> `tidal_limits`; the atlas record keeps its input
   names (`atlas.inputs.RECORD_NAME_OF`). Declared: curated `dfo_salmon.closed_areas`, `review.owner_carries`,
   `review.recheck`, `bathymetry.{matches,contour_matches}`, `stocking.matches` (each "may be absent"); generated
   `bathymetry`, `stocking`, `debug`, `synopsis`, `steelhead`, `curated_writes`, `regs.export`, `gauges.hydat_daily`.
   Left (found, not moved): `GENERATED.bundle.parent / …` (build-manifest.json, build-logs/, deliver-cache/,
   generated `release/` + `release/checks`: `pipeline/build.py`, `pipeline/deliver/__main__.py`, `pipeline/release.py`)
   and `GENERATED.regs.extraction.parent` (`passage_boxes.py`) still find a generated folder beside another key.
   - [ ] Gap (spin-off 3): `dfo_salmon.closed_areas` is declared but kept out of `build.curated_digest`
     (`NOT_DELIVER_INPUTS`), as before: deliver reads it but is covered only because deliver depends on reach.
   - [x] (review) `check_curated`'s bathymetry entry reads the accept's shape (`matches.chart_id`, the sheet code the
     roster's `waterbody_identifier` is). [ ] Its stocking entry is back to `frozen=None`: the accept writes
     `matches[].chart_id` = FIDQ `wbody_id`, but no fetcher writes `bc_stocked_waterbodies.json`, so its
     `waterbody_id` is not shown to be that id; wire it with the roster's writer.
   - [ ] Generated root + literal writers still undeclared: `build-manifest.json` and `build-logs/` (`pipeline/build.py`),
     `deliver-cache/` (`pipeline/deliver/__main__.py`, `pipeline/tools/mini_release.py`), generated `release/` and
     `release/checks` (`pipeline/release.py`, `pipeline/deliver/__main__.py`).
   - [ ] Folders on disk under `data/generated/` with no declared key: `municipal/`, `owner1028/`, `.trace-reach-*`
     (writers not traced).
   - [ ] `closed_areas.json` absent reads as no closed areas (unchanged behaviour; a later guard could raise when the
     marks hold a verified closed area and the file is missing).
5. Per-leaf C lines. PR 5 (merged): a read records its key + root per access (`curated._Tree`); a whole node taken
   (`model_dump`, iteration) records the node. [ ] Import-time constants (`gauges.matches.MATCH_FILE`) key every deliver
   step on `gauges.matches`. [ ] A `test_stage_content_keys`-style pin: each curated / generated read is in its stage key.
6. Review app: one whole-tree generated override; the per-file `CURATION_*` paths refused. PR 6 (#367): `GENERATED_ROOT`
   moves every `GENERATED` path, honoured only by the review backend and tests; 19 per-file overrides refused; kept:
   the served vintage (5) and 3 output paths. [ ] A side `CURATION_BUILD` should require `_REACH_DIR` and `_BUNDLE` too.
   [ ] Found, not fixed (a missing input reads as nothing): backend `dfo.py` 123-166, 1067, 1078, `preverify.py` 43-47,
   `awaiting_water.py` 31-35; `regs/review/tidal.py` 400, `proposals.py` 1781.
7. Option A: the file list moves into the schema; config.yaml about 12 lines. PR 7 (merged, #358): config.yaml
   = `roots:`, `source.fwa_gpkg`, `atlas_build` (was `generated.atlas.default_build`); every other name is an `At` on its
   field, added to the parsed file by `curated.read_config` (the loaders' and `config_now`'s one source; a name stated in
   both is refused). A rename is a curated.py edit: every stage reruns (its bytes are in every code key).
   Left / found, not fixed: [ ] each file's `kind` (authored / promoted / applied / reviewed) as schema data on `At`,
   read by one consumer. [ ] Retire `project_config.py` (`project_root`, `dfo_salmon_cache`, `get_path`; 6 importers +
   CI path lists). [x] PR 9: the stale docs, the unknown-key hint, empty / null blocks, `atlas_build`, every `At` pinned.

## Acceptance
Each PR proves, in its body: every resolved path (`CURATED`, `GENERATED`, `SOURCE`, every `get_config()` accessor)
identical before and after apart from the renames it lists; `curated_digest()` and the atlas input digests
(`python -m pipeline.atlas.inputs`) identical per file; a mini release (`pipeline/tools/mini_release.py`) with identical
bytes. PR 1 proves `yaml.safe_load` of the old and new config.yaml equal, and lists where every removed sentence lives.

Depends: none. Size: M. Owner: two small calls (below). Model: Opus.

## Owner

- [x] 2026-11-26 — Strip the existing notes' ticket ids and dates too: "Yes, strip them too (Recommended)" (PR 8);
  the two stale notes were fixed with the renames (PR 4).
- [x] 2026-11-27 — AGENTS rule 36 said "All paths come from config.yaml"; since #358 only the roots, the FWA gpkg name
  and `atlas_build` do (every other name: `pipeline/common/curated.py`) → "Reword as proposed (Recommended)"
  (2026-11-27); the rule now names `pipeline.common.curated`.
- [ ] 2026-11-27 — The review tool's tunables (backups kept 500 / 60 days / 120 s burst, debounces): a `review_app:`
  block in config.yaml, or code constants (the table in tools/curation-review/API.md)? Default: kept as code constants.
