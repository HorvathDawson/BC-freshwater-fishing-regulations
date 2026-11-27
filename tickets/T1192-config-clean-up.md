# T1192 — config.yaml clean-up: paths only, the file list in the schema (Option A)

Status: IN PROGRESS — lane: the repo clean chat; PR 1 (comments into the schema's field notes) first.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1192 | config.yaml clean-up: paths only, the file list in the schema (Option A) | IN PROGRESS — the repo clean chat | — | left (Claude): PR 4 (one `roots:` block, two keys renamed, unlisted files declared) and PR 5 (a stage keyed on the config keys it reads) in review; then PRs 6-7 below, in order |

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
   PR 2 (done on its branch): `curated.Config` covers all six blocks, `parse_config` is the one load path (`project_config`
   a view on it, `get_path(default=)` gone); the `null` `stocking` / `bathymetry` blocks are deleted rather than
   declared (no such file exists yet; `bathymetry/accept.py` `SETS` names them; PR 4 declares them when they ship).
3. One `source:` block (the FWA gpkg has three names today); the fetcher writes through it.
   PR 3 (merged): one `source:` block (`fwa_gpkg`, `coast_gpkg`, `synopsis_pdf`, `fetch_temp`; `curated.Source`) read as
   `SOURCE.<name>` by every reader and the fetcher; the five `project_config` accessors retired; `check_data_paths.py`
   refuses the gpkg names in code. A read records `source.<name>` + its root, per leaf.
4. Relative paths under `roots`; rename `waters.areas` → `area_closures`, `waters.tidal` → `tidal_limits`; declare the
   unlisted curated files (`closed_areas.json`, `owner_carries.json`, `recheck.json`, bathymetry, stocking) and the
   generated folders; `release` becomes a root. [x] (From PR 2's review) Point `check_curated`'s stocking / bathymetry
   entries at `pipeline/bathymetry/accept.py`'s frozen paths (`frozen_path`), not `None`: today's report says "not
   declared in config.yaml", which is awkward once they are declared.
   PR 4 (done on its branch): one `roots:` block (source, curated, generated, release, cache); every other path is a
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
5. Per-leaf C lines. PR 5 (done on its branch): a read records its key + root per access (`curated._Tree`); a whole node
   taken (`model_dump`, iteration) records the node. [ ] Import-time constants (`gauges.matches.MATCH_FILE`) key every
   deliver step on `gauges.matches`. [ ] A guard: each curated key a stage reads has its content in that stage's key.
6. Review app: one whole-tree generated override; the per-file `CURATION_*` paths refused.
7. Option A: the file list moves into the schema; config.yaml about 12 lines. The file names live as schema-side names
   under the roots, NEVER as pydantic field defaults, so `test_no_field_has_a_default` (PR 2) stays as it is.

## Acceptance
Each PR proves, in its body: every resolved path (`CURATED`, `GENERATED`, `SOURCE`, every `get_config()` accessor)
identical before and after apart from the renames it lists; `curated_digest()` and the atlas input digests
(`python -m pipeline.atlas.inputs`) identical per file; a mini release (`pipeline/tools/mini_release.py`) with identical
bytes. PR 1 proves `yaml.safe_load` of the old and new config.yaml equal, and lists where every removed sentence lives.

Depends: none. Size: M. Owner: two small calls (below). Model: Opus.

## Owner

- [ ] 2026-11-26 — The existing `curated.py` field notes still carry ticket ids and dates (T226, T322, T1021, P08/P18,
  "owner 2026-11-13", …). Strip them too, git keeping the record (A), or leave them (B)? Default: B; PR 1 moved
  config.yaml's facts without them and left the existing notes alone.
- [x] 2026-11-26 — Two existing notes are stale: the `Waters` docstring's reason for keeping `tidal` / `areas`
  (also in `data/curated/waters/README.md`), and the `Domain` docstring "Stocking and bathymetry are named before they exist"
  (`pipeline/bathymetry/accept.py` writes their files beside these keys). Fix them in PR 2 (A) or leave them (B)?
  Default: A, with the renames (PR 4) and the null placeholders (PR 2).
