# T1210 — Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river)

Status: IN PROGRESS — lane T1208 (worktree wt-T1210p); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1210 | Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river) | IN PROGRESS — 0.10.0 re-cut | 2c007fcda, 6bdff675d | the pipeline half proven (below), on feat/T1210-tile-label-layers; owed: the PR, then the re-cut's tiles build; the app half rides look A (T1208) |

Source: T1200 (owner 2026-11-26). Two PRs: this pipeline half (`feat/T1210-tile-label-layers`), so the re-cut's tiles
carry the layers while the look waits on the owner; the style that draws them rides look A (T1208).

## Work

1. `stream_name` from z4 (`NAME_BANDS` 4-7; their own tippecanoe pass, `ALONE_SPLIT`, so z8+ are cut as before).
2. `lake_name`, a decorative point per lake water from the written `lake` file: inside its biggest piece, `name` and
   `area_m2`, first drawn at the later of the lake's first zoom and its area rank's (`LAKE_NAME_RANK_ZOOM`); split parts
   named as one (`LAKE_NAME_JOINS_PARTS`, one switch: lake parts are design-only); its pass keeps every point.
3. Today's style reads no `lake_name` and draws `stream_name` from z8: the pipeline half alone changes no look.

## The rebuild (the re-cut's data build; not run by the lane)

The tiles key holds the stage's code (`build.py` `tiles_key`): the re-cut's `pipeline build` reruns it. Quicker, same
vintage: `scripts/heavy.sh tiles 30 -- .venv/bin/python -m pipeline.deliver.tiles --build data/generated/atlas/full
--tiles-only`, then the release as usual.

## The proof (2026-11-27; scratch builds of the same layer files)

origin/main's stage rebuilt the live atlas byte for byte; against it this branch keeps every other layer and
`stream_name` z8+ identical (117/117 zoom/layer pairs) and adds 93,629 bytes (z4-z7 only); two builds, the same bytes;
the map's pixels at z4, z6.3, z7 and z10.5: 0 differing.

Acceptance: unit tests on small layer files; the tile contract regenerated. Depends: the re-cut's data build. Size: M.
Owner: no.
