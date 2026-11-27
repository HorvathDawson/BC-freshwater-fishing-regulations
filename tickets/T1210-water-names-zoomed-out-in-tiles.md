# T1210 — Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river)

Status: IN PROGRESS — lane T1208 (worktree wt-T1210p); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1210 | Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river) | IN PROGRESS — 0.10.0 re-cut | — | the pipeline half on feat/T1210-tile-label-layers (its own PR, ahead of the look); the app half (the style that draws the names) waits on look A (feat/T1210-water-names-in-tiles, after T1208) |

Source: T1200 (owner 2026-11-26: "build A ..."). The prototype made these at run time from the drawn tiles; the build
puts them in the tiles. TWO PRS (planning chat 2026-11-26): the pipeline half on `feat/T1210-tile-label-layers`, so the
re-cut's tiles build carries the layers while the look waits on the owner (they draw nothing until the style asks: the
proof is in this PR); the app half on `feat/T1210-water-names-in-tiles`, after look A (T1208).

## Work

1. Pipeline (`pipeline/deliver/tiles`): `stream_name` from z4 (name bands 4-7 added to `NAME_BANDS`: the same merge,
   simplify and Chaikin rounding per band); a new decorative point layer `lake_name`, derived from the written `lake`
   layer file alone: one point per lake water (a split lake's parts are one water, `LAKE_NAME_JOINS_PARTS`: one switch,
   since lake parts are design-only), inside its biggest piece (pole of inaccessibility), `name` and `area_m2` (the
   parts' sum), first drawn at the later of the lake's own first zoom and its area rank's (`LAKE_NAME_RANK_ZOOM`); its
   pass keeps every point (`--drop-rate=1`, that pass only). Pure, sorted, rounded: two runs write the same bytes.
2. App style (the second PR, after look A): river names along `stream_name` from z4, lake names from `lake_name` below z8,
   biggest first.
3. The pipeline half alone changes no look: today's style reads no `lake_name` and draws `stream_name` from z5 as
   before (the proof build in this PR: every other layer's tiles byte for byte, and the map's pixels).

## The rebuild (the 0.10.0 re-cut's data build; not run by the lane)

The tiles stage's key includes its code, so the re-cut's `scripts/heavy.sh build 30 -- python -m pipeline build` reruns
it. The quicker equivalent on the atlas already built (the same vintage: tiles and bundle stay paired):
`scripts/heavy.sh tiles 30 -- .venv/bin/python -m pipeline.deliver.tiles --build data/generated/atlas/full --tiles-only`
(re-derives `stream_name` and `lake_name` from the layer files on disk, then tippecanoe), then the release as usual
(`python -m pipeline release --staging`). Until the served tiles carry the layers, the app draws no new names: today's map.

## Acceptance

Unit tests on small layer files; the tile contract regenerated; the 0.10.0 re-cut's data build runs the tiles stage
(tiles and bundle share one vintage).

Depends: the re-cut's data build. Size: M. Owner: no.
