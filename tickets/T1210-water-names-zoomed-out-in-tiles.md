# T1210 — Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river)

Status: DONE — 2c007fcda, 6bdff675d, 1f991e377, a7249b62a, 00d2d0906 (the tiles half merged in #356; the app's label style is here; the names show once the 0.10.0 re-cut's tiles build has run).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1210 | Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river) | DONE | 2c007fcda, 6bdff675d, 1f991e377, a7249b62a, 00d2d0906 | — |

Source: T1200 (owner 2026-11-26: "build A ..."). The prototype made these at run time from the drawn tiles; the build
puts them in the tiles. TWO PRS (planning chat 2026-11-26): the pipeline half on `feat/T1210-tile-label-layers`, so the
re-cut's tiles build carries the layers while the look waits on the owner (they draw nothing until the style asks: the
proof is in this PR); the app half on the look A branch (`feat/T1208-map-look-A`, T1208).

## Work

1. Pipeline (`pipeline/deliver/tiles`): `stream_name` from z4 (name bands 4-7 added to `NAME_BANDS`: the same merge,
   simplify and Chaikin rounding per band); a new decorative point layer `lake_name`, derived from the written `lake`
   layer file alone: one point per lake water (a split lake's parts are one water, `LAKE_NAME_JOINS_PARTS`: one switch,
   since lake parts are design-only), inside its biggest piece (pole of inaccessibility), `name` and `area_m2` (the
   parts' sum), first drawn at the later of the lake's own first zoom and its area rank's (`LAKE_NAME_RANK_ZOOM`); its
   pass keeps every point (`--drop-rate=1`, that pass only). Pure, sorted, rounded: two runs write the same bytes.
2. App style (the second PR, after look A): river names along `stream_name` from z4, lake names from `lake_name` below z8,
   biggest first.
3. The pipeline half alone changes no look: today's style reads no `lake_name` and draws `stream_name` from z8 as
   before (the proof build in this PR: every other layer's tiles byte for byte, and the map's pixels).

## The rebuild (the 0.10.0 re-cut's data build; not run by the lane)

The tiles stage's key holds its code (`build.py` `tiles_key`): the re-cut's `pipeline build` reruns it. Quicker, same vintage:
`scripts/heavy.sh tiles 30 -- .venv/bin/python -m pipeline.deliver.tiles --build data/generated/atlas/full --tiles-only`
(re-derives `stream_name` and `lake_name` from the layer files on disk, then tippecanoe), then the release as usual
(`python -m pipeline release --staging`). Until the served tiles carry the layers, the app draws no new names: today's map.

## The proof (2026-11-27; the same layer files, scratch builds, `--tiles-only --no-release`)

origin/main's tiles stage rebuilt the live atlas byte for byte. This branch's, against it: every other layer and
`stream_name` from z8 identical tile for tile (117 of 117 zoom/layer pairs; z8-z13 the same bytes); added only
`stream_name` z4-z7 (1.5 / 6.3 / 35.2 / 144.1 KB) and `lake_name` z4-z7 (0.5 / 1.4 / 3.7 / 18.1 KB): 197,744,622 ->
197,838,251 bytes. Two builds of the branch: the same bytes. Today's style reads no `lake_name` and starts `stream__label`
at z8, so the map's pixels at z4, z6.3, z7 and z10.5 are identical (0 differing).

## Acceptance

Unit tests on small layer files; the tile contract regenerated; the re-cut's data build runs the tiles stage (one vintage).

Depends: the re-cut's data build. Size: M. Owner: no.
