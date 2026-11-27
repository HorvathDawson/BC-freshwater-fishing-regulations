# T1210 — Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river)

Status: IN PROGRESS — lane T1208 (worktree wt-T1208); release 0.10.0 re-cut

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1210 | Lake and river names zoomed out: a label layer in our tiles (one point per lake, one smoothed line per river) | IN PROGRESS — 0.10.0 re-cut | — | build in progress (lane T1208) |

Source: T1200 (owner 2026-11-26: "build A ..."). The prototype made these at run time from the drawn tiles; the build
puts them in the tiles. Branch `feat/T1210-water-names-in-tiles`.

## Work

1. Pipeline (`pipeline/deliver/tiles`): `stream_name` from z4 (name bands 4-7 added to `NAME_BANDS`: the same merge,
   simplify and Chaikin rounding per band); a new decorative point layer `lake_name`, derived from the written `lake`
   layer file alone: one point per lake water (a split lake's parts are one water), inside its biggest piece (pole of
   inaccessibility), `name` and `area_m2` (the parts' sum), first drawn at the lake's own first zoom. Pure, sorted,
   rounded: two runs write the same bytes.
2. App style: river names along `stream_name` from z4, lake names from `lake_name` below z8, biggest first.
3. Until the tiles carry them the new layers draw nothing: today's map.

## Acceptance

Unit tests on small layer files; the tile contract regenerated; the 0.10.0 re-cut's data build runs the tiles stage
(tiles and bundle share one vintage).

Depends: the re-cut's data build. Size: M. Owner: no.
