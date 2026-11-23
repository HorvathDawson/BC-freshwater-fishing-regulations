# T1000 — Tiles: an Adskwatim Creek stream polygon is drawn in the lake layer but missing from the stream layer

Status: IN PROGRESS — fix built (lane L6, 2026-11-22: `export_waterbodies` draws nothing outside B.C.); left (Claude): run `test_one_water_kind.py -m ""` on the 0.9.0 release's tile build (needs that data release).

Source: T931 review on `test/T931-live-tier`, against LATEST r-741815c6090d. `pipeline/tests/test_one_water_kind.py:229`
and `:453` fail. Section 1544569 is a stream polygon of Adskwatim Creek (`gnis:7384`, `lake:329268440`). The tile
build of 2026-11-20 16:53 (`4ae7b62955bc9990.fc51e82f7c91`) draws it in the `lake` layer with `water: stream`, and not
in the `stream` layer. The tiles and the bundle share registry 7caa748d, so this is not a mismatched build.

## Confirmed (2026-11-22)
Re-run on main with the live data, slow tests included (`-m ""`): both fail (`test_one_water_kind`,
`test_tile_stream_polygon_draws_with_its_river`). The T777 review's "0 failures" ran without the slow tests.

## Work
1. Find why the export puts a stream-kind polygon that carries a `lake:` id into the lake layer only
   (`pipeline/deliver/tiles/export.py`: layer choice by id prefix vs by kind). List every other section with the same
   shape (stream kind, lake id), not just this one.
2. Fix the layer choice so one water draws in one layer by its kind (AGENTS: one water, one kind). A test on a small
   fixture pins it; the two live tests go green on the next tile build.
3. Re-run the tiles in the next release's build (no tile build in this chat).

## Acceptance
`test_one_water_kind.py` passes on the live tier after the next tile build. The fixture test fails when the layer
choice reverts.

Depends: next release build (0.8.6). Size: S–M. Owner: no.

## Cause and fix (lane L6, 2026-11-22)
- **Cause: not the layer choice, the outside-B.C. rule.** `lake:329268440` is stamped `out_of_bc` (T679 #1's one
  outside flag: it is in no region polygon; its outline sits on the B.C./Alberta line at 120° W, 57.19° N).
  `export_streams` drops every `out_of_bc` node ("OUTSIDE BC IS NOT DRAWN AT ALL"), so the creek's route through it was
  never written; `export_waterbodies` had no such check and drew the polygon in `lake` with `water: stream`.
- **Every section of the same shape** (probe on the live atlas `full`): stream-owned polygons outside B.C.: **1** (this
  one, itself a lake-kind polygon). Lake / wetland polygons outside B.C.: 145 (71 lake-kind polygons INCLUDING Adskwatim:
  70 border slivers + Adskwatim; 74 wetlands; the 70 slivers and the wetlands unnamed, the biggest 3.7 ha, in no item
  but Adskwatim's). The 74 wetlands were already undrawn (T64: unnamed, in no item); the **71 lake-kind polygons (70 slivers + Adskwatim) were drawn**,
  tappable and ruleless — the border slivers T679 #1 meant to take off the map.
- **Fix:** `export_waterbodies` skips an `out_of_bc` node (after the T64 wetland cut, so `undrawn.json` is unchanged),
  counts and prints it. One rule for both exporters: an outside node is in no water layer.
- **Tests:** `test_tile_outside_bc.py` (invented atlas: a creek, its polygon in and out of B.C., a plain lake each side;
  the two exporters run for real) — passes; FAILS with the check reverted (mutation run 2026-11-22).
  `test_one_water_kind.py`: the two tests leave out `out_of_bc` polygons as they leave out tidal ones (both pass on
  today's tiles), and the new `test_nothing_outside_bc_is_drawn` is the live gate: today it fails with exactly 71
  sections (70 slivers + Adskwatim's), green once the 0.9.0 release rebuilds the tiles.
- **Needs the data release**: the tiles change (71 polygons off the map); no rule binding moves (the reach and bundle
  already read these sections as outside).
