# T1000 — Tiles: an Adskwatim Creek stream polygon is drawn in the lake layer but missing from the stream layer

Status: IN PROGRESS — fix built (lane L6, 2026-11-22: `export_waterbodies` draws nothing outside B.C.); left (Claude): run `test_one_water_kind.py -m ""` on the 0.9.0 release's tile build (needs that data release).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1000 | Tiles: an Adskwatim Creek stream polygon draws in the lake layer, not the stream layer (2 live tests red) | IN PROGRESS — fix 27c6812c8 (nothing outside B.C. drawn: 71 polygons off the map); left (Claude): the live gate on the 0.9.0 tile build | | T931 review 2026-11-22 |

Source: T931 review on `test/T931-live-tier`, against LATEST r-741815c6090d. `pipeline/tests/test_one_water_kind.py:229`
and `:453` fail. Section 1544569 is a stream polygon of Adskwatim Creek (`gnis:7384`, `lake:329268440`). The tile
build of 2026-11-20 16:53 (`4ae7b62955bc9990.fc51e82f7c91`) draws it in the `lake` layer with `water: stream`, and not
in the `stream` layer. The tiles and the bundle share registry 7caa748d, so this is not a mismatched build.

Confirmed 2026-11-22 on main with the slow tests (`-m ""`): `test_one_water_kind`, `test_tile_stream_polygon_draws_with_its_river` fail.

## Done (lane L6, 2026-11-22, 27c6812c8)
- Cause: not the layer choice, the outside-B.C. rule. `lake:329268440` is stamped `out_of_bc` (T679 #1; on the
  B.C./Alberta line at 120° W, 57.19° N); `export_streams` drops every `out_of_bc` node, `export_waterbodies` did not.
  Same shape on the live atlas: 71 lake-kind polygons outside B.C. were drawn (70 border slivers + Adskwatim), tappable
  and ruleless; the 74 wetlands were already undrawn (T64).
- Fix: `export_waterbodies` skips an `out_of_bc` node (after the T64 wetland cut, so `undrawn.json` is unchanged).
  Tests: `test_tile_outside_bc.py` (fails with the check reverted); `test_one_water_kind.py` leaves out `out_of_bc`
  polygons, and the new `test_nothing_outside_bc_is_drawn` is the live gate: today it fails with exactly 71 sections.
  No rule binding moves (the reach and bundle already read these sections as outside).

## Left (Claude)
- Run `test_one_water_kind.py -m ""` on the 0.9.0 release's tile build (needs that data release: 71 polygons off the map).

## Acceptance
`test_one_water_kind.py` passes on the live tier after the next tile build. The fixture test fails when the layer
choice reverts.

Depends: next release build (0.8.6). Size: S–M. Owner: no.
