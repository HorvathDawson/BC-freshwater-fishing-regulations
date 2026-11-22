# T1000 — Tiles: an Adskwatim Creek stream polygon is drawn in the lake layer but missing from the stream layer

Status: OPEN (found by the T931 review, 2026-11-22; two live-tier tests red on it).

Source: T931 review on `test/T931-live-tier`, against LATEST r-741815c6090d. `pipeline/tests/test_one_water_kind.py:229`
and `:453` fail. Section 1544569 is a stream polygon of Adskwatim Creek (`gnis:7384`, `lake:329268440`). The tile
build of 2026-11-20 16:53 (`4ae7b62955bc9990.fc51e82f7c91`) draws it in the `lake` layer with `water: stream`, and not
in the `stream` layer. The tiles and the bundle share registry 7caa748d, so this is not a mismatched build.

## To check first (2026-11-22)
The T777 review ran `test_one_water_kind.py` on main against the same LATEST and saw 0 failures, where the T931 review
saw 2. Re-run both tests on main with the live data before working on the export: the gap may be an artifact of the T931
reviewer's worktree links.

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
