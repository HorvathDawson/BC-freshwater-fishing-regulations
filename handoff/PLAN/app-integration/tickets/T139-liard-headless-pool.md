# T139 — A third headless one-kind pool appeared: Liard River watershed (r7, 7-53)

Status: OPEN — found 2026-10-23 by the guard fix (1ac96d72).

Source: `pipeline/tests/test_answers_one_kind_group.py::test_every_shipped_headless_water_is_drawn_but_the_two_open_shapes`
(needs_bundle) fails on the current bundle: besides the two known open shapes (Duncan DV, z8 r5) a third headless pool is
shipped — `r7:liard_river_watershed_see_map_on_page_63@7-53::liard_river_watershed.r2`.

## Work
Find which change made the Liard pool headless (T80 one-kind groups, A_ONE_KIND_GROUP_ONLY_ON_A_DRAWN_ROW, T104/T128
wording, or a data change), decide whether it is a data/ruling defect or a third legitimate open shape, fix at the cause
(or add it to the known shapes with the reason), and keep the card gate at 0 problems.

## Acceptance
The needs_bundle test passes on a fresh deliver; the Liard card reads correctly (name its lines before/after).

Depends: none. Size: S. Owner: no.
