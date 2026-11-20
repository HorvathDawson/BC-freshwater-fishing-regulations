# T942 — Kitimat: the "2 over 50 cm, only 1 not a hatchery steelhead" rule is enforced but no longer shown

Status: OPEN.

Source: lane/t936-t931 (T931 group A), 2026-11-20. Content missing on screen.

## What
Owner ruling 2026-10-21 (b): on the Kitimat a day allows 2 fish over 50 cm, and only 1 of them may be anything other
than a hatchery steelhead. The card_check engine still enforces it (its `PLUS_ONE` basket check passes: 2 hatchery
steelhead plus a 55 cm cutthroat is illegal), but since the T854 / RULINGS C2 refinement the angler is not told: on
Aug 15 the Kitimat pool's `conds` caps are `[]`, the group tree has no size tier, and the Kitimat trout table says
nothing of 50 cm except in the steelhead subline.

## Evidence
`pipeline/tests/test_answers_kitimat_steelhead_lift.py::test_kitimat_pool_says_the_hatchery_steelhead_count_toward_a_2_over_50_tally`
and `::test_kitimat_hatchery_steelhead_are_plus_one_over_50_like_the_vedders` fail on r-6258ffded676
(`[] == [{'c': 'cap', … 'a': 50 …}]`; `gnis:3225`). T931 refused to re-pin them (left red).

## Work
Find where the T854/C2 change dropped the over-50 cap from the pool and trout table, put the words back on the card and
the trout table, keep the two pins as they are.
Depends: none. Size: S-M. Owner: no (the ruling stands).
