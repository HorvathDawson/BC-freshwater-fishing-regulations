# T942 — Kitimat: the "2 over 50 cm, only 1 not a hatchery steelhead" rule is enforced but no longer shown

Status: IN PROGRESS — lane/lb-rules-leftovers done; left (Claude): main chat review + merge.

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

## Result (lane/lb-rules-leftovers, 2026-11-20)
Cause: T854 put lake trout under r4's 1-from-streams, which made r2's cap look moot; the raised clause was then also
taken as said by the share count, and the steelhead sit in the river's own tier, which is never split.
- `rows/conds.py` `moot_cap`: a cap a water's quota raises (`_raise_of`, rows F17) is never moot.
- `card.py`: a raised clause is not "said by" the share count, and it binds its kinds.
- `tree.py`: when the steelhead sit in the water's own tier, the size tier holds the clause's kinds and still says the
  "+1"; the room is the tally's and the steelhead's own number.
- `card_check.py` (ROUND19 3 placement): a capped kind outside a size tier is accepted only when the tier's `extra`
  names it AND it sits in an own tier (the extra is still proved against the engine). Pinned by
  `test_the_card_gate_takes_the_kitimats_plus_one_said_from_outside_its_own_tier_and_nothing_else` with two mutation
  pins (steelhead not named by the extra; steelhead moved out of the own tier), and it fails with the exemption off.
- Kitimat Aug 15: the card's size tier reads "Only 1 of all your trout and char may be over 50 cm (20 in) — not 1 per
  species. 2 if one of them is a hatchery steelhead."; the trout table strip reads "only 1 over 50 cm (20 in), 2 if one
  is a hatchery steelhead" (plus a sheet line on the EB, GB, CT, LT, ST rows; RB rows unchanged).
- Pins: the two Aug 15 pins are back exactly as they were. The December half of the tally pin is re-pinned to "no
  pool row, no cap" (since T854 every fish of the pool goes back from streams Nov 1–Jun 30), reason in the comment.
- Province-wide change list (answers_diff r-39e0f6ef6117 -> r-b2cf828d8f11): 4,742 of 1,957,010 sections, Jul 1–Oct 31
  only, all on the Kitimat and 23 of its tributaries (Hirsch, Wedeene, Aveling, Little Wedeene, Lukes, Davies, Chist,
  Deception, Dahl, Humphrys, Hoult, Hunter, McKay, Nalbeelah, Raley, Tetlock, Bolton, Bowbyes, Buell, Cecil, Iron
  Mine, Lone Wolf, McCullough creeks/rivers; 4,555 unnamed stretches): the pool's `conds` cap and the card tree. One
  keeping-law change, errs closed, Kitimat River 28 sections: 1 lake trout + 2 hatchery steelhead over 50 cm together
  allowed -> not allowed (the ruling). Trout tables: 4 answer keys change, the same 24 waters (the strip above). No
  other water is hit by the own-tier tree change. Card gate 0 problems, trout check 0 problems, both builds.
- Tests: the related answers suite (test_answers_*, c2, t183, t834, trout_check, t810, reader_answers) 1557 passed + 1
  failure fixed by the gate change above, then re-run green.
- Trout fixture: not refreshed. `python -m pipeline.tools.emit --check` is clean (the vectors decode the frozen
  fixture), and the committed fixture already differs from a fresh cut of base 52f647546 (section ids +2 throughout),
  so a refresh here would carry unrelated drift. The kitimat-jul15 / humphrys-jul15 cases pick up the strip at the
  next `scripts/trout-refresh.sh`.
- Shots: `T942-shots/kitimat-aug15-before.png`, `kitimat-aug15-after.png`.
