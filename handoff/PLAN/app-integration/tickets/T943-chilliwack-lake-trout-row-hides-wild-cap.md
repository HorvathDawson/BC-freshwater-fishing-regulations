# T943 — Chilliwack Lake: the lake trout row shows no wild cap, so it never says a wild one can't be kept

Status: OPEN.

Source: lane/t936-t931 (T931 group A, seen while re-pinning `test_t810_cutover`), 2026-11-20.

## What
Under RULINGS C2 amended 2026-11-18 (Chilliwack Lake), "No wild trout over 50 cm, 1 bull trout over 60 cm" leaves lake
trout inside the trout line, so a wild lake trout over 50 cm goes back; Region 2's "1 char, none under 60 cm" also
applies. Together a wild lake trout can never be kept. The trout table's lake trout row reads "60 cm (24 in) or longer"
with no wild cap, so it does not say so.

## Evidence
Live answers r-6258ffded676, Chilliwack Lake trout table, lake trout row. The hatchery/wild split pin in
`test_t810_cutover.py::test_hatchery_and_wild_twins_are_one_row` leaves LT out until this is fixed.

## Work
Check the trout table's composition for a char whose two size rules leave no keepable length for wild fish; the row
should say wild ones go back. Pin Chilliwack Lake LT whole.
Depends: none. Size: S. Owner: no (C2 as amended).
