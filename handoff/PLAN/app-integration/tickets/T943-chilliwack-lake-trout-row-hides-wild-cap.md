# T943 — Chilliwack Lake: the lake trout row shows no wild cap, so it never says a wild one can't be kept

Status: DONE — 10ddff399; shipped in 0.8.4 (cut e769f710a, data r-fec3fab4c5aa; 0.9.0 audit 2026-11-22, every hash is an ancestor of origin/main and of e769f710a). (Earlier status: IN PROGRESS — lane/lb-rules-leftovers done; left (Claude): main chat review + merge.)

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

## Result (lane/lb-rules-leftovers, 2026-11-20)
No defect, data or display. The engine releases wild lake trout (won by `chilliwack_lake.r1`) and keeps hatchery lake
trout at 60 cm or longer; both the live release r-6258ffded676 and the current build show the lake trout row labelled
"Hatchery only" with the sheet line "Hatchery only: wild ones go back." T931 looked only at `lines[0]`.
- Pin: lake trout whole in `test_t810_cutover.py::test_hatchery_and_wild_twins_are_one_row` (label, line, sheet, the
  engine's statuses and winning rules); passes. The side deliver changes nothing on Chilliwack Lake.
- Shots: `T943-shots/chilliwack-lake-jul15-before.png`, `-after.png` (identical; the "HATCHERY ONLY" label).
