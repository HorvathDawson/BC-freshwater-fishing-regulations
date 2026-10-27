# T204 — a part's separate stretches and every place two parts meet (T203 items 4–5)

Status: DONE (9e0960ec; built in r-8b43fb267199)

Source: T203 lane 2026-10-25. Owner: non-adjacent stretches of one part are SEPARATE stretches on the map (own label,
own selection); Kanaka Creek's end labels are wrong.

## Cause
The bundle makes ONE `part_mark` per pair of parts. Kanaka part 2 meets part 1 at two places (112 Ave bridge and Cliff
Park Falls) and holds only the falls mark, so the falls label lands on the bridge. Chilliwack works because its parts
form a chain (each pair meets once). The release carries no section adjacency, so the app cannot derive stretches.

## Work (pipeline + bundle + app; needs a deliver rebuild)
- Deliver: a part_mark per MEETING PLACE (not per pair), and each part's stretches (connected runs of its sections,
  from the reach graph's adjacency) with their own end marks.
- App: draw/label/select each stretch separately; the parts sheet lists "Two stretches" as two rows.
- Tests on Kanaka (2 places) and Chilliwack (chain, unchanged); request/size budget check (T182 gates).

## Outcome (2026-10-27, 9e0960ec; release r-8b43fb267199)
- **Stretches** (`topology.stretches`): a part's main-stem runs, mouth up, merged unless ANOTHER PART of the same water
  has a main-stem section in the gap (a lake or unnamed piece between two runs does not split them); side-channel runs
  join the stretch they re-enter. 610 `part_stretch` rows (geo file) for the waters that have a part of 2+ stretches;
  none elsewhere.
- **Marks per meeting place**: marks are made per pair of STRETCHES of different parts (`part_mark.down_stretch` /
  `up_stretches`, NULL when all are stretch 0). Kanaka (gnis:11318): "112 Avenue bridge" (part 0 stretch 0 below part 1)
  and "Cliff Park Falls" (part 1 below part 0 stretch 1); the old single mark had the falls' label at the bridge's point.
  Chilliwack (a chain): the same three marks, no stretch columns, no stretch rows. 969 marks in 603 waters.
- **App**: `waterGroup` walks one entry per stretch (own sids, box, midpoint; name "… · stretch k of n"); the parts list
  shows each stretch as a row; a step between two stretches of one part keeps the frame's part, and a step onto another
  part's later stretch reopens there. Tests: `test_part_geo.py` (pure + Kanaka/Chilliwack on the bundle),
  `test_release.py` (geo e6), `waterNav.test.ts`, `navReads.test.ts`.
- **Sizes (T182)**: geo 578,909 → 612,854 B gz, water 2,159,412 → 2,162,883 B gz; the release's gates passed.
- Not checked by eye on a phone.
