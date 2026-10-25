# T204 — a part's separate stretches and every place two parts meet (T203 items 4–5)

Status: OPEN

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
