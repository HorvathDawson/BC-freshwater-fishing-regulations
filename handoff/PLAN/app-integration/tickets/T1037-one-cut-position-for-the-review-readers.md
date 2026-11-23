# T1037 — Two review readers pick a cut's position from `splits.resolved.json` by different rules, and neither is the build's

Status: OPEN — needs a decision on what "the cut's position" is for a reader with no extent (not small; no build output changes until then)

Source: independent review of PR #48 (T775 / T776 lane), 2026-11-23. Not changed in that PR: the brief said to change it only
if the build's own rule was clear and changing it moved no build output.

## What is wrong
A cut id has more than one row in `splits.resolved.json` when it lands on several lines or measures. Two readers keep one row:
- `pipeline/regs/review/audit.py` `primary_split_pos` (T776 item 1): the FIRST row with neither `concern` nor `picked_up`.
- `curation-review/backend/reuse.py` `host_rows` (the cut drawn on the map and its handle): the LAST row with no `concern`.

On the served atlas (`data/generated/atlas/full`, read 2026-11-23) 190 cut ids have several rows and the two readers disagree on
157 of them:
- 120: several blue lines, no `concern` / `picked_up` on any row (a cut across a braid): first row vs last row.
- 20: one line, two measures, no flags (an area boundary crossed twice, a lake-end alias): e.g. `adams_river__adams_lake`,
  `area:BEDNESTI LAKE ECOLOGICAL RESERVE`, `area:BOWSER ECOLOGICAL RESERVE`, `area:CINEMA BOG ECOLOGICAL RESERVE`.
- 17: a `picked_up` row (14 on several lines, 3 on one): e.g. `pennask_creek__pennask_lake` (blk 356344849) — audit 0.0,
  host 1,447.27 (`picked_up`).

## What the build does
The reach build never reads `splits.resolved.json`. `pipeline/atlas/reach/extent.py` `_cut_at` reads the graph's node bounds
carrying the cut: the PRINCIPAL line is the one with the most length in the extent's own universe (its item), and on that line
the measure nearest the mouth comes first with the others as alternatives; `resolve_extent` then takes the candidate nearest
the rule's other end (`AMBIGUOUS_CUT_NEAREST_THE_OTHER_END`). So the build's position of a cut DEPENDS ON THE EXTENT: there is
no one row to share, and a `picked_up` row is still a real place the cut stands (`sectionizer.place_points`).

## What to decide / build
- For each reader, what it needs: the audit reads a position per extent (an order or a length) and could call `_cut_at` with
  the extent's item universe — the build's own rule; the map draws the cut where it stands, which may be every row.
- Then one shared helper (or `_cut_at` itself) for both, a before/after of the audit's findings and the review map's cut
  layer, and a test on built data on the cases above.
