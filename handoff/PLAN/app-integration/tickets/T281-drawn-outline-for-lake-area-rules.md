# T281 — Draw the area of a lake an in-part rule holds on (François "map 1", Shuswap A/B/C, Mahood, Canim)

Status: POSTPONED (deferred) (waits on lake splits — owner 2026-10-28, Q142, memory `lake-splits-deferred`).

Source: T208's last work item ("later a drawn outline when lake splits land").

## Work
When lake splits exist: split each mapped lake area (T208 Outcome 3 list: François r1, Shuswap r1-r5/r5x/r11x,
Mahood r1-r8, Canim r1, and the outlet/inlet-only lakes) so the rule binds its area, then drop its `not_yet_mapped`
tag; the app's area tag becomes an ordinary drawn part.

## Acceptance
Each listed rule binds a drawn part; no `not_yet_mapped` left on a mapped area; binding diff reviewed.

Depends: lake splits. Size: M. Owner: lift the deferral.

## Worklist (moved here by T324, 2026-11-05)
`T281-files/sub_lake_areas.json` — the worklist for splitting a lake into the waters the book treats it as (was
`data/curated/waters/sub_lake_areas.json`; no build reader, so it left curated). Its `_` text is partly stale (see T100).
