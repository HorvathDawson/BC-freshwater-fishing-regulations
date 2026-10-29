# T281 — Draw the area of a lake an in-part rule holds on (François "map 1", Shuswap A/B/C, Mahood, Canim)

Status: DEFERRED (waits on lake splits — owner 2026-10-28, Q142, memory `lake-splits-deferred`).

Source: T208's last work item ("later a drawn outline when lake splits land").

## Work
When lake splits exist: split each mapped lake area (T208 Outcome 3 list: François r1, Shuswap r1-r5/r5x/r11x,
Mahood r1-r8, Canim r1, and the outlet/inlet-only lakes) so the rule binds its area, then drop its `not_yet_mapped`
tag; the app's area tag becomes an ordinary drawn part.

## Acceptance
Each listed rule binds a drawn part; no `not_yet_mapped` left on a mapped area; binding diff reviewed.

Depends: lake splits. Size: M. Owner: lift the deferral.
