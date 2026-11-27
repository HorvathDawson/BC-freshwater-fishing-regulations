# T1241 — A side channel that answers like its river is not drawn below zoom 12

Status: OPEN — in the 0.10.0 re-cut (water option C and D share it).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1241 | A side channel that answers like its river is not drawn below zoom 12 | OPEN — in the 0.10.0 re-cut | — | the keep flag in the tiles step, the hide below z12, the pictures |

Source: the owner's pick of 2026-11-26 (planning chat): "D: simplify the water itself" (which includes C's map
calming), and "Hold the re-cut for all of them". The notes: `braids-design` (option A "map-only hide below z12"),
`water-options` (C: "same-answer side channels not drawn until z12 = 4,005 lines"). Related: T1176, T1240, T1242.

## Work
1. The tiles step marks each side-channel line "same answer as its river" (the keep flag): a channel that carries its
   own name, its own rule, a closure, or a different answer than its river is kept at every zoom.
2. Below zoom 12 only the keep-flagged lines draw. The first estimate is 4,005 named-water lines hidden (60 of 77 lines
   at zoom 10 on the Skeena); unnamed lines are known only once the flag exists. Tiles and deliver rebuild, about 45
   minutes; no binding and no section id changes.
3. Pictures at z10-z13 on the Skeena, the Squamish and the Klinaklini before and after, with the count hidden per zoom.

## Acceptance
No closed or differently answering channel is ever hidden (a test over the bundle); counts per zoom match the
estimate; pictures sent to the owner.

Severity: low (a look). Model: Opus. Size: M. Built by the data and release chat.
