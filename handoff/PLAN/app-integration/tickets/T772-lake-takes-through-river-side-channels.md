# T772 — A lake "including tributaries" takes the side channels of its own through-river (Kitsumkalum Lake: 16 pieces)

Status: DONE — fa098f1f4, d5547fe69. A lake's tributary walk leaves its through-river's side channels out (Kitsumkalum Lake); owner OK'd the changes 2026-11-17; marks re-saved cb008158f.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T772 | A lake "including tributaries" takes its through-river's own side channels (Kitsumkalum Lake: 16 pieces, 14.6 km of the Kitsumkalum River above the lake) | DONE | fa098f1f4, d5547fe69, cb008158f | — |

Closed by the 2026-11-20 audit: built on lane/t804-t807 (fa098f1f4, d5547fe69), and the lane is an ancestor of redesign/stream-sections. The owner OK'd the side-stream changes 2026-11-17 ~15:04 (e59d7146f) and said 'land it' at ~17:01; the 20 marks were re-saved (cb008158f). Shipped with the 0.8.0 data release and later releases.

Source: second-look fix lane B, 2026-11-15 night (`second-look-fixes-2026-11-15.md`, Regions 2-8 finding 1).

## What is wrong
The Kitsumkalum Lake DFO place with tributaries on binds 16 side-channel pieces (14.6 km) of the Kitsumkalum River
above the lake. The through-river rule (memory "lake must not answer to its river") drops only the river's MAIN line
from the lake's tributary walk; the river's own side channels still come in as if they were lake tributaries.

## What to build
- In the lake tributary walk, the through-river's side channels (the pieces `reach/channels.py` gives the river) are
  left out together with its main line.
- List every lake place this changes (binding diff, before and after) and attribute each; not only the Kitsumkalum.
- A test on built data (anchors rule): a lake with a through-river that has side channels.

2026-11-17 ~15:04, owner: see OWNER-TODO 15:04 (answer recorded verbatim there).

2026-11-17 ~17:01, owner: land it (verbatim in OWNER-TODO 17:01); Tatshenshini named-tributary places bind only the named tributary; dormant three follow DFO words; Claude re-saves the 12 changed marks.
