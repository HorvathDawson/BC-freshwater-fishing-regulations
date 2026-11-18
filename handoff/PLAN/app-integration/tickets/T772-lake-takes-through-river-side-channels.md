# T772 — A lake "including tributaries" takes the side channels of its own through-river (Kitsumkalum Lake: 16 pieces)

Status: IN PROGRESS (lane/t804-t807, worktree .claude/worktrees/t804-t807, with T804/T807: built, proved on a side reach run; left: the main chat merges, the owner reads the list on the changed-answers page before the release).

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
