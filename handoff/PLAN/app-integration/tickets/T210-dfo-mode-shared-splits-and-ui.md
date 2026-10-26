# T210 — DFO review mode: proposed splits shared along the river, Place "y" without a new split, simpler new-split UI

Status: OPEN

Source: owner 2026-10-26, using the DFO extents mode (T164).

1. A split proposed on one DFO location (a `new_splits` entry in a mark) must show on EVERY other DFO location / entry
   on the same river (same item / blk), drawn as "proposed" and pickable as a point — today it disappears when you move
   on, so it gets entered several times. Read proposed splits from all marks; dedupe by anchor (same blk + measure
   within a few metres, or same confluence/lake anchor).
2. When the extent is right with the existing cuts, the reviewer must be able to answer Place = y; find and remove what
   pushes the answer to ? / n when no new split is added.
3. New-split UI: click the river → pick an anchor type (confluence · lake inlet/outlet · bridge · dam · sign · here) →
   confirm; name and side pre-filled from the anchor and the extent; edit only if needed. No long form.
Mark stays the only write (T185 / AGENTS 66–67).
