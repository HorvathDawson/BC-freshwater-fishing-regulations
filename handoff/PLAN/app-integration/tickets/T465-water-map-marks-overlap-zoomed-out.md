# T465 — Water map: boundary labels with DIFFERENT words still overlap when zoomed out

Status: IN REVIEW — owner: OWNER-TODO "Look (you): T465 Fraser top labels" (shots T465-shots/); code 2c08348fb on the T997 merge work (PR open).

T443 drew one label for two same-words marks a few steps apart (Landstrom Bar). Marks with different words still
pile up where several sit close together at a low zoom: the Fraser's top part (Williams Lake, about z5.5) shows
overlapping boundary labels.

## Work
1. List the release's waters where two or more labelled marks fall within one label's width at the zoom the part is
   framed at (the same check T443 used for same words; 12 such pairs in 9 waters were found there).
2. Pick one rule for both platforms: e.g. the label of the mark nearest the stepped part wins, the others draw only
   their dot, or labels collide (symbol placement) and the dropped ones stay readable in the step bar. Test on the
   Fraser top and one more water.
3. Shots at 360 dp, before and after, to the owner.
Related: T443. Size: S–M.

## Result (2026-11-22, lane L3; builds on T997)
Rule picked (one for both platforms): boundary labels whose boxes would meet at the framing are ONE box naming each
(T997's `mergeNearLabels`, framing from the part's or the whole water's bounds), and the label layer collides what a
pan/zoom brings together. Sweep at WHOLE-water framing (r-741815c6090d): 901 part frames with 2+ labelled marks, 558 merge
a box (209 with the mouth), 0 kept boxes meet (the old rule left 499 colliding); at the part's own framing 73 / 0 (T997).
Test: the Fraser and the Chilliwack at every part, part and whole framing: no two drawn boxes meet (fails with the merge off).
Shots: the Fraser top part (Williams Lake, ~100 km scale) before / after, web. Merged boxes at whole-water framing can name
several marks ("A · B · C"): that is the cost of never overlapping; the owner may prefer dropping to the nearest instead.

State 2026-11-22 (stopped for credits): done: invariant test (Fraser + Chilliwack, every part and whole framing, mutation-proved) and Fraser-top before/after shots; left: owner look, PR review. Branch fix/T465-zoomed-out-labels (contains the T997 branch).
