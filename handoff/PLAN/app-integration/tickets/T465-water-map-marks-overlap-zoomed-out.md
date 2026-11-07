# T465 — Water map: boundary labels with DIFFERENT words still overlap when zoomed out

Status: OPEN (found 2026-11-06 by the step-screen lane while doing T443).

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
