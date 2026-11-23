# T465 — Water map: boundary labels with DIFFERENT words still overlap when zoomed out

Status: DONE (owner rule 2026-11-22, T997 round 2: only the lit part's two ends are labelled, hung apart; code 4cdf3245f). Found 2026-11-06 by the step-screen lane while doing T443.

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

## Closed by the owner's rule (2026-11-23, lane L18)
Owner 2026-11-22 (T997 + T465, round 2): "Only need boundaries for highlighted section." So no water map draws more
than two boundary labels — the lit part's own ends — and those two hang one above and one below their rings, so they
never meet (T997 round 2, 4cdf3245f). The Fraser's top part zoomed out now shows "Region 5 boundary" alone (its upper end
is the source): `T997-shots/round2-fraser-top-web-dark-{before,after}.png`, `round2-fraser-whole-*`. PR #51 (the framing
estimate and its invariant test) is closed unmerged: its estimate under-counted metres/px up to 8.4x and its test re-ran
the code's own estimate (review findings), and with two labels hung apart no estimate is needed.
