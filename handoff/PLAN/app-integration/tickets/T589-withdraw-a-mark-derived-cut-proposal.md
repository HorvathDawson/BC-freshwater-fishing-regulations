# T589 — A cut proposal can be withdrawn (the Allco fence)

Status: IN REVIEW — 680c1d6eb, 2026-11-10: the withdraw call is built and tested (`marks.ConfidenceOverride.withdrawn`, `marks.withdrawn_groups`, the panel's "Withdrawn cut proposals" line). NOT done: withdrawing the Allco cut itself (the owner's call: one entry in `data/curated/review/proposal_overrides.json` with `withdrawn: true` and a note; there is no button for it yet). Not looked at in a browser (nothing is withdrawn today)

Source: T532 round 6, leftover c.

## Today
`proposal_overrides` can only set a proposal's confidence. The mark-derived cut
`alouette_river__allco_park_boundary_signs_allco_hatchery_fence` is still listed as a proposal although the owner
replaced it; there is no way to say "withdrawn" short of a new mark that no longer names it.

## Work
Add `withdrawn: true` (with `why` and `by`) to the override schema; a withdrawn proposal is not built, not offered by
any picker, and listed under "withdrawn" where proposals are listed (never silently gone). Test + mutation pin: the
Allco cut withdrawn is absent from `proposed_on` and present in the withdrawn list. Withdrawing the Allco cut itself
is the owner's click, not a default.

Depends: —. Size: S–M. Owner: no (the Allco click is his).
