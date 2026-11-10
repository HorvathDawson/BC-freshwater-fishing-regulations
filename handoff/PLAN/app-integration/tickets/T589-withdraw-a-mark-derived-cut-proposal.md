# T589 — A cut proposal can be withdrawn (the Allco fence)

Status: OPEN

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
