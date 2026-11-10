# T589 — A cut proposal can be withdrawn (the Allco fence)

Status: PARTIAL — 680c1d6eb, 54a06dc89 (2026-11-10): the withdraw call is built and the Allco fence cut is withdrawn by the owner; the button to withdraw one himself is open (it needs a new mark kind and a writer: see below)

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

## Owner 2026-11-10: "yes" — the Allco fence cut is withdrawn (54a06dc89)
One entry in `data/curated/review/proposal_overrides.json` (`withdrawn: true`, "withdrawn by the owner 2026-11-10").
Checked on the real marks: it is out of `marks.amend` / `marks.proposals` (not built, not offered), on
`marks.withdrawn_groups`, no mark in force uses it as a point, and the landed check is unchanged (19 waiting, as
before). Seen on screen on the Alouette place: "Withdrawn cut proposals on this water (not built, not offered): “Allco
Park boundary signs (Allco hatchery fence)” — withdrawn by the owner 2026-11-10 …" (`.playwright-mcp/
t589-withdrawn-line-and-landed-banner.png`).

## Still open: a button to withdraw one himself
NOT built. The app serves exactly one write, a mark (`test_curation_review_safety.py` pins it), and the overrides file
is not written from marks. A button therefore needs: a mark that says "withdraw this proposed cut" (a new scope or a
call on the place's mark, with its prints = the cut's anchor), a file writer that merges those marks into
`proposal_overrides.json` beside the hand-written entries (never dropping one), its write-through domain, and the
button on each proposed cut. Until then a withdrawal is one entry in that file, added by Claude on his word.
