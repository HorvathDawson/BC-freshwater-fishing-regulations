# T589 — A cut proposal can be withdrawn (the Allco fence)

Status: PARTIAL — 680c1d6eb, 54a06dc89 (2026-11-10). The button is NOT built and was stopped 2026-11-22 as too big for a leftover lane: it is a new mark scope (schema, validators, `export` domain, an `apply` writer into `proposal_overrides.json`, write-through, the button on each proposed cut, API.md), the `closed_area` scope is the pattern. Own ticket-sized lane. left (Claude): the withdraw button (a new mark scope).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T589 | A cut proposal can be withdrawn (the Allco fence) | PARTIAL — Allco withdrawn (owner 11-10); the button open, stopped 11-22 as too big (new mark scope); left (Claude): the withdraw button | 680c1d6eb, 54a06dc89 | T532 leftover c; the Allco click is the owner's |

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

State 2026-11-22 (stopped for credits): see Status above; code c77eeb601, ledger f087c8968; branch fix/T775-review-app-leftovers. fast-fail not confirmed (was still running).
