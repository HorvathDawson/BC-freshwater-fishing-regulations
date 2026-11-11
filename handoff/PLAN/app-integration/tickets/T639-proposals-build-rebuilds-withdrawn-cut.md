# T639 — Review proposals: `build()` rebuilds a cut the owner's mark withdrew

Status: DONE — owner 2026-11-11 let chat 11 make this one fix in chat 10's lane. `group_for` returns no group for a member a withdrawn override matches; pinned by `test_a_withdrawn_cut_is_not_rebuilt_from_the_mark_that_still_names_it`; both tests fail with the guard off. `marks.jsonl` untouched.

Source: suite trace 2026-11-11. Lane: chat 10 (`pipeline/regs/review/` is theirs; chat 11 did not touch it).

Failing: `test_curation_review_split_preview::test_every_proposal_is_shown_under_its_real_id_and_the_build_lands_the_same`.

Cause as traced (not yet confirmed by chat 10): in `pipeline/regs/review/proposals.py`, `build()`'s `group_for`
fallback rebuilds a cut that `marks.amend` dropped as withdrawn (T589). The owner's uncommitted `marks.jsonl` rows
(2026-11-09, Alouette / Allco) still carry `alouette_river__allco_park_boundary_signs_allco_hatchery_fence`, so it
lands as a `new_split` while `dfo.real_ids` leaves it out.

## Work
1. In `build`, skip a member that matches a withdrawn override in `src.overrides`. Never edit `marks.jsonl`.
2. The test passes on the owner's live marks.

Depends: T589. Size: S.
