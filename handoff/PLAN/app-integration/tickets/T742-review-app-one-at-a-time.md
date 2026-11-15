# T742 — Review app: no grouping, region ends in every picker, and faster

Status: IN PROGRESS (review-app lane, 2026-11-14)

Source: owner 2026-11-14, while labelling the Fraser's DFO places: "the fraser marks are all fucked up the groups are all
messed up. i think we should just remove the grouping option in the review app it makes it have a lot of glitches instead
one at a time is better I think. we should revise the review curation then also make sure the region cuts are in all the
app please." and "also the review app needs to be faster it is SO SO SO slow rn".

Stub written by the main chat so the ledger is whole; the lane fills in the work, findings and result below.

## Work
A. What grouping does wrong on the Fraser; the list of group-written Fraser marks
   (`handoff/PLAN/curation-review/fraser-group-marks-2026-11-14.md`).
B. Remove grouping: every place shown and marked one at a time; old group-written marks stay, flagged.
C. Region-boundary ends offered, by plain name, in every picker.
E. Speed: the fixes ranked in the profiling findings (render loop in the editor, the per-request section map, the wait
   after a save, the entries list on the DFO screen, the slow start).
