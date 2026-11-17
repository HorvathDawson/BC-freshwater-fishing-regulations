# T811 — The owner's Squamish "downstream of the powerline crossing" tick reads "binding moved"

Status: OPEN, waiting on the owner's re-tick in the review app (no code change).

Found by: `pipeline/tests/test_marks_never_orphaned.py::test_no_owner_mark_is_orphaned_or_dropped_live` (needs_atlas,
not in the CI tier), failing on marks line 2511 (scope dfo, target
`2:squamish-river-including-powerhous:downstream-of-the-powerline-crossi`, verified 2026-11-09, P answered).

## Cause (traced 2026-11-16 16:10)
A DFO mark's binding is the curated geography (`pipeline/regs/review/dfo.py` `geography`): the water record's
`item_ids` and tributaries plus the place's own binding. The place's binding is unchanged since Nov 9 (downstream of
`squamish_river__powerline_crossing_at_9_mile`). The WATER RECORD changed: commit 7102afb9b (2026-11-15 20:43, "Fraser/
Squamish side channels in their DFO water records", from the owner's answers that day) added `blk:360219234` (Squamish
Powerhouse Channel) to `2:squamish-river-including-powerhouse-channel`, which both Squamish places share. The owner
re-ticked the "whole" place after it (marks 2764 to 2766) but not this one.

## What it means
The Powerhouse Channel meets the Squamish by Ashlu Creek (cut `squamish_powerhouse_channel__ashlu_creek_into_squamish_river`,
about 49.896 N), far above the powerline crossing (49.786 N). The "downstream of" clip leaves it out, so what this
place draws should not change; only the fingerprint moved. Not a data fault.

## To close
The owner re-ticks the place in the review app (its picture shown with the other follow-up saves). Then the live
test passes again.
