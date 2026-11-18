# T846 — A decided fish answer says "keep" where a take-0 sub-cap over part of the pool releases that fish (Bella Coola cutthroat)

Status: OPEN (after the trout cutover; the trout table already says release and J pins it).

Source: T810 L re-review 2026-11-18. On the Bella Coola tributaries (Ape, Assanany, Burnt Bridge Creeks, Atnarko River;
79 cells, 35 waters) `atnarko_bella_coola_rivers.r4` ("all cutthroat trout catch and release") is a take-0 sub-cap holding
only some of the pool's fish. `rows/evaluate.py` (the subcap loop) turns it into a "limit" line and never sets the fish's
own status to release, so the rows frame's decided answer reads keep, daily 1. The engine allows none (checked on a 0.5 cm
grid, both origins), and so does the book. The trout table moves the fish to Release (`A_NONE_KEPT_IS_RELEASE`). But other
readers of `fish` (the year strip, any non-table reader) may still say keep.

## Fix
At the source, in the decided answer: a fish a take-0 sub-cap releases gets status release. Diff every decided answer
before and after (answers_diff) and check every reader of `fish`.
