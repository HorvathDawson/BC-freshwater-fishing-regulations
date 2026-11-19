# T846 — A decided fish answer says "keep" where a take-0 sub-cap over part of the pool releases that fish (Bella Coola cutthroat)

Status: IN PROGRESS (main chat): fixed in code 2026-11-18; DONE when the data release carrying it is on staging (T863).

Source: T810 L re-review 2026-11-18. On the Bella Coola tributaries (Ape, Assanany, Burnt Bridge Creeks, Atnarko River;
79 cells, 35 waters) `atnarko_bella_coola_rivers.r4` ("all cutthroat trout catch and release") is a take-0 sub-cap holding
only some of the pool's fish. `rows/evaluate.py` (the subcap loop) turns it into a "limit" line and never sets the fish's
own status to release, so the rows frame's decided answer reads keep, daily 1. The engine allows none (checked on a 0.5 cm
grid, both origins), and so does the book. The trout table moves the fish to Release (`A_NONE_KEPT_IS_RELEASE`). But other
readers of `fish` (the year strip, any non-table reader) may still say keep.

## Fix
At the source, in the decided answer: a fish a take-0 sub-cap releases gets status release. Diff every decided answer
before and after (answers_diff) and check every reader of `fish`.

## Done (2026-11-18, main chat; goes in the data release before production, T863)
`rows/evaluate.py` `_subcap_released` (`A_ZERO_SHARE_RELEASES`): a speaking take-0 `subcap` that covers the fish whole
(no lengths, not only part of the day) releases it. The decided answer is status release, the share governs, and the
quota is moot for that fish, beside F16's `_range_released`. Pinned in test_trout_check.py
`test_t846_a_take_0_share_of_the_quota_releases_its_fish_in_the_decided_answer` (the mini world's Sample Lake, mutation
built in a child process). J's zero -> Release guard stays as a backstop (its test now feeds a pre-T846 frame).

Owner, ~18:05 via the app chat, verbatim: "please make sure no other item has the same issue as bella coola did. do a
general check across everything". The app chat runs an independent read-only audit across the whole release: every
decided keep the engine or the book allows none of, all fish, both origins. Findings are fixed at source in the same
data release.

Audit result (app chat, 18:2x, r-86d43303bee2, 22 fish, both origins): Bella Coola cutthroat is the only finding (35
waters, 72 key-segments, winner r3, blocked by r4, p.43). The export has three take-0 subcaps: r4 here; khartoum_lake.r5b
and lois_lake.r4b (steelhead) already decide release. The T846 diff, rebuilt for those 35 items, reads release with r4
governing. Aside, examined: atnarko_bella_coola_rivers.r8 "no rainbow may be over 50 cm" (Apr 1-May 31, a sizecap
within r6) is in no decided answer's roles. On a steelhead water a rainbow's range ends at 50 cm (p.80, `fish_range`),
so its band has nothing to act on; no answer is lost.

Card side effect (the old rows card the trout table is built from; fixture `answers28` BELLA/BELLA_TRIB): with the
cutthroat out of the group, every kept fish's answer also counts toward Region 2's 4 (`total_of`), so the river's 1 is the
own tier under that total ("This river's rule: up to 1 a day (part of the 4)"), the shape its Apr 1-May 31 row already
had; the cutthroat is a not-kept line ("Cutthroat trout: release every one"). Before, the cutthroat's displaced region
quota kept the 4 out by accident. A trial that kept the river's 1 as the root (own pool over the same fish, fewer a day)
broke that May shape, so it was dropped. Tests re-pinned: r13 Z/S, r14 6, r16 2, r18 8, r20 1, r20g, t475 1, ur1 F,
trout_check's A_ZERO_COUNT_IS_RELEASE pin (source fix off too). The own-total-as-root pins moved to COUNT (a creek's 2).
