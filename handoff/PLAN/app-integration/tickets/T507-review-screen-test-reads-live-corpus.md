# T507 — test_the_corpus_tiers_match_plan_section_3 reads the live corpus

Status: DONE — 7bb9d6c67 (found 2026-11-08, full pipeline suite, unrelated to T503).

`pipeline/tests/test_review_screen.py::test_the_corpus_tiers_match_plan_section_3` counts tiers over the LIVE curated
corpus: one tier now has 452 entries, the test pins 445. Curation changes (owner reviews / write-through) move it, as
T491's test did. Give it its own frozen fixture (like T491), or turn the counts into bounds the plan states.

**Built 2026-11-11.** The test reads `pipeline/tests/fixtures/review_tiers_sample.json`, a frozen sample cut from the
corpus that day: 58 entries, the shortest of every tier × risk combination (28), each pinned to its exact tier and
risks; 19 cut points, two of every note kind (grade, "Verify:", human-review). A mutation pin sits inside (tiers
without the flag move exactly the flag-only P1/P3 rows). The live corpus keeps one count-free check
(`test_every_live_entry_takes_a_tier_and_every_tier_is_in_use`, through `under_test`). The day's counts (1515 entries,
P1 221 / Z 122 / P2 266 / P3 99 / P4 747 / sweep 60, 458 cut points) are in the fixture for the record; nothing
asserts them. `test_review_screen.py`: 17 passed, 1 deselected.
