# T507 — test_the_corpus_tiers_match_plan_section_3 reads the live corpus

Status: OPEN — found 2026-11-08 (full pipeline suite, unrelated to T503).

`pipeline/tests/test_review_screen.py::test_the_corpus_tiers_match_plan_section_3` counts tiers over the LIVE curated
corpus: one tier now has 452 entries, the test pins 445. Curation changes (owner reviews / write-through) move it, as
T491's test did. Give it its own frozen fixture (like T491), or turn the counts into bounds the plan states.
