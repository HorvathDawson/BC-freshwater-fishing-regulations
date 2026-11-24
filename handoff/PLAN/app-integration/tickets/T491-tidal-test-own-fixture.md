# T491 — The Nanaimo tidal test read live curated data

Status: DONE — 763bb4d.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T491 | test_curation_review_screen tidal test reads live curated data; the owner's 11-06 Nanaimo mark decided it | DONE | 763bb4d | found 2026-11-07; give the test its own fixture |

`test_curation_review_screen` T449 test expected the Nanaimo place undecided; the owner's 2026-11-06 mark wrote
`tidal_water` onto the live place, so it failed. The test now takes the flag off in its own sandbox copy (fixture
`undecided_nanaimo`), puts it back after; live curated untouched. Whole file: 18 passed.
