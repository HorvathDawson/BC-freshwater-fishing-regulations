# T491 — The Nanaimo tidal test read live curated data

Status: DONE — 763bb4d.

`test_curation_review_screen` T449 test expected the Nanaimo place undecided; the owner's 2026-11-06 mark wrote
`tidal_water` onto the live place, so it failed. The test now takes the flag off in its own sandbox copy (fixture
`undecided_nanaimo`), puts it back after; live curated untouched. Whole file: 18 passed.
