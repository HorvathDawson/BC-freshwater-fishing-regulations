# T306 — The slow suite fails 8 tests on the live data (found 2026-10-31)

Status: IN REVIEW — f0b92de23 and 876e63529 fixed some; NEXT-SESSION reports slow 272 pass; left: re-check which of the 8 are still red. The full suite was run once at the end of the build pass: fast 6193 passed; slow 262 passed, 8 failed.
The build pass did not cause them. Every one reads LIVE generated data (the atlas, the bundle, the tiles) or a fixture,
the pass touched no live output, and the side rebuild of the same inputs is byte-identical to the live bundle and
release data files (`build-pass-2026-10-31.md`).

| test | says |
|---|---|
| test_status_index::test_the_index_agrees_with_effective_rules_on_6000_section_days | **198 of 6000 disagree** (e.g. sid 322, 2025-07-14: index 0, reader 3). The release on staging is affected: look at this first |
| test_zone_decisions::test_provincial_licences_do_not_bind_inside_national_parks | 5 sids listed as national-park exceptions that are not parks/tidal (435, 59020, 1497022, 1497179, 1497827) |
| test_ambiguous_cut::test_cayoosh_below_the_falls_starts_at_the_lower_crossing | KeyError `cayoosh_creek__cayoosh_falls` (the split was renamed or removed) |
| test_one_water_kind (3 tests) | 117 != 116 and the like: the flowing-polygon counts moved (T292/T299 data?) |
| test_unnamed_trib_sweep | the sweep moved against `fixtures/t57_trib_sweep.json`. Note that the fixture also has an UNCOMMITTED edit from another lane |
| test_curation_review_browser::test_j_opens_the_next_entry_with_no_question | playwright 60 s timeout (possibly flaky) |

Depends: none. Size: M. Owner: no (status_index first).
