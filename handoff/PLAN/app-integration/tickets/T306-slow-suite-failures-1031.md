# T306 — The slow suite fails 8 tests on the live data (found 2026-10-31)

Status: DONE 2026-11-06 (chat 3; f0b92de23, 876e63529 earlier; 1e061ed5f the last 6). Below: the original 8; then the 2026-11-06 re-check.
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

## Re-check 2026-11-06 (each run alone, unfiltered `.venv/bin/python -m pytest <id> -m slow`)

No real data or code defect left; the release and the map are not affected. Every red was a pin stale against an owner ruling.

| test | now | cause | fix |
|---|---|---|---|
| status_index 6000 section-days | PASS | fixed by f0b92de23 (tidal/outside/closed/own decided in ONE `section_code`); LATEST r-5a5999c94588 (11-04) and CANDIDATE r-0fd5c0e4e8b8 were built after it and ship the same paint bytes | — |
| zone_decisions national parks | PASS | fixed earlier (876e63529) | — |
| ambiguous_cut cayoosh | PASS | split exists again in cuts.json | — |
| curation_review_browser `j` | PASS 3/3 | load flake; waits already raised to 180 s | — |
| one_water_kind ×3 | fixed | T322 (owner 10-30): the confirmed OKANAGAN RIVER OXBOWS are streams in place; 10 are isolated (no edge, no route geometry), so nothing goes in the `stream` line layer — their polygon draws in `lake` with `water: stream` | pin: an oxbow is a stream ⇔ its polygon is a slough call (32 of 40); a routeless stream polygon must be a confirmed call (1e061ed5f) |
| reach_tributaries reachability | fixed | the test's own BFS started at the reach only, so a stream whose mouth sits on the lower cut (360885316 at 360873822:164908, the documented rule) had its 7,338 upstream sections called unreachable; the sample hit it after a registry change | BFS also starts at the lower-cut mouths (1e061ed5f) |
| watershed_parts Skeena above Cedarvale | fixed | 7 polygons are confirmed sloughs folded into their creeks (Pinkut, Station, Darling, Telkwa: T322), so a stream's own sections | allowed when slough call ∩ stream item (1e061ed5f) |
| unnamed_trib_sweep | fixed | only `bundle.reach_digest` moved (a01d4c9a → 225ecb22); all 11 result items identical | digest re-pinned (1e061ed5f). The queued rebuild will move it again if its reaches change: then rerun `--write-result` |
