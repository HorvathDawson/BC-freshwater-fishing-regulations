# T931 — Live-data test tier: ~55 stale pins after the 11-15..11-19 rulings, and one invariant break

Status: IN PROGRESS — lane merged (bff397878, 11-20); reviewed 2026-11-22: 3 pins fixed (a6f2a47cd Skutz Falls copied removed review words + unlabelled marks 25→22 by T944; 7e126231a T981's local-authority closure has no book page by design; 2f8d3b72e the Babine hand placement check had become always-true). The 3 refusals (T942, T944) and the panel invariant pass now. Left: T1000 (Adskwatim tile gap, 2 live tests red until the next tile build); `test_answers_one_kind_group.py` ~533 (Beattie: bull trout and lake trout became separate trout-group entries with no ruling named) needs its ruling or commit cited.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T931 | Live-data test tier: ~55 stale pins after the 11-15..11-19 rulings, one lake-loop invariant break (T753), env flakes | IN PROGRESS — reviewed + fixed 2026-11-22; left: T1000 tile gap (2 live tests), Beattie group split needs its ruling cited | 68224fd7d, 0d2effc0f, 9f4f53e97, 89fcc181e, a6f2a47cd, 7e126231a, 2f8d3b72e | main chat 2026-11-19 tier diagnosis |

Source: main chat 2026-11-19 21:28. The first `scripts/test-full.sh` run in days (on lane/t920-batch) finished red. A
read-only diagnosis (scratchpad `diag-a.log`, `diag-b.log`) found none of it comes from that branch: the 0.8.3 and 0.8.2
"green" tiers were CI's no-data tier (`not needs_bundle and not needs_atlas …`, 1,650 deselected); the `needs_*` tests had
not run against the live tree since the rulings landed. The live data matches the code (all 273 deliver files in
`data/generated/build-manifest.json` hash-match; bundle, tiles and atlas agree). Rebuilding fixes nothing.

## Done
- Lane/t936-t931 (2026-11-20, merged bff397878), run against r-6258ffded676, every re-pin names its ruling in a comment:
  E 68224fd7d (review-screen staging call stubbed; `with-optional-portrait` accepts its own line alone; `android/` untouched
  — `android/app/debug.keystore` there is the shared signing key; `test_review_dfo`'s segfault is a load flake), A 0d2effc0f
  (38 re-pinned), B 9f4f53e97 (10 re-pinned by place; unlabelled part marks 110 -> 25 and multi-part floor 500 -> 475
  attributed to 93a481b71 `A_DEAD_END_GOES_WITH_ITS_WATER`), C 89fcc181e (`panels.standing_water_lines`; `test_one_water_kind`
  accepts exactly `lake_loop_report.json`'s moved LINES in the stream layer; tiles not changed). D was fixed on lane/t920-batch.
- The refusals became T942 (Kitimat steelhead: enforced, not told), T943 (Chilliwack Lake's lake trout row), T944 (section
  773423 carries both Bulkley and Morice); found by the same run: T945, T946.
- Review 2026-11-22: a6f2a47cd, 7e126231a, 2f8d3b72e (see Status).

## Left
- T1000 (Adskwatim tile gap): 2 live tests red until the next tile build.
- `test_answers_one_kind_group.py` ~533 (Beattie: bull trout and lake trout became separate trout-group entries with no
  ruling named): cite its ruling or commit.

## Acceptance
`scripts/test-full.sh` green on the live tree; each re-pin names its ruling in a comment.
Depends: none. Size: M.
