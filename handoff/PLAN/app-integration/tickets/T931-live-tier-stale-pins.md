# T931 — Live-data test tier: ~55 stale pins after the 11-15..11-19 rulings, and one invariant break

Status: IN PROGRESS — groups A, B, C, E built on lane/t936-t931 (3 tests refused as suspected data bugs, 1 waits for the bundle rebuild); left (Claude): main chat review + merge.

Source: main chat 2026-11-19 21:28. The first `scripts/test-full.sh` run in days (on lane/t920-batch) finished red. A
read-only diagnosis (scratchpad `diag-a.log`, `diag-b.log`) found none of it comes from that branch: the 0.8.3 and 0.8.2
"green" tiers were CI's no-data tier (`not needs_bundle and not needs_atlas …`, 1,650 deselected); the `needs_*` tests had
not run against the live tree since the rulings landed. The live data matches the code (all 273 deliver files in
`data/generated/build-manifest.json` hash-match; bundle, tiles and atlas agree). Rebuilding fixes nothing.

## Groups
- A. Answer and trout pins stale against rulings (~38): T834 (Duncan/Lardeau bull-trout lift dropped: test_answers_27,
  _28_live), T810 cutover (test_answers_one_kind_group 13, _t805_live 6, _rows_fixes 2, _t127_displaced_xref), T810/T907
  (test_t810_cutover 2, test_trout_check 2, test_trout_sheets 2), C2 amended + T854 (test_answers_v22 4,
  _kitimat_steelhead_lift 2, test_export_ui_rules both_hold, test_lift_decisions Nicola, test_trout_scope_and_dated_releases).
  Re-pin each to the ruling's current output.
- B. Geometry/binding/count pins (~12): test_answers_26 + test_part_geo (Cowichan "whole" off, T803), test_part_runs
  Thompson (Juniper/Deadman cuts), test_one_water_kind split-lake parent + test_review_closed_areas (Babine now a split
  parent, T812), test_rulings_pinned f6 (Peace part_ix renumbered: pin by place, not number), test_curation_review_queue
  (1469 vs 1467), test_part_names_unnamed_lake, test_phase3_rulings (Bulkley/Morice on 773423), test_review_notes_1115
  (Bella Coola). Look at the phase3 and Bella Coola bindings before re-pinning.
- C. A real invariant break (2): test_one_water_kind::test_one_water_kind, test_stream_polygon_panels::
  test_no_lake_or_wetland_wears_a_panel. T753 moved nameless lake-loop lines to their lake items (Sproat `354087683:898`,
  `:1295`, Tatlatui, Tsintsunko…); the tiles still draw them as stream and flow panels sit on them. Fix: the tile kind and
  the panel pass follow the lake owner (preferred), or the tests allow the `lake_loop_report.json` pieces.
- E. Environment: test_curation_review_screen GETs `/api/reports`, which calls the live staging feeds worker (503 on a 30 s
  timeout): stub it. test_review_dfo: `node --test` segfault under load (passed on rerun). `app/tools/r8-config.test.ts`
  fails when a prebuilt gitignored `app/apps/mobile/android/` sits in the checkout (T913 plugin throws on its own
  declaration): make the plugin accept its own `required=false` line, or introspect in a clean copy. Do NOT delete
  `android/` to get round it: `android/app/debug.keystore` there is the shared signing key build-apk.sh copies (deleted
  2026-11-19 22:10, restored from a worktree copy, sha256 221e0a31…; a lone keystore does not trip the test).
- (D, the TestFlight message test broken by T918, was fixed on lane/t920-batch.)

## Acceptance
`scripts/test-full.sh` green on the live tree; each re-pin names its ruling in a comment.
Depends: none. Size: M.

## Built (lane/t936-t931, 2026-11-20): 68224fd7d (E), 0d2effc0f (A), 9f4f53e97 (B), 89fcc181e (C)
Run against the main checkout's live data (r-6258ffded676, read-only links). Every re-pin names its ruling in a comment.
- **A (0d2effc0f), 38 re-pinned, 2 refused.** T834 (Duncan/Lardeau lift, Kootenay "Lead with 5", the raised box holding
  the one-kind tiers: `test_answers_one_kind_group` keeps its D19 mutation pins on a pre-T834 build), the T810 cutover
  (possession duty now on the trout table's row sheet: `test_answers_t805_live`), RULINGS C2 amended 2026-11-18 + T854
  (Nicola r3/r3x, Region 6 r4/r7: `test_lift_decisions`, `test_export_ui_rules` both_hold, `test_answers_rows_fixes` —
  the count-collision witness moved to the Ashnola —, `test_answers_v22`, `test_trout_sheets`, `test_t810_cutover`, the
  scope test), T806 (Sheldens Creek one item: `test_trout_check`'s stale `blk:360846083`).
  REFUSED, left red (suspected answers bug): `test_answers_kitimat_steelhead_lift` x2 — the 2026-10-21 (b) ruling (2 over
  50 cm a day, only 1 of them other than a hatchery steelhead) is still enforced by the card_check engine, but since
  T854/C2 the Kitimat pool's `conds` caps are empty and its trout table says nothing of 50 cm: enforced, not told (T942).
  Also seen, not pinned: Chilliwack Lake's lake trout row reads "60 cm or longer" with no wild cap (C2: "no wild trout
  over 50 cm" covers lake trout, so a wild one is never kept; the row does not say so; T943).
- **B (9f4f53e97), 10 re-pinned by place, 1 refused.** Cowichan six parts (T743 Skutz Falls), Thompson cut at the
  Juniper signs (T573), Peace F6 by place (part below the dam closed by `dinosaur_lake.r1b`, T794), Babine circles placed
  and Babine a split parent (T812), Bella Coola tributaries-only (T807), queue 1469 (46 "lake split later"). The two
  counts are ATTRIBUTED (measured on the bundles before / after the 11-15 rebuild, reach 4f1bdab9 -> 6ca9a300):
  unlabelled part marks 110 -> 25 (76 by 93a481b71, `A_DEAD_END_GOES_WITH_ITS_WATER`: the side part, a dead end, now
  carries the walk's rules and joins the main part; 9 on 8 waters re-cut or now worded by T850's cut names), and the
  multi-part-waters floor 500 -> 475 (export 482 today; 86 waters left the several-parts set, 83 by the same change).
  REFUSED, left red (suspected reach/licensing bug): `test_phase3_rulings::test_no_section_carries_two_classified_waters_units`
  — section 773423 (`360242522:0`), a dead-end piece of Shea Creek (`gnis:22080`, Burnie-Shea Park), carries BOTH the
  Bulkley and the Morice designations while Shea Creek's main stem carries none; likely `A_DEAD_END_GOES_WITH_ITS_WATER`
  attaching the sink to both walks (T944).
- **C (89fcc181e).** Panel pass follows the lake owner: `panels.standing_water_lines` takes the flow panel off every LINE
  a lake/wetland item owns (fixture + mutation tests). Live proof needs the next bundle rebuild:
  `test_no_lake_or_wetland_wears_a_panel` stays red on r-6258ffded676 (the 7 loop pieces) until then. Tiles NOT changed:
  making the tile say "lake" on a line needs a `water` attr on the `stream` layer that the app style must read (tile
  contract + "tiles carry only what is read"), so `test_one_water_kind` takes the ticket's alternative — it accepts exactly
  `lake_loop_report.json`'s moved LINES drawn in the stream layer (as T753's rebuild verified).
- **E (68224fd7d).** Review screen test: the staging feeds call is stubbed at the network edge. `with-optional-portrait`
  accepts its own line, exactly and alone (a prebuilt `android/`), still refuses any other; `android/` untouched.
  `test_review_dfo`'s `node --test` segfault is a load flake: nothing changed.
- Found by the one test-full run (2026-11-20), not this lane's: T945 (the committed unnamed-tributary sweep is stale,
  41 streams), T946 (20 Tier 2 tributary lines: Lake Revelstoke tributaries' DV rows, Iverson Creek).
