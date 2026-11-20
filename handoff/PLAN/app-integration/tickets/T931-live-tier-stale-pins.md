# T931 — Live-data test tier: ~55 stale pins after the 11-15..11-19 rulings, and one invariant break

Status: OPEN.

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
  declaration): make the plugin accept its own `required=false` line, or introspect in a clean copy.
- (D, the TestFlight message test broken by T918, was fixed on lane/t920-batch.)

## Acceptance
`scripts/test-full.sh` green on the live tree; each re-pin names its ruling in a comment.
Depends: none. Size: M.
