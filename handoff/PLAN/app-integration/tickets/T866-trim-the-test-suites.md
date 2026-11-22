# T866 — Trim the test suites: combine the overlapping ones, remove true duplicates

Status: IN PROGRESS (main chat 2026-11-22: proposal written, owner picked three of four; two lanes).

Owner 2026-11-18 ~19:10 (to the app chat), verbatim: "We should make a ticket for after the releases to try and clean up
tests and reduce there being way too many tests. We could combine a bunch and maybe remove some that are duplicates".

Today (the 0.8.1 web tier, 2026-11-18): app 727 vitest files / 8,517 tests (993 skipped); pipeline ~9,280 collected
(7,633 in the fast tier, 1,650 deselected as slow/needs-data); the full tier takes ~8 min on the Mac, and a single
landing often fails it on a pin that restates what another test already holds.

Build:
1. Measure first: per file, test count, run time, and what it asserts; find the overlap (the same view model or rule
   pinned in several files, the same string pinned in core + ui + shell tests, near-identical render tests per screen,
   pipeline tests that re-pin a curated value another test already proves). One table, by area.
2. Combine where a group pins the same behaviour through the same seam: one parameterised test per behaviour.
3. Remove true duplicates only: a test whose every assertion another test also makes (prove it per removal: the kept
   test fails under the mutation the removed one caught). No coverage loss in the guards that earned their place
   (no-silent-skips, test anchors, surfaces/T60, one-colour-pathway, ticket guard, the mutation pins).
4. Report before/after: files, tests, tier time; the owner sees the list of what went before it lands.

## Owner 2026-11-22, dialog on the proposal (`handoff/PLAN/audit-2026-11-21/t866-proposal.md`)
Picked: "Speedups, remove nothing (Rec.)", "Remove 5 true duplicates", "Fold the 30 wording files". Not picked: parallel
pipeline tests (pytest-xdist). So: (1) app-ci vitest split across 4 runners; ~15 website tests on the build's bare mode
(same outputs checked); the 21 s stall timer test and the 48-page full-scale render test made fast without losing their
mutation; (2) the 5 duplicates D1-D5 removed, each proven redundant by a mutation first; (3) the 30 wording-round files
folded into one table per water and day.

## Lane test/T866-dedupe-fold (2026-11-22): (2) the 5 duplicates and (3) the fold — c7894a5e8, 979b13f60, a65145a81
Each removal was proved first: the mutation the removed test caught, planted, fails the kept one (after any check only the
removed one had was moved in).
- D1+D2 (inputs.release "T810 M", 55 tests → trout.release "T810 N"): M also checked `inp.s`, a lake of one part and ONE
  section at the case's metre (N took the first); all three moved into N first. Mutations failing N: trout cell decoded
  null (60 fail), segment off by one (59), a duplicated section line (11; N alone passed it before), vectors with no null
  cell (1). inputs.release 185 → 130 tests.
- D3 (card.parity oracle meta ⊂ settle.parity's): oracle version, year, a dropped state each fail the kept test.
- D4 (r13 gear sentences = r12 test_w5): its "no any hook" check moved into r12's. Mutations failing r12: the no-hook
  suppression removed, the weight sentence reworded.
- D5 (test_one_data_flow promote refusal): NOT a pure duplicate as it stood — it asked with `dry_run=True`, and a dry
  run returning before the refusals was caught by it alone. The kept test_atlas_inputs test now asks a dry run too (no
  marker; an empty folder); then both mutations (refusals removed; dry run first) fail it.
- Fold: every sentence pinned on a card or a gear answer in the 30 files is one entry of one row:
  `pipeline/tests/test_answers_card_words.py` (no data: CARDS 18 rows / 127 entries per fixture water and day, GATE 2/4,
  GEAR 8/11) and `test_answers_card_words_live.py` (needs_bundle, LIVE 8 rows / 33 entries); a sentence two rows say is a
  named constant. The rounds keep numbers, ids, mutations, gate refusals and composers on made-up records; 46 emptied
  tests are gone, no file deleted (RULINGS/ANSWERS-SPEC cite them; no removed test name is cited anywhere).
  Before → after (30 files; after = the same 30 + the 2 tables): 254 → 213 test functions, 261 → 252 collected
  (no-data tier 228 → 213), 728 → 504 asserts plus 175 table entries; 485 sentence asserts (282 distinct sentences) →
  141 distinct left in round asserts + 181 in table rows. Inventory, assert by assert (moved / collapsed-into / stays,
  with reason): `handoff/PLAN/audit-2026-11-21/t866-fold-inventory.tsv`. A wording mutation of the record duty failed 4
  files before, 1 after; of the share line's "of the 4", 7 before, 1 after.
- Live tier, these 32 files (+ the 2 tables after): 306 passed before, 296 after (the difference is the removed tests);
  no-data tier of the touched files, anchors scanner, test_workflow_pins / test_ci_paths / test_test_anchors /
  test_smoke_coverage / test_no_silent_skips pass; app: the 6 touched conformance files 717 passed, tsc -b clean.
- Left for later (not folded, not card words): r20g/r20h pin the same possession glossary term; r15/r16_wording the same
  under-16 sentences; r14/r15 the same weekday sentence (`say_weekdays`); r18/r19 the same cm_in pins.
