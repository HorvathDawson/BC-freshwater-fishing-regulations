# T866 — Trim the test suites: combine the overlapping ones, remove true duplicates

Status: DONE (2026-11-22: both lanes landed by PR — the speedups (#16, 67c6c813c) and the 5 duplicates + the wording fold (#17: c7894a5e8, 979b13f60, a65145a81); the owner did not pick pytest-xdist; "Left for later" lines below are small follow-ups, not this ticket).

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

## The speedups lane (perf/T866-speedups, 2026-11-22): owner picked "Speedups, remove nothing"

Proposal: `handoff/PLAN/audit-2026-11-21/t866-proposal.md`. Nothing removed; every changed test re-proved by mutation.
- **S1, app-ci vitest on 4 runners** (`.github/workflows/app-ci.yml`): a `shard: [1..4]` matrix, `--shard=i/N` with N =
  `strategy.job-total` (typed once), fail-fast off, 2 workers per leg as before; `app ok` needs `vitest`, whose result is
  every leg's. Each leg runs `tools/vitest-shards.mjs N` first (vitest's own sequencer over its own file list: the N slices
  partition the unsharded list, 782 files = 196+196+196+194 on 11e4f0b1a) and `--ran i` last (the files the leg's json report names are
  its slice). Pinned by `test_workflow_pins.py::test_app_vitest_is_sharded_and_every_file_runs_in_exactly_one_leg` (5
  workflow mutations each fail it) and `tools/vitest-shards.test.ts` (planted missing / doubled / foreign / empty).
- **S2, bare SEO builds**: 17 of the 19 fixture-site builds in `tools/seo/{site,gauges,gauges.share,legacy,appLinks}.test.ts`
  read no water-page HTML, pane stylesheet or share PNG and are now `bare: true`. Each switched call was built both ways
  with its own arguments: pages (all but a water page's HTML), snapshot, models, slug map, removed, regions, gauges, every
  other file, the legacy map and the problems were equal in 15 of 15 (full ~9 s, bare ~1.5 s each). Kept full: site.test's
  `built` (panes, budget, share cards, static output) and gauges.test's main build (the gauge share card). New pin in
  site.test: "(T866) a bare build is the full build without the water panes and the share pictures". 11 mutations (lastmod,
  hub split, changes counts, a day-dependent description, the refresh date, the removed-slug 301, bare != full, gauge page
  list, legacy target, assetlinks, AASA) each fail their bare-built test.
- **S4, `net.test.ts` cut-mid-way**: `fetchVerifiedVia` takes `limits` / `sleep` through to `fetchResilient`; the test
  passes the FAST limits its neighbours use (21 s -> 0.1 s); no-resume and no-progress mutations still fail it.
- **S5, `test_review_screen.py` thread race**: the 83 s was not the renders (24 renders at scale 1.0 take 0.5 s) but the
  printed -> PDF map read cold by all 8 threads at once (pdfplumber, ~9 s each under the GIL). The test reads it once first;
  scale stays 1.0. With `PDFIUM_LOCK` removed every thread still fails on both routes (10 of 10 script runs at scale 1.0
  and 0.25; the test 5 of 5). 83 s -> 10 s (0 s more once another test in the process read the map).
- **Measured** (laptop, `scripts/heavy.sh` lane 2, the whole app suite): 225.6 s -> 160 s wall, 9,146 -> 9,151 tests passed
  (+5: the bare pin and vitest-shards' 4). Files: site 154 -> 72 s, appLinks 45 -> 7 s, legacy 44 -> 8 s, gauges.share
  32 -> 8 s, gauges 48 -> 37 s, net 23 -> 2 s. The 4 shards locally at 2 workers each: 106 / 177 / 107 / 138 s, each
  `--ran` check green. **CI estimate** (run 45015118710's per-file times scaled by those ratios): test time 1,609 -> 1,216 s;
  the longest leg (site.test's) ~400 s of test time + ~150 s collect/environment over 2 workers ~4.6 min, + ~1.5 min
  setup: the vitest job's critical path **~21 min -> ~6 min**. To confirm on the PR's first run.
- Left for the other lane / later: D1-D5, C1 (the duplicates lane), S3 (pytest-xdist on a branch), S6, X.

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
