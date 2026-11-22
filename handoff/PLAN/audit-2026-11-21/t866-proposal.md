# T866: trimming the test suites (a proposal; nothing has been removed)

Measured 2026-11-22. Sources:
- **CI app:** app-ci run 45015118710 on main (vitest job 20 min 55 s; 780 files, 9,171 tests, 996 skipped because there is no release data on CI).
- **CI pipeline:** pipeline-ci run 45016556888 on main (`check` job 19 min 18 s; the pytest step 995 s, 8,042 passed).
- **Local:** one run through `scripts/heavy.sh` lane 2. vitest took 209 s (9,142 passed); pytest-chunked on the no-data tier took 573 s over 37 chunks (all passed). Raw files are in this scratchpad (`vitest.json`, `pytest.out`, `gh/`).

## The short version

**Almost none of the time goes to duplicate tests.** It goes to three things:
1. **The website build tests (`tools/seo/`).** They take 671 s of the 1,609 s of app test time on CI (42 %). They build the whole fixture site 19 times. Most of those builds draw 53 water pages and 53 share pictures that the test never looks at.
2. **CI runs everything on 2 workers.** Since 2026-11-18 the app job is capped at 2 workers, and the pipeline job runs as one single process.
3. **A handful of slow single tests.** For example, one real 21 s network-stall timer, and one PDF render test that takes 83 s.

True duplicates exist, but they are few and cheap. Removing them makes landings less brittle (fewer copies of the same pin to update). It saves seconds, not minutes.

| # | Proposal | Tests affected | Time saved (CI) | Risk |
|---|---|---|---|---|
| S1 | **Shard app-ci's vitest job across 4 runners** (`--shard=i/4`, each keeping 2 workers) | none | wall **21 → ~6 min** (≈ 5 min with S2) | low; total runner minutes about the same (+~4 min of setup in all) |
| S2 | **SEO builds a test only reads hubs, snapshot or redirects from use `bare: true`** (15 of the 19 builds) | 0 removed; ~15 build calls in 5 files changed | **~375 s of CPU** (site.test.ts alone 370 → ~110 s); local wall 209 → ~130 s | low; bare output was checked equal (below) |
| S3 | **pytest-xdist on CI** (`-n 4 --dist loadfile`) | none | pytest step **16.6 → ~5 min** | medium (shared state between tests); try it on a branch twice |
| S4 | Make `fetchVerifiedVia` pass `limits`/`sleep` through, as `fetchResilient`'s tests already do | 1 test (net.test.ts) | ~21 s | low |
| S5 | PDF thread-race test: render at a small scale | 1 test (test_review_screen.py) | ~70 s locally, more on CI | low, once its mutation is re-proved |
| D1–D5 | Remove 5 true duplicates (listed below) | ~60 tests (55 of them are one describe) | < 2 s | none, once each mutation is proved |
| C1 | Fold the pipeline's "wording round" files into one card table per (water, day) | 254 tests in 30 files → ~1 table + the unique checks | ~10 s | low; it is a large diff |
| C2 | One shared Chrome per worker for the 9 layout tests | none | ~20–30 s | low |
| X | (later) `isolate: false` for the logic project, minus the 21 files that mock | none | unknown, maybe 1–3 min of CPU | medium |

**Suggested order:** S1 + S2 first (both are mechanical, remove nothing, and together cut app CI from 21 min to about 5). Then S4, S5, D1–D5, and S3 on a branch. C1 is a maintenance clean-up whenever it suits.

---

## 1. Where the time goes

### App, CI (2 workers)

CI reports `Duration 1165 s (transform 24 s, setup 1 s, collect 353 s, tests 1,609 s, environment 177 s, prepare 48 s)`. About 2,200 s of worker time split over 2 workers is the 19.5 min. The job's own setup (checkout, pnpm, Python venv) adds 1 min 24 s.

- **Setup vs assertions:**
  - 24 % of the worker time is collect (importing and transforming modules, once per file) plus jsdom environment start.
  - Inside "tests", the SEO files spend ~95 % in `buildAll(...)`, not in their assertions. Three of them (gauges, gauges.share, legacy) are 100 % `beforeAll`: two full builds each.
  - The shell, render and a11y files are mostly mounting the whole `<Shell>` (~1.4 s per test).
- **By area (CI CPU s):** SEO 671 · all render (jsdom) 613 · Shell-mounting files 254 · headless-Chrome layout files 129.

Slowest 30 app files on CI. The local column is test time only: top-level `beforeAll` hooks are left out, so gauges.share and gauges show 0.

| # | file | CI s | local s | tests | where the time goes |
|---|---|---|---|---|---|
| 1 | `tools/seo/site.test.ts` | 370 | 184 | 26 | 11–12 full `buildAll` calls |
| 2 | `tools/seo/gauges.share.test.ts` | 69 | – | 2 | 2 full builds in `beforeAll` (1 is a probe for ids) |
| 3 | `tools/seo/legacy.test.ts` | 67 | 48 | 6 | 2 full builds (1 probe) |
| 4 | `tools/seo/appLinks.test.ts` | 66 | 28 | 13 | 2 full builds |
| 5 | `tools/seo/gauges.test.ts` | 66 | – | 8 | 2 full builds (1 probe) |
| 6 | `tools/water-map-steps-layout.test.ts` | 55 | 39 | 1 | jsdom renders + headless Chrome |
| 7 | `ui-native/src/Search.shell.test.tsx` | 47 | 40 | 34 | Shell mounts |
| 8 | `ui-native/src/regs/a11y.test.tsx` | 37 | 29 | 39 | 3 sizes × every tab and sheet |
| 9 | `apps/mobile/src/back.test.tsx` | 35 | 28 | 26 | Shell mounts |
| 10 | `ui-native/src/a11y.shell.test.tsx` | 29 | 26 | 56 | Shell mounts |
| 11 | `ui-native/src/regs/palette.render.test.tsx` | 29 | 25 | 184 | palettes × golden pages |
| 12 | `tools/style-valid.test.ts` | 27 | 16 | 8 | MapLibre style validation × themes |
| 13 | `ui-native/src/WaterMapFraming.shell.test.tsx` | 24 | 19 | 3 | Shell mounts |
| 14 | `data/src/net/net.test.ts` | 23 | 23 | 32 | **one test 21 s: a real stall timer** (S4) |
| 15 | `tools/regs-text-scale.test.ts` | 23 | 30 | 3 | Chrome layout |
| 16 | `tools/water-frame-swipe.test.ts` | 21 | 14 | 2 | Chrome layout |
| 17 | `ui-native/src/Search.close.shell.test.tsx` | 18 | 15 | 8 | Shell mounts |
| 18 | `data/src/release/install.test.ts` | 16 | 8 | 9 | real hashing of a .part file |
| 19 | `ui-native/src/WaterMode.test.tsx` | 13 | 8 | 9 | mounts |
| 20 | `ui-native/src/Search.keys.shell.test.tsx` | 13 | 11 | 12 | Shell mounts |
| 21 | `tools/seo/topics.test.ts` | 13 | 7 | 6 | already bare builds |
| 22 | `ui-native/src/Spots.shell.test.tsx` | 12 | 7 | 11 | Shell mounts |
| 23 | `ui-native/src/closingControls.test.tsx` | 12 | 6 | 65 | mounts |
| 24 | `data/src/release.test.ts` | 12 | 8 | 10 | sha256 / inflate at every slice size |
| 25 | `ui-native/src/regs/OnePicture.test.tsx` | 12 | 9 | 2 | mounts |
| 26 | `ui-native/src/regs/TroutCard.test.tsx` | 11 | 5 | 21 | mounts |
| 27 | `tools/water-frame-layout.test.ts` | 11 | 13 | 2 | Chrome layout |
| 28 | `ui-native/src/water/oneLoadPath.test.tsx` | 10 | 5 | 3 | mounts |
| 29 | `ui-native/src/WaterScreen.regs-props.test.tsx` | 9 | 5 | 7 | mounts |
| 30 | `apps/mobile/src/surfaces.test.tsx` | 9 | 6 | 2 | mounts |

- **The top 10 files are 52 % of local test time, and the top 30 are 73 %.**
- **Local only (live release, skipped on CI):** `conformance/.../slices.release` 129 s, `t800.release` 110 s, `tools/one-part-one-page` 93 s, `tools/seo/t670.web.release` 77 s, `unnamed.sample.release` 64 s. On the laptop, `site.test.ts` (184 s) is the critical path of the whole 209 s run. The next one (`slices.release`, 129 s) is what S2 would leave as the floor.

### Pipeline

- **CI:** one process, 995 s for 8,042 tests. Job setup before pytest takes 2 min 20 s, of which `emit --check` is 76 s.
- **Local (chunked):** 573 s. Tests of 0.3 s or more add up to 433 s, and only 33 s of that is fixture setup. The rest is collection, imports and ~7,800 fast tests.
- **Setup vs assertions:** setup is small here. The cost is in a few heavy test bodies.

Slowest 20 pipeline files (local, setup + call, tests of 0.3 s or more):

| # | file | s | main cost |
|---|---|---|---|
| 1 | `test_review_screen.py` | 90 | `test_pages_render_from_many_threads_at_once` **83 s**: 48 serialized PDF renders at scale 1.0 (S5) |
| 2 | `test_dfo_live.py` | 42 | many ~4–7 s tests (feeds the live fetch logic) |
| 3 | `test_dfo_vectors.py` | 39 | `test_the_committed_vectors_are_what_the_python_reads_today` 19.5 s |
| 4 | `test_dataflow_gates.py` | 26 | source scans |
| 5 | `test_release_follows.py` | 23 | `test_the_tests_that_compared_with_latest_are_found_by_the_rule` 19.6 s (scans every test file) |
| 6 | `test_curated_paths.py` | 19 | `test_no_module_writes_to_a_literal_output_path` 18.6 s (AST scan of the tree) |
| 7 | `test_review_marks.py` | 17 | renumbering the real corpus (10 s) |
| 8 | `test_regs_store.py` | 15 | shard dictionary training (9.4 s) |
| 9 | `test_staging_scripts_fail_safe.py` | 13 | shell scripts run in a sandbox |
| 10 | `test_mini_release.py` | 13 | 12.4 s setup: builds the mini chain twice |
| 11 | `test_deploy_gates.py` | 10 | scripts |
| 12 | `test_synopsis_pages.py` | 9 | 8.7 s setup: reads every PDF footer |
| 13 | `test_curation_review_safety.py` | 9 | CORS test 8.5 s (one param) |
| 14 | `test_trout_section.py` | 8 | |
| 15 | `test_curation_review_writes.py` | 8 | build-cache test 8.3 s |
| 16 | `test_t810_c2_gaps.py` | 8 | 8.3 s setup |
| 17 | `test_answers_28_card_solver.py` | 6 | fast == brute on every query |
| 18 | `test_dfo_wording.py` | 6 | |
| 19 | `test_review_proposals.py` | 5 | |
| 20 | `test_one_data_flow.py` | 5 | `test_no_closure_test_is_spelled_twice` 4.6 s |

- **Chunks 28 and 33 (161 s and 155 s) are half the local run.** They hold the dfo, dataflow, release and review-screen files.
- **Several of the slow ones are source scanners that each walk the whole tree:** curated_paths, release_follows, dataflow_gates, test_anchors, one_data_flow. One shared parsed-tree cache per session (an `lru_cache` over `ast.parse` of each file) would cut most of their ~70 s. That is an option, not a must.

---

## 2. True duplicates (same guarantee, same input, same oracle)

Each removal must be proved before it lands: the kept test fails under the mutation the removed one caught.

| # | Remove | Keep (stronger) | Why it is a duplicate |
|---|---|---|---|
| D1 | `app/conformance/src/regs/inputs.release.test.ts:187-226`, the whole "T810 M" describe (55 tests) | `app/conformance/src/regs/trout.release.test.ts:128-150` ("T810 N") | Same fixture (`openTroutFixture`), same `loadRegsInputs` call per vector, same `expect(inp.trout).toEqual(c.frame)`. N also checks the VM, the lead and `reports`. **Move `expect(inp.s).toBe(c.s)` into N first**: it is the only assertion M has that N lacks. The `sectionOf` helper exists twice too. |
| D2 | `inputs.release.test.ts:214` "the vectors hold tables and null cells both" | `trout.release.test.ts:129` (same title) | Same file, same two counts. It goes with D1. |
| D3 | `app/conformance/src/regs/card.parity.test.ts:35` "the oracle: page v38's code…" | `settle.parity.test.ts:37` | Asserts a subset (version + year + state ids) of the same `pageOracle()` meta. The kept one also checks generator, page and the golden id. |
| D4 | `pipeline/tests/test_answers_r13_wording.py:96` `test_g_no_hook_clause_without_a_hook_restriction` | `test_answers_r12_wording.py:167` `test_w5_gear_sentences_have_no_em_dash` | Same 4 `R11._rules(...)` calls with the same arguments, and the same 4 exact-string asserts. Each one's trailing "no em dash" / "no 'any hook'" check is implied by the exact strings. Merge the two trailing checks into the kept test. |
| D5 | `pipeline/tests/test_one_data_flow.py:429` `test_promote_refuses_an_unfinished_candidate` | `test_atlas_inputs.py:282` (same name) | An empty directory refused with "not a finished atlas". The kept test covers both refusal branches (missing registry; no or partial COMPLETE.json). `dry_run=True` cannot matter, because the refusal comes before it (`pipeline/atlas/promote.py:201-208`). |

**Duplicated assertion blocks inside otherwise unique tests.** Trim the copy, keep the test:
- `test_answers_r20_header.py:65-68` re-asserts the BELLA_TRIB "own" tier's `says` and `line`, exactly as `test_answers_r18_tree.py:157-163` does. The r18 test also checks the card.
- The Vedder card's headline, total line and share strings are pinned in 4 files each (`test_answers_25/26/27/28_card/r20f/ur1/ur2`). There are 27 such repeated literal asserts across `test_answers_*`. This is what C1 removes.

**Searched and not found:**
- No test body is byte-identical (AST) to another anywhere in the pipeline.
- Only 12 app test titles repeat across files, and apart from D1–D3 each one guards a different subject.

---

## 3. Overlaps to combine

- **C1: the pipeline wording rounds** (`test_answers_r10…r20i`, `ur1…ur7`, `23/25/26/27/28_*`: 30 files, 254 tests).
  - Each round added a file that re-pins the whole current card of the same few waters (Vedder, Bella Coola and its trib, Okanagan, Kootenay, Cowichan) on the same day (Oct 10), through `answers28_fixture.card(water, md)`.
  - Proposal: one table `{(water, day): {field path: expected words}}`, asserted once per field. Keep each round's unique logic tests (mutations, `check_card` refusals, `fold_words`, `share_place`) as they are, moved into one or two files by subject.
  - Time saved is small (~10 s; `F.card` is cheap). The point is the ticket's complaint: a wording change today fails 3–4 files that pin the same sentence.
- **C2: the headless-Chrome layout family** (`tools/*-layout`, `-swipe`, `regs-text-scale`, `style-valid`; 129 s on CI).
  - Each file already batches its pages into one `wide-measure.mjs` Chrome. A shared Chrome per worker (a `globalSetup` that starts one, with files connecting over CDP) would save only the launches, ~20–30 s. Low priority.
- **Not recommended: merging the `Search.*.shell` files** (4 files, 60 tests, ~85 s CI) into fewer mounts. Each test drives a fresh Shell, and sharing one mount couples their states. The only cheap merge there is their identical `vi.mock("./MapScreen")` stub, made into one helper (no time saved).

---

## 4. Cheap speedups that remove nothing

| Speedup | Expected saving | Risk and how to check |
|---|---|---|
| **S1. Shard app-ci vitest**: `strategy.matrix.shard: [1,2,3,4]`, `pnpm run test -- --shard=${{ matrix.shard }}/4`. `app ok` already reads `needs`, and a matrix leg's failure fails it. | 21 min → ~6 min wall (2,200 s of worker time / 8 workers + 1.4 min setup). The leg holding `site.test.ts` is the long pole until S2. | **Low.** vitest shards by file count, not duration, so the legs are uneven. Each leg pays 1.4 min of setup (cache pnpm, and skip the venv in the legs that need no Python). Keep `maxWorkers: 2` per leg (the onTaskUpdate timeout stays avoided). |
| **S2. `bare: true` for SEO builds that read no water-page HTML or share PNG.** Checked on the fixture: a bare build takes 1.4 s vs 6.7 s full (outside vitest), and gives an **identical `snapshot.waters`, identical water-page hashes in `snapshot.pages` (53/53), identical hub HTML, water descriptions, `_redirects` and sitemap**. It only skips the panes and the PNGs. | ~15 builds × ~25 s ≈ **375 s of CPU on CI** (23 % of app test time). `site.test.ts` 370 → ~110 s. | **Low.** Candidates: `site.test.ts:139, 189, 198, 204, 226, 295, 320, 331, 335, 337`; the probes at `gauges.test.ts:20`, `gauges.share.test.ts:16`, `legacy.test.ts:37`; `legacy.test.ts:43` and `appLinks.test.ts:55` after a read of their asserts. Keep full: `site.test.ts:29` (panes, budget, PNG sizes), `gauges.share.test.ts:32` (share cards), `gauges.test.ts:33`, `appLinks.test.ts:16`. Add one pin to `site.test.ts`: "a bare build's snapshot, hubs, redirects and sitemap equal the full build's", so the swap can never hide a difference. |
| **S3. pytest-xdist on CI** (`pytest-xdist` into requirements and constraints; `-n 4 --dist loadfile`) | 995 s → ~300 s (4 vCPU) | **Medium.** Session fixtures run once per worker. `backend_mods` pops `sys.modules`, but that stays inside a file with `loadfile`. The smoke and no-silent-skips hooks run on each worker. Prove it with 2 green runs plus `test_no_silent_skips` / `test_smoke_coverage` under `-n`. A safer fallback: split the files into 3 matrix jobs by path hash (~2.3 min of setup each). Locally, `pytest-chunked.sh` stays as it is (memory). |
| **S4. `fetchVerifiedVia` takes `limits`/`sleep`** (`app/packages/data/src/net/verified.ts:40`), and `net.test.ts:307` passes `FAST`/`noSleep` as its neighbours at :198–246 do | ~21 s | **Low.** The test waits out the real stall timer today. Same assertions. |
| **S5. PDF thread-race test at a small scale** (`pipeline/tests/test_review_screen.py:306-343`: 8 threads × 3 renders × 2 routes at scale 1.0) | 83 s → ~10 s | **Low.** The race is about concurrent access, not pixels. Keep the thread count. Re-prove that removing `PDFIUM_LOCK` still fails at the small scale before landing. |
| **S6. Source scanners share one parsed tree per session** (curated_paths, release_follows, dataflow_gates, test_anchors, one_data_flow) | ~40–60 s locally | **Low to medium.** The cache must key by path and mtime so a test that plants a file still sees it. |
| **X. vitest `isolate: false` for the "logic" project**, keeping the 21 logic files that `vi.mock`/`stubEnv` in an isolated project | collect is 353 s of CPU on CI, part of it logic. Guess: 1–3 min of CPU. | **Medium.** It leaks module-level state between files. Try it on a branch: the full suite twice, plus once with `--sequence.shuffle`. |
| **pool: `threads`** instead of the default `forks` | unknown, often 10–20 % of collect | Low to medium (`node:sqlite` works in threads; no test calls `process.chdir`). Measure before adopting. |

---

## 5. Never remove (these look redundant but are not)

- **`WaterScreen.outside.test.tsx:36` vs `regs/Edges.test.tsx:49`.** They have the same `keep-outside` text and the same `regs-content` children assert. Edges proves RegsScreen draws nothing when it is given no Gear, Methods or Licence slot. WaterScreen.outside proves that WaterScreen hands it none. Each holds half of the guarantee.
- **`a11y.shell.test.tsx` vs `regs/a11y.test.tsx`.** Disjoint surfaces: everything outside the Regulations view, vs the Regulations view and its sheets at 3 sizes.
- **`tools/update-service.test.ts` vs `packages/data/src/update/service.test.ts`.** Both cover metered, SHA mismatch and prune. One is the service over a mocked origin, the other is the same service over the real `bootRelease` (the real stage, apply and prune). They are the unit and integration halves.
- **`packages/data/src/feed/inseason.test.ts:59,82` vs `packages/ui/src/inseason.render.test.tsx:119,147`.** Same error strings. One pins that the parser says it, the other that the UI shows and reports it (the no-silent-fallbacks guard).
- **Each parity file's "`… runs against a paired release — SKIPPED: …`" test** (card, settle, inputs, tap…). Each one is that file's no-silent-skip reporter.
- **The `tools/one-*.test.ts` "each pattern catches the spelling it replaced" tests** (same title in 4+ files). Each is the mutation pin of its own scanner (one-colour pathway and similar).
- **The SEO full builds kept in S2, and the thread-race test in S5.** Make them faster, but never drop them: the pane, budget and PNG checks and the pdfium crash guard exist only there.
- **`test_atlas_inputs.py:282`**, the D5 keeper, and every `MUTATION` / `planted` test in a family proposed for folding (C1). They move; they are not deleted.
