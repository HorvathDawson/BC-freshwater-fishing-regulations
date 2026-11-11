# T667 — Tests that grow with the app: audit and design

Work items 1 and 2 of `tickets/T667-tests-that-grow.md`. Read-only: no test or product code was changed, no suite
was run. Written 2026-11-11 on `redesign/stream-sections` at c3b9e4f7d (live release r-303b4007a70a, build 27).

## In one paragraph
Most tests are fine. About 6 in 10 test files use only data they build themselves and cost little to keep. The cost
sits in a small group that follows the live data: 233 of 1,053 files (22 %) took 37 % of all test edits and 59 % of
the edits where only a number or id changed. Four habits cause nearly all of it: (1) tests name a section or part by
its number, which moves with every atlas; (2) fixtures can only be re-cut after promote, so tests are red or skipped
in between; (3) a frozen reference page is corrected by hand, in two languages, for each ruling; (4) tests hold a
copy of a number the code already knows (format versions, live counts). Fixing the first two is small and removes
most of the per-release work.

## 1. Audit

### How it was counted
- Files: `git ls-files`, `test_*.py` (pipeline) and `app/**/*.test.{ts,tsx,mjs}`. Class: a regex scan per file for
  what it reads (`needs_*` markers, `GENERATED`/`CURATED`, `liveRelease`, `openRegsFixture`, `taps()`, `hydrate()`,
  oracle names, `fixtureRelease`, `readFileSync` over source). One class per file, the most release-bound one wins.
  Spot-checked on about 60 files and corrected twice; expect a few files per class to be misfiled.
- History: non-merge commits since 2026-09-30 (six weeks, 3,957 commits). A "modification" is one existing test file
  changed in one commit (new files are not counted as cost). A "value-only line" is a changed line that is the same
  as the old one once numbers, hashes and release ids are blanked: a pure re-pin. It undercounts (a pin that also
  changed its comment layout or spans lines is missed). "Test-only commit": changes tests or fixtures and no product
  code. "Re-pin wording": the subject says re-pin, re-cut, stale pin, pins moved/follow, release pins on, renumber.
- Not measured: run time and memory per class (no suite was run), and how many edits were needed versus chosen.

### Size
| suite | files | test functions | lines |
|---|---|---|---|
| pipeline (pytest) | 422 (412 in `pipeline/tests`, 10 added-streams; 21 are `test_curation_review*`) | 5,471 | 100,318 |
| app (vitest) | 631 (the ticket's 604 has grown by 27 already) | 5,671 | 81,527 |

### What each file is tied to, and what it cost in six weeks
| suite | class | files | modifications | per file | value-only lines |
|---|---|---|---|---|---|
| pipeline | synthetic (builds its own data) | 189 | 428 | 2.3 | 37 |
| pipeline | live generated tree (`needs_*`, bundle/atlas/release) | 158 | 1,085 | 6.9 | 186 |
| pipeline | source scan (reads the code as text) | 42 | 142 | 3.4 | 16 |
| pipeline | live curated tree, read in place | 26 | 91 | 3.5 | 26 |
| pipeline | curated tree, copied to a sandbox | 7 | 38 | 5.4 | 0 |
| app | synthetic | 452 | 1,385 | 3.1 | 206 |
| app | source scan (`one-*`, `no-*` gates) | 81 | 302 | 3.7 | 52 |
| app | fixture cut from a release (regs fixture release, taps, VM fixtures, section-lines) | 43 | 379 | 8.8 | 455 |
| app | live release on disk (`*.release.test`, `liveRelease()`) | 25 | 153 | 6.1 | 61 |
| app | frozen synthetic releases (the three from 2026-10-23) | 17 | 87 | 5.1 | 20 |
| app | frozen reference page v36, with amendments | 7 | 60 | 8.6 | 33 |
| app | other generated/curated files | 6 | 37 | 6.2 | 15 |
| app | files since deleted (mostly the parity suites retired in T439) | — | 254 | — | 131 |

Pins found by scan (current tree): release ids typed in 12 pipeline and 39 app test files; raw section ids in 4 of
the 75 release-following app files (only 8 of the 75 use the `sectionNames` helper AGENTS already requires);
literal count asserts in 86 pipeline files (306 asserts; 47 of the files read live data) and 51 app files; corpus
entry ids named in 138 pipeline files (2,349 literals; 78 of the 158 live files).

Environment: 85 pipeline files need the bundle, 42 the atlas, 21 fetched sources, 3 each reach run / tiles / cache.
45 app files branch on the environment; 27 are on the skip allowlist (`app/tools/no-silent-skips.test.ts:48`).

### Hand-update cost
- 811 commits touched pipeline tests, 948 touched app tests. 1,820 + 2,657 modifications of existing test files.
- 213 commits (5.4 %) changed only tests or fixtures. 128 commits (3.2 %) carry re-pin wording.
- 1,238 value-only lines in 214 files across 262 commits. 306 file edits were nothing but value changes.
- The regs fixture release was re-cut and committed 64 times; `taps.json` 35 times; each VM fixture 10–39 times.
- `pipeline/tests/smoke.txt` was hand-edited in 58 commits (its header says "Edit by hand when a file is added").
- Concentration: the top 10 % of files hold 39 % (pipeline) and 45 % (app) of modifications. 86 pipeline and 170 app
  files were never touched after they were written.

### Top 20 offenders
Score = test-only edits + commits with value-only edits + re-pin-worded edits. Deleted files left out.

| # | file | class | mods | value lines | what moves |
|---|---|---|---|---|---|
| 1 | `app/packages/ui-native/src/water/WaterMapView.test.tsx` | cut fixture | 40 | 131 | part numbers, part names, handles |
| 2 | `app/conformance/src/regs/reads.release.test.ts` | live release | 21 | 18 | ids and counts on the live store |
| 3 | `app/conformance/src/regs/waterGroup.fixture.test.ts` | cut fixture | 19 | 21 | section ids, group members |
| 4 | `pipeline/tests/test_review_screen.py` | live curated | 19 | 15 | queue counts (`:160-161`: 122, 1515) |
| 5 | `pipeline/tests/test_answers_tidal.py` | live generated | 28 | 22 | a format version, bumped by hand 22 times |
| 6 | `app/conformance/src/regs/unnamed.release.test.ts` | live release | 13 | 19 | unnamed handles |
| 7 | `app/conformance/src/regs/harness.parity.test.ts` | cut fixture | 17 | 9 | VM fixtures per release |
| 8 | `app/conformance/src/regs/settle.parity.test.ts` | frozen reference | 14 | 20 | amendments per ruling |
| 9 | `app/conformance/src/regs/golden.test.ts` | live release | 36 | 8 | golden meta per release |
| 10 | `app/conformance/src/regs/card.parity.test.ts` | frozen reference | 17 | 9 | amendments per ruling |
| 11 | `pipeline/tests/test_moments.py` | live generated | 20 | 16 | the same format version (`:204`) |
| 12 | `app/tools/regs-web/states.test.ts` | cut fixture | 13 | 11 | state ids, counts |
| 13 | `app/conformance/src/regs/tap.parity.test.ts` | cut fixture | 11 | 12 | per release |
| 14 | `app/packages/ui/src/waterMap.test.ts` | cut fixture | 23 | 58 | part numbers and handles |
| 15 | `pipeline/tests/test_answers_year_strip_live.py` | live + reference | 10 | 5 | amendments per ruling (`:68-76`) |
| 16 | `pipeline/tests/test_review_queue.py` | live generated | 13 | 10 | queue counts |
| 17 | `app/packages/ui-native/src/WaterMode.test.tsx` | cut fixture | 25 | 21 | one handle: 638, 578, 576, 578 (`:208`) |
| 18 | `app/tools/back-layers.test.ts` | source scan | 46 | 9 | follows the code, not the data |
| 19 | `app/conformance/src/regs/inputs.release.test.ts` | live release | 12 | 3 | per release |
| 20 | `pipeline/tests/test_steelhead_streams.py`, `app/packages/ui-native/src/WaterScreen.unnamed.test.tsx` | live / cut | 20 / 12 | 18 / 28 | counts (`:1215`: 57) / sample handles |

Support files that are edited as often: `smoke.txt` (58), `taps.json` (35), `conformance/src/regs/golden.ts` (21),
`oracles.ts` (16), `fixtureRelease.ts` (14), `pipeline/tests/fixtures/t57_trib_sweep.json` (13).

## 2. Root causes

**A. A position is used as a name.** Section handles and part numbers are positions in one build's order.
- `app/packages/data/src/regs/line.test.ts:30,45` — `sectionLine(316756)`, was 316511 one release earlier.
- `app/packages/ui-native/src/WaterMode.test.tsx:208` — `578 as SectionId`; its comment lists four past values.
- `WaterMapView.test.tsx` — `mount("gnis:2936", 2)` became `1` when T517 reordered the Skeena's parts (f3bc6a83e).

**B. Fixtures follow the release, but can only be cut after it is promoted.**
- `app/tools/regs-recut/sync.mjs:21` reads `data/release/LATEST`; so the re-cut is always one commit behind.
- `app/conformance/src/regs/golden.ts:41-48` — a parity suite skips when the release on disk is not the one
  `taps.json` was cut from, so between build and re-cut the parity is not red, it is absent.
- `pipeline/tests/test_paint_sync.py:131-135`, `test_unnamed_trib_sweep.py:179-182`,
  `test_curation_review_inseason.py:121` compare the new build with LATEST (the "13 red tests": 10 + 1 + 1 + 1).

**C. A frozen reference is corrected by hand, once per ruling, in two languages.** Page v36 cannot be re-cut (its
tooling is archived; a refresh was tried and reverted with 275 breaks, 51423bd4e).
- `app/conformance/src/regs/oracles.ts:31,73,96,108,121` — five amendment tables (renames, retired rules, on-parts,
  drawn part, Peace kokanee), plus `settle.parity.test.ts:40`.
- `pipeline/tests/test_answers_year_strip_live.py:68,72,76` — the same rulings again as Python tables.

**D. A test keeps its own copy of a number the code or the data already holds.**
- `pipeline/tests/test_answers_tidal.py:115` — `{"display": 20, "gear": 12, "licence": 7}` with a 20-entry history
  comment; `test_moments.py:204` pins the same 20. A bump is always three edits, and the test proves only that
  someone typed the number twice.
- `pipeline/tests/test_one_water_kind.py:207-230,317` — 137 sections, 117 aliases, 40 oxbows on the live atlas.
- `pipeline/tests/test_review_screen.py:160-161`; `test_steelhead_streams.py:1215,1219`.

**E. Rulings are pinned inside logic tests.** `pipeline/tests/test_region_homes.py:403-412` checks the Nechako and
West Road rows by entry id on the live bundle; the owner's 11-11 answer meant editing the test's loop (43a8bbc48).
The pin is right to exist; it is in the wrong place and has to be found by a red run.

**F. Lists kept by hand beside the tests.** `pipeline/tests/smoke.txt` (one line per test file, 403 lines);
`app/tools/no-silent-skips.test.ts:48` and `pipeline/tests/test_no_silent_skips.py:24` (a skip COUNT per file).

**G. Each runner sees a different world.** A laptop has the builds; fast-fail hides them
(`pipeline/tests/ci_no_build.py`); the chunked runner and `pytest` directly do not; the app has "release present",
"absent" and `REGS_STORE_REQUIRED=1` (`app/conformance/src/fixtureRelease.ts:87`). Two tests failed only under the
plugin on build 27 (a8a1fbc12, 9f81817c8). The suite needs 31 processes because one grows to 111 GB (T638); the
cause is not known, only worked around (`pipeline/tests/conftest.py:80` copies the whole curated tree per sandboxed
test, which is the first thing to measure).

## 3. Design

### What NOT to change
- The 641 synthetic test files (61 %). Their edits are feature edits, 2.3–3.1 per file. Leave them alone.
- The 123 source-scan gates (`one-*`, `no-*`, dataflow gates). They are the repo's rules written as tests and cost
  the same as ordinary tests. `back-layers.test.ts` is the one outlier and follows a busy file, not a design fault.
- Mutation tests, `needs_*` markers, `need()` and the no-silent-skips rule. They are what makes a red run mean
  something; every proposal below keeps them.
- The three frozen fixture releases in `app/packages/data/dev/fixture-release/` (no re-pin edits in six weeks).
- The shape of `taps.json`: fixed questions (water, part, date) in `states.json`, answers re-cut by command. That is
  the model the rest should copy.

### Rules for new tests
1. **One anchor per test, declared by the helper it uses.** (a) data it builds itself; (b) the small frozen release;
   (c) the release under test, through one helper; (d) a generated expectations file. Nothing else reads the
   generated tree or `data/release`.
2. **No positions as names.** No raw section handle, part number, release id, digest or build-derived count in a
   test. Name a section by blue line + metre or water + landmark, a part by its ends, a rule by entry id + rule id,
   and resolve at run time (`sectionNames` in the app; a Python twin over `section_handles`). A name that no longer
   resolves fails saying which name.
3. **A value that follows the release lives in a generated file, never in test source.** The release's re-cut step
   (T666) writes it; a person reviews the diff.
4. **On live data, assert rules, not samples.** "Every alias names exactly one water" instead of "117 aliases".
5. **A ruling is recorded once**, in a table both languages read, keyed by the ruling id.
6. **No number typed twice.** A version or enum is read from its one home; the test checks a relation.
7. **No hand list per test file.** A new test file needs no edit anywhere else.

### Judgement on each idea
| idea | verdict | how it still bites |
|---|---|---|
| Stable keys resolved at run time | Do first. Cheap, the helper exists, the six worst files hold about a fifth of all pure re-pins. Not for tests on their own synthetic data (`99_999 as SectionId` is fine). | A renumbered copy of the fixture must pass; a section moved 100 m must fail by name. Both become one mutation test of the resolver. |
| Release-following values generated by the re-cut | Do, with one condition: the file is cut by the Python reference from the release's output, and the test compares what the APP read path returns. Two independent paths, as `taps.json` does today. A file cut by the code under test would launder. | The re-cut sorts its diff: "renumber only" (handles, digests) versus "an answer changed". An answer change stops for a named acceptance. A mutation in the TS reader must still turn parity red. |
| Whole-release invariants instead of sampled pins | Do for counts (D). Do not use for rulings (E): "the Nechako lifts Region 7's closure only" is a fact about one water and must stay a named pin. | Each invariant gets a planted violation on the small release that it must catch. The old count moves to the release diff as a printed statistic, where a jump is seen at review. |
| One small frozen synthetic release | Do, but last: biggest saving for UI tests (43 files, 8.8 edits each), biggest cost. Built by the real pipeline from a tiny committed curated tree, so a format change is a re-run, not hand surgery. | CI rebuilds it and compares bytes, so it cannot drift from the builder. Real waters stay covered by the release-tier invariants and about ten named conformance walks. |
| References re-cut by command | Page v36 cannot be re-cut. Short term: one amendments table for both languages, each entry self-expiring (they already throw when stale). Longer term: retire v36 where the Python-reference parity (`taps.json`) covers the same answer. Needs an inventory first; see decision D1. | Before any v36 suite is dropped, list what it asserts that the taps parity does not, and keep or port each item with a mutation that proves it. |
| One test-environment contract | Do. One script sets the variables for every runner; unmarked tests ALWAYS run with builds hidden (the fast-fail plugin becomes the default, not a special mode), marked tests run against "the release under test" (candidate during a release, LATEST otherwise). | The existing `need()` failure, plus one test per runner that a planted unmarked read fails there. |
| Tiers and memory | Tier by anchor, not by a hand list: no-data tier = anchors (a) and (b); release tier = (c) and (d), run once per candidate inside the release command. Measure the per-chunk footprint (T638 item 4) before changing how the review tests sandbox. | Tier membership comes from markers, which `need()` already forces to be truthful. |

### Target structure
- `pipeline/tests/` and app packages: logic tests on own data or the small release. No `needs_*`.
- `release tier` (pipeline `needs_*` files + app `conformance/` + `tools/release-*.test.ts`): invariants over the
  release under test, plus named pins read from generated expectations.
- `app/fixtures/golden/` grows into the one home of generated expectations for both suites (taps, hydrate, and a new
  named-pins file for the pipeline's ruling pins). Cut by one command, listed in T666's table.
- One amendments/rulings table (JSON) read by `oracles.ts` and the Python live tests.
- `scripts/test-env.sh` sourced by fast-fail, test-smoke, test-full, pytest-chunked, CI and the release command.

## 4. Migration plan
Each slice proves no coverage was lost the same three ways: (1) the list of test ids before and after
(`--collect-only` / `vitest list`), every dropped id named with its replacement; (2) the slice's mutation list: each
seeded break that failed before fails after; (3) the migrated tests pass unedited on BOTH releases on disk
(r-d14bdb180f55 and r-303b4007a70a). The third check is the ticket's acceptance, tested directly.

| # | slice | size | files | saves |
|---|---|---|---|---|
| 1 | **Guard, as a ratchet** (work item 4). A scanner fails on a raw `as SectionId` / `sectionLine(<n>)` / release id / digest literal, or a read of `data/release`, `GENERATED`, `LATEST` outside the allowed helpers. Existing offenders go in a baseline that may only shrink (as `smoke_pending.txt` does). Its own test plants one offender of each kind. | S | new `app/tools/check-test-anchors.mjs` + test, `pipeline/tests/test_test_anchors.py`, a baseline file, `app/package.json` `check:fast` | Stops growth from day one. |
| 2 | **Names, not numbers, in the app.** Handles and part numbers through `sectionNames`; parts named by their ends. | S | `line.test.ts`, `WaterMode.test.tsx`, `WaterMapView.test.tsx`, `waterMap.test.ts`, `WaterScreen.unnamed.test.tsx`, `waterGroup.fixture.test.ts`, `releaseDir.ts` (part lookup) | These six files hold 270 of the 1,238 value-only lines. |
| 3 | **Re-cut against the candidate** (with T666 items 1–2). `sync.mjs` and `taps.py` take a release dir; the 13 LATEST tests read "the release under test"; the release command sets `REGS_STORE_REQUIRED=1`. | M | `sync.mjs`, `taps.py`, `hydrate.mjs`, `fixtureRelease.ts`, `golden.ts`, `pipeline/tests/conftest.py`, 4 pipeline test files, T666's table | The red-until-promote window, the extra commit and the second fast-fail on every release. |
| 4 | **No number typed twice, no hand lists.** Version pins become relations plus a generated shape ledger (`emit --check`: shape changed without a bump fails). `smoke.txt` derived (a marker in the file, or the first no-data test). Skip allowlists keyed by helper, not by per-file count. | S | `test_answers_tidal.py`, `test_moments.py`, `conftest.py`, `test_smoke_coverage.py`, both no-silent-skips tests | About 60 hand edits per six weeks in `smoke.txt` alone, and 3 edits per format bump. |
| 5 | **One rulings table**, read by `oracles.ts` and `test_answers_year_strip_live.py`; then the v36 inventory (D1). | M | `oracles.ts`, `settle/card/describe.parity.test.ts`, `test_answers_year_strip_live.py`, new table | One edit per ruling instead of 3–5. |
| 6 | **Pipeline live tests: counts to invariants, ruling pins to generated expectations.** Worst 15 files first (`test_review_screen`, `test_review_queue`, `test_one_water_kind`, `test_steelhead_streams`, `test_region_homes` ...). | M | about 47 files with count pins, in batches | Most of the 186 value-only lines in the live class. |
| 7 | **Environment contract and tiers.** One env script; no-build as the default for unmarked tests; footprint per chunk measured, then the review sandbox fixed if it is the cause. | M | `scripts/*.sh`, `ci_no_build.py`, `conftest.py`, `pipeline-ci.yml` | The "fails only in fast-fail" class; possibly the 31-process run. |
| 8 | **Small frozen synthetic release**, then move UI logic tests off the regs fixture release. | L | new tiny curated tree + builder, about 40 app test files, `regs_fixture.py` shrinks | 64 fixture re-cuts per six weeks become rare. |

Order: 1, 2, 3 give the ticket's first acceptance line for the app. 4–6 give the second. 7–8 are structure.

## 5. Decisions for the owner
- **D1. Page v36 as a reference.** Keep amending it, or retire it suite by suite once the Python-reference parity
  covers the same answers? Default: retire, after the inventory in slice 5 shows nothing is lost; one shared
  amendments table until then.
- **D2. Small synthetic release.** UI logic tests would run on about a dozen invented waters instead of the Vedder
  and the Skeena. Default: yes, built by the real pipeline from a tiny curated tree; about ten real-water walks stay
  in the release tier.
- **D3. Live counts** (117 aliases, 1,515 queue rows). Drop them as tests and print them in the release diff, or
  keep them in the generated pins file? Default: drop as tests, print in the diff.
- **D4. Guard strictness.** A shrinking baseline of today's offenders, or fail on all of them now? Default: the
  shrinking baseline.
- **D5. Re-cut acceptance.** May the release commit "renumber only" fixture diffs by itself and stop only when an
  answer changed? Default: yes.
- **D6. `smoke.txt`.** Derive the smoke set instead of listing it by hand (the set may differ slightly from today's
  hand-picked one)? Default: yes, with the guard tests kept in by marker.
