# Implementation progress (IMPLEMENT-HANDOFF.md)

Restart here. Update this file after EVERY step: what is done, what is running, and the exact next action.
Logs from timing runs live in the session scratchpad, which a reboot wipes. The numbers that matter are copied here.

## Owner answers (2026-10-20)
- Heavy jobs: allowed now for the day-1 baseline (via heavy.sh, one at a time, ≤4 workers).
- Order: **P00 first**, then P05 + P03 (then P06, N1 scaffold).

## §7 First day
| step | state |
|---|---|
| 1 read docs | AGENTS.md + IMPLEMENT-HANDOFF read; APP-PREP §0–§1, MINIMAL-PACKAGING, integration README still to read before P05/P03 |
| 2 log / pause / go-ahead | HEAD 7a326133; UR7 committed; no `heavy-job.pause`; go-ahead given |
| 3 reader files | ALREADY DONE in a4b37eff (`app/design/regs-v11/port/reader/`) |
| 4 baseline | DONE, see Baseline counts |
| 5 golden re-sync | NOT NEEDED: golden.json meta ref 7fb6809f, v38, misses unchanged (4) |
| 6 first tickets | P00 a4ee3a9b, P03 c2d15a05, P05 8cbfd8c7, P06 done → N1 next (then P07, P01/P02/P04/P12/P13/T22 in parallel) |

## P00: test tiers
- [x] measure: pytest fast (durations), pytest slow (collect count only), vitest per file, `pnpm check` per step, check.mjs total + per round, card gate
- [x] pytest `smoke` marker (registered in pytest.ini) on ~1 test per gate/invariant, no live-bundle scans
- [x] vitest smoke (not needed: whole suite 5 s) filter/project
- [x] check.mjs `ONLY=smoke` (NOTE: check.mjs is in the prototype lane, rule 12; P00 asks for it explicitly, so the edit is additive only)
- [x] `scripts/test-smoke.sh`, `scripts/test-full.sh`; one line in AGENTS.md
- [x] commit with a before/after time table

### Measurements (2026-10-20, laptop, through heavy.sh)
- pytest fast (default `-m "not slow"`): **3572 passed, 6 skipped, 264 deselected in 504.8 s (8:24)**, wall 528 s,
  serial (no xdist). 196 files. The top 6 files take 50 %: test_anchors 75 s, test_pfma_drainage 53 s,
  test_curation_review_api 43 s, test_export_ui_rules 37 s, test_steelhead_streams 33 s, test_competition 29 s.
  811 of the fast tests carry a `needs_*` marker. 11 files hold only `needs_*` tests.
- pytest slow: 264 tests (collect only, not run).
- `pnpm -C app check`, per step: boundaries 0.3, platform 0.3, style 0.4, colours 0.3, spinner 0.4, **fixture 8.4 (FAILS)**,
  gauge-policy 0.3, calendar 0.3, deps 0.3, typecheck 0.7 (incremental), test 4.7 s. Total ≈ 17 s.
  - vitest: **58 files, 642/642 passed**. The slowest are check-boundaries.test 2.9 s and agree.render 1.9 s; the rest are < 1 s.
- check.mjs full: **136/136 in 439 s** (7.3 min). By round: round20 94 s, core/unattributed 94 s, 20e 45 s, ur6 26 s,
  18 26 s, 20b 22 s, 20c 21 s, 20h 17 s, 19 16 s, ur7 16 s, ur1 14 s, 20g 13 s, ur5 13 s, 20f 7 s, int1 6 s, ur2 4 s, ur3 4 s.
- card gate (shipped answers): **0 problems**, 15,862 groups on 7,144 distinct cards, 87 s wall.

### P00 result: Tier 1 = 74 s (app check 13 s, pytest smoke 40 s / 185 tests, check.mjs smoke 21 s / 18 checks)
- pytest: repo-root `conftest.py` marks the ids in `pipeline/tests/smoke.txt` (the fastest no-needs test per file, 185 of
  196 files; 11 files have only needs_* tests). A whole-tree `-m smoke` run exits 4 naming any stale id (mutation-checked).
  The default and slow collections are unchanged (3578 / 264).
- vitest: no smoke split needed. The whole suite is 5 s, so Tier 1 runs all of it inside `pnpm check`.
- check.mjs `ONLY=smoke` = no-hand-facts + ur2, ur3, int1 (all 134 states rendered and scrolled), 20f. This is one
  additive line in a prototype-lane file, which P00 asked for.
- The rebuilt dev fixture was committed separately (P03's fixture item), because it blocked `pnpm check`.

## P03: CI gaps (DONE)
- f6332295: the rebuilt dev fixture. APP-PREP calls this regeneration "human-run", but `pnpm check`'s fixture step
  rebuilds it on every run anyway; committing it was the only change. Flagged to the owner.
- app-ci now runs `pnpm check` itself (this adds colours, spinner, fixture, calendar, gauge-policy and answers) with a
  `.venv` from requirements.txt. It also triggers on `schema.sql` / `indexes.sql`.
- pipeline-ci triggers on `app/packages/core/src/*.generated.*` (it was calendar only). `reference/calendar.generated.js`
  is under `pipeline/**`.
- `app/package.json` `answers` script, inside `check`.
- `app/packages/data/dev/fixture-schema.test.ts`: the fixture DDL equals schema.sql + indexes.sql. On the old fixture it
  FAILED (6 missing tables + section_ruleset changed); it passes now.
- Mutation: a hand edit to answers.generated.ts makes `pnpm -C app check` exit 1 ("stale"). Clean run: 59 files / 645 tests.
- NOT verified: a real CI run on a scratch branch (nothing pushed). Open: the fixture writes meta `shed_rule` where the
  bundle writes `trust_bands`; no app code reads either, so it is left for whoever next touches build-fixture.mjs.

## P05: card gate on the shipped answers (DONE, except the full `deliver all` run)
- `card_check.check_shipped` decodes `ui-rules-answers.json` from its bytes (`encode.decode`) and proves every key.
  `check_built` (rebuild, `--items`) is unchanged and shares `check_model`.
- `card_check --all` writes the stamp + report `card-check.json` BESIDE the answers: problems, parts_checked,
  keys_checked, cards, groups, dates, code_sha (all of pipeline/deliver/answers/*.py), answers_sha256. Any old stamp is
  deleted first.
- DEVIATION from APP-PREP: the stamp is a sidecar, not inside `answers.about`. Writing it into the file after checking would
  change the bytes that were checked (and the `.gz`). The sha binds the stamp to the exact bytes instead. The store copies
  it into its meta `card_check`; P08 copies it into the package meta.
- `deliver all`: answers → **card_check** → store. There is also a `deliver card_check` step. The store build refuses
  without a passing stamp for its exact bytes (`require_card_check=False` is used only by test_regs_store's encoding
  fixture).
- Tests: `test_card_gate_in_chain.py`, 5 tests (chain order, failing gate stops before store, stamp refusals, store
  refusal, live mutation: Vedder headline n+1 fails). All 5 FAILED on the old code; all pass now.
- Live: `deliver card_check` → 0 problems, parts_checked 20461 (= the answers' part count), keys 2268, 57 s. A store built
  to scratch carries meta.card_check. The live `data/generated/regs/card-check.json` now exists.
- NOT YET RUN: `python -m pipeline.deliver all --out-dir <tmp>` (the full heavy chain). Batched with P07, which changes
  the same chain (owner: heavy jobs sparingly).

## P06: Feb 29 (DONE)
- One rule: `calendar_spec.reading_day` (Feb 29 → 59) + `weekday_of` (the real weekday) + `cases()`, generated to TS
  (`readingDay`, `weekdayOf`, `CALENDAR_CASES`), JSON, and the page JS (`readingDayOf`, `realWeekdayOf`).
- Readers: app `statusIndex.dayOfYear` → readingDay; Python `calendar.reading_day_of`, used by `status_index.Index.code`
  / `item_code` and `encode.tap_key`. Builders keep `day_of`'s 366 slots.
- Page `page_v36.js`: LEAP_VIEW makes Feb 29 read Feb 28 on its own weekday; the date box keeps 02-29. The script was
  re-inlined into `page_v36.html` (only the script block changed). The prototype `regs-v11.html` has no weekdayOf and
  was not touched.
- Tests: test_feb29.py adds 4 (spec cases; the page's weekdayOf + calendar in node; needs_bundle: no segment starts at
  day 60 and status profile slot 60 == 59; Zymoetz Fri 2036-02-29 → r10 speaks, Thu does not, Thu Feb 29 == Feb 28).
  test_status_index's tiny fixture is now closed on Feb 28 only (Feb 29 must read closed). The app test reads Feb 29
  as 59 and loops over CALENDAR_CASES.
- Before the change, 6 Python tests failed and the app test failed to load; all pass after. Tier 1: 75 s.
- NOT done: the port harness `hydrate.mjs` YEAR 2036 case (prototype lane; T05 covers taps.json parity at 2036-02-29).
  The next golden sync will report page_v36.js `weekdayOf` / `setMd` / `fromBox` as changed functions.

## P07: bundle format + atomic deliver all (DONE, except the heavy live run)
- bundle meta gains `format` = "bundle/1" (`build.BUNDLE_FORMAT`) and `schema_sha256` (schema.sql + indexes.sql). `schema`
  is kept.
- `deliver all` (`_all`): every step writes into `.deliver-staging-*` beside the destination; promotion by os.replace
  happens only after the last step + `report_all(strict)`. A file with no home refuses the promotion. Staging is
  removed on every exit.
- `vintage.Artifacts` + `report_all`: tiles · bundle (+format) · status index · export pair (+rule_ids) · answers ·
  card-check stamp · store (handles, reach, rule ids, answers_sha256). The package joins at P08. `pipeline build` calls it.
- Tests: test_deliver_atomic.py (8: a failing step at 5 points leaves live bytes and mtimes unchanged; a vintage
  mismatch promotes nothing; a pass promotes all; canonical homes) and test_vintage.py +6. All failed before.
- Live report_all: lenient = every artifact agrees; strict refuses only the bundle's missing format/schema_sha256
  (it predates P07; the next bundle build writes them).
- HEAVY ACCEPTANCE RUN DONE (2026-10-20, heavy.sh, 493 s): `deliver all --out-dir <scratch>` passed. Card gate 0
  problems / 20461 parts / 59 s; strict report_all: every artifact agrees; 9 files promoted, staging gone. The answers
  are byte-identical to live (sha 03e9d33b…). The live files were not touched.

## P04 (DONE): the 3 cron workflows deleted; archive/README updated.

## P02 (DONE): feed freshness
- http `stamp` → null for a missing or malformed fetchedAt; `Observations.fetchedAt: number | null`; source.ts no longer
  fabricates `Date.now()`.
- core `STALE_AFTER_MS` = 2 h (the default for `freshness`), and `readingAgeLine(f, observedAt)`, the whole sentence
  ("… · observed … UTC · checked 10 min ago" / "last checked 3 h ago, may be out of date" / "age unknown").
- `Conditions.observedAt`; ConditionsPanel shows readingAgeLine. `useStationReading` re-polls every 5 min (usePoll),
  and `useAsync(…, sameAs)` keeps the last reading during and after a failed refresh.
- Tests: http +3, core +6 (8 failed before), async refresh +2. pnpm check 657/657.

## P01 (DONE): spots store
- `spots/core.ts` `openSpotsWith(fs, name)`: atomic save (.writing → .bak → rename); load prefers a parseable
  .writing, then the file, then .bak; a corrupt file is renamed `.corrupt-<ts>` and `SpotsCorrupt` is thrown; writes
  are serialised (`queue.ts`); `patch(id, f)` changes a spot only if it still exists. Native = expo-file-system
  adapter; web = localStorage adapter (a quota error now rejects).
- ui `useSpots(store)` (awaited save/remove/refresh, `spotSaveFailed` sentence); App uses it; the refresh writes through
  patch only onto the spot as it was read.
- Shell awaits save and delete and shows the error; a tab change with a dirty edit asks (SpotScreen askToDiscard /
  onKeepEditing). SpotCapture awaits onSaved (error shown, sheet stays open); a capture failure goes back to When
  with the reason; When's back goes to the point step (it looped to itself).
- Tests: core 9 (corrupt kept + nothing written, crash between write and rename recovered, torn .writing, concurrent
  puts, remove during refresh, patch on the current spot, .bak, quota rejects, queue order). useSpots 3. SpotScreen +2.
  pnpm check 671/671.
- NOT done: a mounted SpotCapture flow test (it needs the map; covered through SpotScreen + useSpots instead).
- FOUND, not changed: `Spot.section` stores a sid ("only to replay the trace"), against AGENTS 5. Owner decision.

## P21 (DONE): the atlas build no longer writes blk_chains.pkl; guard in test_dataflow_gates. The live copies were
deleted with the owner's §6 approval: atlas/full, atlas/full.prev, and the scratch_a26 symlink (5.1 GB).

## P18 part 1 (DONE, code; the HUMAN tile rebuild is pending)
- `tiles/version.py`: version = "<handles16>.<tilebuild12>", where tilebuild12 = the sha of the archive BEFORE the
  stamp. `pmtiles edit --metadata` writes tile_version / section_handles / labels_registry into the archive; the
  sidecar also gets tile_version. `to_release` copies the archive + bc_outside.geojson (boundary.build) into
  data/release/tiles/<version>/, and an existing version folder is refused (immutable).
- The tile step (both paths) stamps and releases; `--no-release` is available for scratch builds.
- export: `region_id_of` gives one spelling ("7A") on all 18 region features (9 had a name only).
- build.py: `tiles_key` = handles + gpkg stamp + areas.json + tile code (deliver/tiles, atlas/splits). No registry,
  places or corpus. `--labels` forces tiles.
- vintage.report: a registry difference is a NOTE (labels), never a refusal; pairing is on handles only.
- GENERATED.release = data/release (config.yaml), ignored by data/.gitignore.
- Tests: test_tiles +4 fast (inventory snapshot, version id + metadata merge, region_id, the tiles key ignores the
  registry) + 2 slow needs_tiles (version stamped, release has archive + mask; every region feature has region_id). The
  2 slow ones FAIL on today's tiles (as they should) until the rebuild. test_vintage: the registry tests now say "note".
- HUMAN: rebuild tiles, `.venv/bin/python -m pipeline.deliver.tiles --build data/generated/atlas/full`
  (≈ 1 h; via heavy.sh 70). Then: the tile tests (`pytest pipeline/tests/test_tiles.py -m slow`).
- NEXT (P18 part 2): self-hosted glyphs + sprites (map-assets-<n>/) and the style pointing at them.

## N1: native map spike (BLOCKED on the owner, not started)
- It needs a native dev-client build (xcodebuild / gradle: heavy and long) plus a simulator or emulator.
  - On 2026-10-20 `xcrun simctl list` hung for more than 2 min (CoreSimulator not answering).
  - The Android SDK is at ~/Library/Android/sdk (it has emulator/ and build-tools/), but `adb` is not on PATH.
- Asked the owner: go-ahead for the native builds, and whether to fix the simulator / emulator setup.
- Spike rules (APP-PREP N1): throwaway branch, nothing merges; results go in handoff/PLAN/app-data/N1-SPIKE.md.
- Light tickets that can run meanwhile: P01, P02, P04, P12, P13, T22; P07 (it heads packaging; the full
  `deliver all` heavy run is batched with P05's acceptance).

## Baseline counts
- pytest fast: 3572 passed / 6 skipped (unfiltered; matches the UR7 report's 3462 plus the tests added since).
- `pnpm -C app check`: **RED at `fixture`.** `build-fixture.mjs` is deterministic (two rebuilds are byte-identical), but the
  committed `app/packages/data/dev/bundle.sqlite` is stale. The rebuild adds 6 empty tables (item_down, mu_set,
  region_place, section_straddle, trib_nearer, unnamed_part) and changes the `section_ruleset` schema. This is audit #5, so
  it **belongs to P03**: commit the rebuilt fixture there. The tree was restored to HEAD after measuring.
  Every step after `fixture` passes when run alone.

## Not verified
(fill in)
