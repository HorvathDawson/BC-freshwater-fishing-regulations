# T667 — Tests that grow with the app: fewer hand updates per release and per feature

Status: IN PROGRESS — slices 1–7 done (slices 1–2 9bdc5ea2e, 5e8d817d9; 3 by T666; 4a/4b 716999ebc, 0f7b29678; 5a c79b4516b; 5b 5618e63bc, 3e933cc04, 34a4e0f61, afb6a5eda; 4c 26535857f; 6 424a4be66; 7 1e924f612); slice 8 steps 1–3 done (the mini world through the real atlas + reach, acd561469, 4a7c92f10; deliver on invented inputs 1cb204a3f; the mini release r-0266d965cfb6 committed and held to a rebuild by content, c9f050557) and step 4's first batch (@app/data line.test.ts on the mini release, 1c1609e51); left (Claude): step 4's rest — the ~36 app test files on `openRegsFixture*` (ui, ui-native) one package per batch, the pipeline logic tests that build toy bundles, the page oracle on the mini release, shrinking the regs fixture to the D2 walks.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T667 | Tests that grow with the app: fewer hand updates per release and per feature (audit and design first, Fable) | IN PROGRESS (slices 1–7 done; slice 8 steps 1–3 done: mini release r-0266d965cfb6 committed c9f050557, step 4 first batch 1c1609e51; left (Claude): step 4 rest — ~36 app test files on openRegsFixture*, toy-bundle pipeline tests, page oracle on the mini release, shrink the regs fixture) | 9bdc5ea2e, 5e8d817d9, 716999ebc, 0f7b29678, c79b4516b, fb91259ef, 5618e63bc, 3e933cc04, 34a4e0f61, afb6a5eda, 26535857f, 1e924f612, 424a4be66, acd561469, 4a7c92f10, 1cb204a3f, c9f050557, 1c1609e51 | owner 2026-11-11; closed-testing run; with T666 |

Source: owner 2026-11-11: "we need to also make the tests more expandable; right now they get progressively more
unwieldy ... a Fable task to improve the tests generally, to naturally grow with the app without too many hand
updates needed every time."

## What
Tests pinned section handles and counts that move with every atlas, frozen references ("page v36" oracles) needed a
hand amendment per approved rule change, live-release tests were red from build to promote, and runners disagreed
(fast-fail's no-build plugin, the chunked runner, a fresh clone). Audit + design: `app-integration/T667-audit.md`
(§4 the slices); the offender list and batches: `app-integration/T667-offenders.md`.

## Owner decisions 2026-11-11 (on `app-integration/T667-audit.md` D1–D6)
- D1: retire the page v36 reference suite by suite, after the inventory shows nothing is lost.
- D2: yes, logic tests run on a small invented release built by the real pipeline; about ten real-water walks stay.
- D3: live counts stop being tests and are printed in the release diff.
- D4: **the guard fails on ALL offenders, no shrinking list; migrating them is the FIRST task of the next chat** (so the
  guard goes in when the ~230 files are clean; until then it runs in report mode only).
- D5: the release commits renumber-only fixture diffs by itself and stops only when an answer changed.
- D6: the smoke set is derived by rule, not hand-listed.

## Done (slices 1–7, slice 8 steps 1–3 + step 4's first batch; commits in Status)
- 1–2 the anchors guard is ON (D4): pipeline 0 of 432 test files, app 0 of 642 offend. `pnpm anchors`
  (`app/tools/check-test-anchors.mjs --strict`, in `pnpm check` and `check:fast`), `python -m pipeline.tests.anchors`
  (in `scripts/fast-fail.sh`), `test_test_anchors.py::test_the_live_tree_has_no_offender`; each failure names the hit
  and what to write instead; scanner rules in the scanners' headers. Helpers: `sectionNames` `.part` / `.partAt`;
  `pipeline/tests/under_test.py` (the one way a pipeline test reaches live data; sections by blue line + metre); the
  release under test is `RELEASE_DIR` else LATEST (`liveRelease` / `namedRelease`, `under_test.release`).
- 3 re-cut against the candidate: by T666 (`sync.mjs` / `taps.py` / `regs_fixture` take `--release`).
- 4a app skip allowlist keyed by helper (716999ebc); 4b answers section versions as relations (0f7b29678); 4c smoke
  set derived by rule (D6): `pipeline/tests/smoke.py` via the root `conftest.py` (a file's `@pytest.mark.smoke` tests,
  else its first test with no `needs_*` / `slow` marker); `smoke.txt` deleted; `test_smoke_coverage.py` pins it.
- 5a one rulings table (c79b4516b); 5b page v36 retired (D1): `app/tools/regs-recut/page-oracle.mjs` runs page_v36.js
  over page blocks `page_blocks.py` cuts from the release under test (Python reference decoder), writes
  `app/fixtures/golden/page-oracle.json` (121 tapped states, rules by DURABLE KEY); `sync.mjs` step 2 cuts it;
  `release_follows` row `page-oracle` has `answers=True` (a change STOPS the release for `--accept-fixtures`). Retired:
  `hydrate.json`, `hydrate.mjs`, `v36-rulings.json`. Hand work per approved ruling: none.
- 6 live counts → invariants + the release diff (D3): `pipeline/deliver/release/live_counts.py` (printed by
  `release.stage_checks`, `checks/<rid>/live_counts.txt`, `checks.counts` on the ledger record; LISTED, never a stop);
  `test_one_water_kind` / `test_review_queue` pins → `alias_problems` / `run_problems`.
- 7 one test-environment contract: the tier decides the world (`pipeline/tests/tiers.py`: a run deselecting every
  `slow` / `needs_*` loads `ci_no_build` itself); `scripts/test-env.sh` (sourced; interpreter `$PIPELINE_PY`, `python`
  in `pipeline-ci.yml`, the checkout's `.venv` locally); `pytest-chunked.sh` prints each chunk's peak memory (the
  60 GB / 111 GB growth, T638, is in the DATA tier, not measured). Pins: `test_test_env.py`.
- 8 the mini release (D2), steps 1–3: inputs `pipeline/tests/fixtures/mini/` (`world.json`, `curated/`, `claims.json`;
  invented ids: blk 900000001+, wbk 800000001+, gnis 990001+, code root 920-123456); builder
  `pipeline/tools/mini_release.py` runs the PRODUCTION commands with `CURATED_ROOT` = the mini tree (atlas →
  check_curated → reach → tile layers → bundle → verdicts → status_index → export → answers → card_check → store →
  release `--no-basemap --no-synopsis`), refuses a build dir inside `data/` and any write to the live dirs
  (`watched_dirs`); `--twice`, `--compare A B`, `--write`, `--check` (BY CONTENT; pipeline CI step "The committed mini
  release matches a rebuild"). Production changes (explicit inputs, no flag = as before): `--coast-gpkg`,
  `load_area_polys(fwa, …)`, `build_registry(…, gpkg=)`, `pipeline.deliver.tiles --obstacles` / `--no-archive`,
  `pipeline.deliver bundle --source` / `--clim` / `--dfo-typed` / `--dfo-cache`, and the export claims
  (`pipeline/deliver/export/claims.py`, `--claims FILE` on export / answers; default `claims.book()`, pinned in
  `test_export_claims.py`; the claims input is a PROPOSAL for the owner). First reach digest 30f0ee55cbcabea7.
  Committed: `app/packages/data/dev/mini-release/` (r-0266d965cfb6, 53 files, 396 KB). Shared pieces:
  `under_test.mini()` (fails naming `conftest.MINI_HINT`), app `openMiniRelease()` / `miniReleaseDir()` /
  `MINI_RELEASE_ROOT` (`@app/data/release/node`), anchors count it as a committed fixture.
- The mini world (pinned by `test_the_mini_world_holds_the_plans_inventory`): 27 entries (17 the mini's own + 10 book
  rows the GLOSSARY reads), 19 sections. Fixture River (45 km, Fixture Lake on it, region line at 25 km, tidal mouth,
  braid blk 900000005, hours window 21:00-05:00 Aug 1-Dec 31, exempts z2's summer closure by name), Twofold Lake (two
  regions), Fixture Lake (Sat/Sun kokanee), Mock Creek (unnamed trib, point cut at its falls, "No Fishing upstream"),
  Stub Creek (lift, moot size clause) with Mockup Creek (added, closed whole), Sample Lake, Mockup Marsh, one
  reservoir, the sea, PFMA 29, gauge `99MA001`; zone rows z1/z2 quotas, z1 single barbless hook, z2 summer closure,
  `zp:steelhead` narrowed to regions 1-2 (its twins r1b/r2b/r4b unresolved: no book-known water), `zp:bait` r5. NOT
  BUILT: "a lake cut into parts" (no live lake has more than one part; Twofold Lake stands in). Mini claims: 14 case
  mechanisms, 2 invented examples, both duties.
- Step 4 first batch (1c1609e51): `packages/data/src/regs/line.test.ts` reads the mini release's `lines` file by stream
  key + metre (no more `openRegsFixtureWithLines`); mutations in `slotAt` / `hasLines` each red.

## Work (left: slice 8 step 4's rest)
1. Move the remaining ~36 app test files on `openRegsFixture*` that test LOGIC onto the mini release, one package per
   batch, each with the mutation that proves it still bites: ui-native WaterMode / WaterMapView /
   WaterScreen.unnamed, ui waterMap, regs-web states, waterGroup, decidedStory, twoRegions, the VM-fixture harness.
2. The pipeline logic tests that today build their own toy bundle → `under_test.mini()`.
3. The page oracle (page-oracle.mjs) cut on the mini too, so the settle / card / describe parities run on invented
   waters with no release on the machine.
4. Shrink `app/packages/data/dev/regs-release/` (64 re-cut commits in six weeks) to the ~10 real-water walks D2 keeps
   (Vedder, Skeena, Fraser, Kootenay …), still cut by sync.mjs; taps.json / page-oracle.json stay on the real release.
- Gotchas: a mutation restored with `cp` within the same second and size leaves a stale `.pyc` (clear `__pycache__`
  after a mutation round); a queued `heavy.sh` wrapper dies if another lane's commit changes `scripts/heavy.sh` while it
  waits (re-queue). A lane that runs only its changed files' tests can miss guards CI runs (`test_curated_writer`,
  `test_test_env`, `test_wsc_relation`, `test_review_vintage`): fast-fail now runs the first three when their files
  change. Queued 2026-11-14, not recorded as run: the slow / `needs_*` tier and a fresh-copy run of the mini tests
  (`t667-callers`).

## Acceptance
- A rebuild with no rule change needs zero hand edits to tests (re-cut by command only).
- An approved rule change needs one edit at the source and a reviewed diff, not per-test amendments.

Depends: T666. Size: L. Fable for audit and design (owner). Part of the closed-testing run.
