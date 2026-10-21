# Implementation progress (IMPLEMENT-HANDOFF.md)

Restart here. Update this file after EVERY step: what is done, what is running, and the exact next action.
Logs from timing runs live in the session scratchpad, which a reboot wipes. The numbers that matter are copied here.

## IN FLIGHT (2026-10-20 evening) — check these first on a restart
- Heavy: (1) DONE 2026-10-20: live `deliver all` (9.3 min): card gate 0 problems, strict vintage agrees, 9 files
  promoted + release data/release/r-6960fe8201e3 (13.89 MB gz). Live Tier 2 data checks: test_release + test_part_geo +
  card-gate chain + feb29 + vintage 53/53; release-perf 13/13 (A5 5.7 ms, accepted with debounce).
  (2) RUNNING after it: tile rebuild into a SIDE folder scratchpad/tiles_next (then run the tile tests, promote
  atlas.pmtiles + atlas.meta.json + layers/*.geojsonl FILE BY FILE into data/generated/tiles; never replace the dir).
- Batch 1 MERGED (cherry-picked, conflicts resolved, Tier 1 green: vitest 861, pytest smoke 192, check.mjs 18):
  T20 803bc30d, T00 40b2c67d, T02 53ea8298, spots-no-sid 2e92ddee, P12b 22ba0b08 + review fix 9c56fa3a, P12a 386fa5bb,
  P12c 3a8ed83d, search debounce 200 ms 3f1602d0. All passed adversarial review (one major fixed: a saved spot's
  estimate named today's season). Worktrees removed.
- S1 DONE (owner 2026-10-20 ask): aa1012ad S1 (photos: expo-image-picker 57.0.20 -> documentDirectory/spots/photos,
  web IndexedDB; lead-up: 7 days Open-Meteo + station daily means placed in band, frozen at capture, refresh backfills)
  + ab0f8ba1 review fixes (last December reaches a January lead-up: feed keeps Dec through Jan, prior years whole +
  yearBand; photo refs clock-stamped). pnpm check 953 pass; smoke pass. Open minors (not fixed): draft photos named
  after the first draft id; web picker hangs where the input has no "cancel" event (Safari <16.4); unit words "m",
  "m3/s" in ui/leadUp.ts not words.ts; two web photo-store instances (an object URL not revoked on delete); 5-day
  archive lag and "most severe" weather code are disclosed deviations. Not verified on a device: picking, downscale
  (needs expo-image-manipulator), real IndexedDB, live Open-Meteo.
- Batch 2 MERGED (wf_7703a540-b9f; a network outage killed implement:p16 and fix:p11): T06 fc69e20a + c764595b,
  F9 68016392 (test_answers_reference RUNS, slow+needs_bundle, and FAILS on purpose: 72 unexplained real_daily rows on
  Kitimat River / Gotchen Lake / Phillipps Lake, needs an answers-lane ruling), P11 e635eed7 c820ea00 7a4da843 (release
  adapters, makeReleaseSource, App boots manifest -> paint -> Shell), merge fixes de2f697b. pnpm check 1076 pass, smoke
  pass. P11 review majors OPEN (see follow-up workflow): no status colours on the default path (manifest names no tiles,
  so the pair is never proven); a tile-only release never updates on native; prune leaves old tiles; no rollback when
  the new set fails to open; the app's regs reads bypass openRegsFor. Full findings: scratchpad b2_*.json (session
  368da779).
- Tile rebuild DONE 2026-10-20 (691 s): tile_version d768d6e135e62b99.ffac7c1eb56e; promoted file by file into
  data/generated/tiles (atlas.pmtiles, atlas.meta.json, 15 layers/*.geojsonl; basemap untouched; old copies in scratchpad
  tiles_prev); data/release/tiles/<version>/ holds atlas + mask + map-assets-1. Release rebuilt (same rid r-6960fe8201e3,
  data unchanged): manifest now names tiles.current/mask/assets; basemap still null (P18 gap). Slow tile tests 2/2,
  test_tiles+vintage 46, test_release 29, release-boot 5, release-perf 13 all pass.
- Batch 3 MERGED (wf_5c95925e-a40): P11 review fixes (pair proven from the drawn tiles' tile_version; native update on
  any moved part + tile prune + rollback + PREVIOUS before swap; regs reads via loader.regionDb; sql.js retry; counts on
  demand), P16 Conditions formatting (+ review fixes: local labelled times, fold, one horizon control, dead forecast
  fetch removed; web shots in handoff/PLAN/app-data/p16/), F9 ruling notes (F9-72-rows.md: both causes are page bugs
  under DECISIONS F1 / F16), T06 follow-up. Lead change 92c3b8fe: Centre disclaimer kept VISIBLE under the chart
  (legal: "wherever a forecast appears"); owner may fold it. pnpm check 1089 pass; smoke pass. OPEN: P16 native
  screenshots (need emulator); Kitimat ladder question for owner (r2 lifted for hatchery ST by kitimat r5).
- Batch 4 RUNNING: F9 detectors (F1 rest item, F16 orphan) so Tier 2 is green; P11 minors (native map assets
  download, transient vs permanent refusal, unproven pairing words, App wiring test, live colour screenshot).
- G1 GESTURES (owner 2026-10-20; ticket handoff/PLAN/app-data/G1-gestures.md) RUNNING (wf_ba76b522-0c1): RNGH; back
  everywhere via one route back (Android BackHandler, web History API, edge swipe only where no system back); one map
  gesture policy; sheets drag to dismiss; sideways swipe on a water's Regs/Conditions/Map steps the part (owner "maybe").
- N1 resumed (agent a8261f149d43eead6) after the outage; told to rebase onto HEAD and use heavy.sh for gradle.
- N1 spike agent (worktree) → handoff/PLAN/app-data/N1-SPIKE.md. REDIRECTED by the owner: ANDROID emulator + WEB
  (react-native-web); iOS deferred, code kept platform-neutral so iOS is a build later.

## FINAL CLEAN-UP PHASE (owner, 2026-10-20) — ONLY after the app is done AND the owner has looked at it and confirmed it
## looks like the prototype
1. Remove the design work from app/design/ (the prototype is in the app by then).
2. Audit handoff/ (repo + the memory project's handoff/) and remove what is no longer needed.
3. Audit pipeline/ for OLD files nothing uses any more (not planned/future work), and remove them.
Do not start any of this early.

## Owner direction (2026-10-20, third round)
- Platform order: ANDROID (mobile) + the WEB app (react-native-web) now; iOS later. Keep everything portable to iOS
  (cross-platform binding, platform code only behind .native/.web). Acceptance on native = the Android emulator for now.

## Owner answers (2026-10-20, second round)
- Native builds + tile rebuild: allowed; keep other work going while they run. Live deliver: do it.
- Search A5 ~5 ms: ACCEPTED, provided the input is debounced well.
- Saved spots: remove the sid; a spot = coordinate + river id (item_id); migrate saved files on load.

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
- P18 part 2 (DONE): `tiles/assets.py` fetches the pinned Protomaps glyphs (every style text-font + the basemap's
  Regular/Medium/Italic, 256 ranges each) + the v4 light/dark sprites ONCE into data/source/map-assets/<pin>/ (13 MB,
  git-ignored; already fetched 2026-10-20), and to_release writes map-assets-1/ + assets.json (per-file sha) into the
  version folder. The tile step fetches before the hour of work. Verified end to end into scratch: archive + mask
  (61 KB) + 768 pbf + sprites = 14 MB; a second release of the same version is refused.
- LEFT FOR P10 (app): point TileEndpoints.glyphs / .sprite / the mask at the release folder (runtime-style.ts already
  takes overrides; the Protomaps CDN is only its fallback). The app still reads the mask from its old place until P10/P11.

## P13 (DONE, except the line target): route state
- ui/route.ts: Route (browse | water stack with `from`), RouteAction, reduce, bottomBarShown, useRoute, STACK_CAP 8,
  one back rule. route.test.ts: 7 (the spec's 5 + browse back + tab reset).
- Shell reads tab / water / spot / add flow / Conditions drill from the route and dispatches every move. The TabBar is
  hidden in water mode. Back from a water now restores the Conditions drill it came from (before, the reach was lost).
  vitest 678/678; the steps other than fixture pass (fixture differs only by P09's schema, committed with P09).
- NOT met: Shell is 709 lines (target < 400); P14 (water-mode shell) and P17 take out the rest.

## P09 (DONE): part_geo / part_mark / water_exit
- `bundle/topology.py`: keep_named (1-based handles via id_at; lake outlines from waterbody_polys.pkl) → write after
  derived.write; rank (km mouth-first), midpoint (half the stem by route measure, point_on), marks (≤ 60 m, split
  name), exit (item_down, nearest receiving part). water_map_export imports order_parts/split_label from it.
- Side bundle (heavy, 4.8 min): 20,461 parts, 774 marks, 17,964 exits; 131 stream parts placed by section length.
  test_part_geo.py 10/10 on it: the Chilliwack + Cowichan ranks and marks (words included) = the prototype's
  watermap.js; Chilliwack → Vedder part 0; every mid_sid in its part and inside its bbox.
- The tests caught an off-by-one: sid N is LINE N of section_handles.txt (1-based). Pinned in a no-data test.
- Offline harness (scratchpad p09/extract.py + offline.py) iterates in seconds: extract named geoms once, then run
  topology.write on a live-bundle copy.
- KNOWN GAP (unchanged): Kootenay Lake's marks are between lake-part ITEMS; decide at P15.
- T22 follow-ups committed: tokens test moved to app/tools (boundaries), fish-sprite.html regenerated.

## P08 stage 1 (DONE): the release writer
- `pipeline/deliver/release/` {build, paint, shards, manifest, __main__}; `python -m pipeline.deliver release` and the
  last step of `deliver all` (staged + promoted with the rest; report_all checks each release manifest's handles).
- Files: paint (status + flow runs/members/basins; paint/1), flow-area (target areas as varints, 0.19 MB; as a sid-keyed
  table they cost 0.9 MB), core (item + size/gauged/has_rows flags, alias, item_alias, item_section WITHOUT ROWID
  (sid, ord) + ord index, place; NOCASE indexes, OSM index dropped), near, conditions (gauge_clim → float16 clim/2),
  water (store common tables + display_rule + akey_region + part_section/part_geo (e6 ints)/part_mark (e6)/water_exit
  (no label)), regs-<9 regions> (cells + every row their frames reach), regs-sid. Each gz, sha-named; manifest app-manifest/2;
  data_release id = r-<sha12 of the file shas> unless given.
- Sizes (gzip): paint 0.93, core 1.27, near 1.99, conditions 2.88, water 2.75, regs-sid 1.22, regions 0.13–0.44,
  flow-area 0.19; TOTAL 13.88 MB (gate 14). The plan's 12.5 estimate left out the panel target areas.
- test_release.py: 3 no-data + 5 needs_bundle (on the P09 side bundle): only manifest files; sha + bytes true; every
  file's handles + format; card stamp in water; core ord = water item_ord; paint = status_index.bin + flow tables
  exactly; water + 9 regions + regs-sid = the regs store row for row; each region holds exactly its akeys' cells;
  two builds identical; size gates.
- Stage 2a (DONE): release/contract.py (exact tables + columns per file, units) → emit_pack →
  app/packages/data/src/pack.generated.ts; `--check` in pnpm check (`pack`) + pipeline-ci; a built release must equal
  the contract exactly.
- Stage 2b (DONE): app/packages/data/src/release/queries.ts (the 11 single-file reads, SEARCH rewritten to core's
  size + pieces flags; the cross-file reads listed for P11/T04 as two keyed reads). app/tools/release-perf.test.ts
  (Tier 2, RELEASE_DIR + BUNDLE): 11/11 parity on sampled keys (climatology within float16), keyed plans = SEARCH
  only, p95 A1 0.018 / A2 0.008 / A3 0.007 / A6 0.007 / A7 0.006 / A9 0.004 ms.
- OWNER DECISION PENDING: A5 search p95 5.4 ms against the 2 ms budget. The app's search is a substring scan (the bare
  name scan alone is 1.9 ms); the budget came from a prefix measurement. Options: accept ~5 ms / trigram index
  (+~1 MB, over the core gate) / prefix + word-prefix only (changes results). The test holds A5 to an 8 ms ceiling.
- The live bundle predates P07/P09: the next human `deliver all` (or pipeline build) writes format + part_geo, after
  which the release step runs in the chain on live data.

## P11 parts DONE: core paint decoder (paint.ts, fixture-held to the Python encoder; 28 ms real decode),
release/loader.ts (lazy, verified, per-region; real Vedder opens core + water + regs-2), release/install.ts (native
install/update core, atomic pointer swap). LEFT: the .web/.native adapters, source.ts routing, hooks (useStatusIndex →
paint, usePairing), App first-run screen, conformance on a fixture release — after the P12 workflow lands and N2.
P20 part DONE: stats.json + record() gone.

## P10 (DONE, except device): endpoints, fetchVerified, manifest diff/pair, publish
- app/apps/mobile/src/config.ts: ONE base (EXPO_PUBLIC_DATA_BASE; default the dev server), App reads ENDPOINTS;
  tools/no-localhost.test.ts guards it.
- @app/data: sha256.ts (pure TS; FIPS vectors), fetchVerified.ts (decoded bytes + sha256, gzip either way; fflate added +
  deps.md + layers.json), manifest.ts (parseManifest refuses unknown formats, diff per sha / tiles / basemap / assets,
  pair keeps the last good pair). release.test.ts 7.
- serve-tiles.mjs serves data/release as R2 will (/release/manifest.json = newest r-*, .gz with Content-Encoding gzip;
  RELEASE / RELEASE_ROOT env). Verified end to end against the scratch release (manifest → core fetched, decoded, verified).
- pipeline/tools/publish.py (HUMAN-RUN; dry-run by default; --yes uploads with R2_* env via boto3): tiles first, data,
  manifest last; immutable cache headers. test_publish.py 2.
- LEFT FOR P11: the resumable native variant (.part files) and deleting engine.web/native's duplicated fetch bodies
  (they still open the dev bundle until the reader switches).
- NOT verified: a device build reading the manifest from the LAN dev server (N1/devices pending).
- CAUTION: my server test ran `pkill -f serve-tiles.mjs`, which also stops a `pnpm tiles` server if one was running.

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
