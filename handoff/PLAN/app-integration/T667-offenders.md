# T667 — the offender list: every test file off its anchor, in batches

**EMPTY since 2026-11-11: all seven batches are done and the guard is ON.** Final counts: pipeline 0 of 432 test
files, app 0 of 642 (`.venv/bin/python -m pipeline.tests.anchors --report`, `node app/tools/check-test-anchors.mjs
--report`). Batches: A1 ac70fc8fa, A2 94ac5fc20, P1 0b6e2c30a, P2 cc74ecccc + 07f859bde + af9cf26e6 + fc167831f,
P3 1ecd2aa07, P4 1a427b0c4, P5 0817d9aa4; the leftovers 2e78ea1cc (status index digests), 249be20d8 (docs-guard test),
30c9138e4 (the hand-fixed rows' whole-text pins, restored), the rules and the switch 9bdc5ea2e, fast-fail 5e8d817d9.
Both scanners now fail on every offender (no baseline, no list of excused files): `pnpm anchors` in `pnpm check` /
`check:fast`, `python -m pipeline.tests.anchors` in `scripts/fast-fail.sh`, and
`test_test_anchors.py::test_the_live_tree_has_no_offender` in the pytest suite. What follows is the record of the
migration as it was cut (the 158 files, the recipes); the scanners are the truth.

Work item 3 of `tickets/T667-tests-that-grow.md` (migrate), under owner decision D4: the guard fails on ALL offenders, so
this list went to zero before the guard was switched on. Cut 2026-11-11 from the two scanners.

## The tools
| | app | pipeline |
|---|---|---|
| scanner (the rule is its header) | `app/tools/check-test-anchors.mjs` | `pipeline/tests/anchors.py` |
| the guard (exit 1 on an offender, with what to write instead) | `pnpm -C app anchors` (in `check` / `check:fast`) | `.venv/bin/python -m pipeline.tests.anchors` (in `scripts/fast-fail.sh`); `test_test_anchors.py::test_the_live_tree_has_no_offender` |
| counts only, exit 0 (for reading) | `node app/tools/check-test-anchors.mjs --report` (`--list`, `--json`) | `… -m pipeline.tests.anchors --report` (`--json`) |
| the allowed helpers | `liveRelease()` / `liveReleaseAt()` / `namedRelease()` (`app/conformance/src/fixtureRelease.ts`), `repoData()`, `openRegsFixture*`, `sectionNames(loader)` `.stream` `.streams` `.lake` `.part` `.partAt` (`@app/data/release/node`) | `pipeline/tests/under_test.py`: `bundle` `verdicts` `status_index` `export_dir` `export` `answers` `atlas` `reach_run` `tiles` `tile_layers` `release` (each takes `request`), `path(name)` for a module-level constant, `curated()`, `generated()`, `sections(request).stream(blk, metre)` / `.lake(wbk)` |
| the env contract | `RELEASE_DIR` = the release under test (the release command's candidate), else LATEST | the same `RELEASE_DIR`, plus the side-build overrides the helper owns (`UI_EXPORT_BUNDLE`, `ANSWERS_EXPORT_DIR`, `ATLAS_BUILD`, …: its header) |

## How to work a batch
1. Batches are DISJOINT by file: one agent per batch, no file is in two. Touch only your batch's files (and the helper,
   if a pattern has no accessor: add it there with a test, say so in the commit).
2. Per file: apply the recipe, run that file (`pytest <file>` / `pnpm -C app exec vitest run <file>`), then the scanner
   with `--list` / `--json` and check the file is gone from it. A file whose heavy tests you could not run (the atlas
   graph, the gpkg) is said so in the commit.
3. Keep what bites: every `need()` / `needs_*` marker, `predates`, the no-silent-skips counts, the why-comments
   `app/tools/vintage-pins.test.ts` asks for, and each test's own mutation. Do not turn a failure into a skip.
4. Never "fix" a hit by rewording it past the scanner (a number built by arithmetic, a path split in two strings). If
   the rule is wrong for a case, change the rule in the scanner header + its test, in a commit of its own.
5. DONE 2026-11-11 (9bdc5ea2e, 5e8d817d9): both reports said 0; `app/package.json` `anchors` runs `--strict`, both
   scanners fail by default (`strict`; `--report` only counts), `scripts/fast-fail.sh` runs the pipeline one with a
   "caught:" line and the app one through `check:fast`.

Kinds: `generated` `curated` `env` `live-const` `path` `pointer` are READS of the live tree past the helper;
`raw-section` `raw-part` `release-id` `digest` are PINS (a position of one build typed into a test that follows a
release). Not scanned, later slices: literal counts on live data (D3), entry ids, format versions typed twice.


## Totals (as cut, before the migration; now 0)

158 offending test files: 140 pipeline (of 425), 18 app (of 638).

| kind | files | hits |
|---|---|---|
| generated | 44 | 76 |
| curated | 42 | 105 |
| env | 78 | 118 |
| live-const | 62 | 95 |
| path | 33 | 60 |
| pointer | 2 | 4 |
| raw-section | 6 | 15 |
| raw-part | 6 | 26 |
| release-id | 0 | 0 |
| digest | 6 | 18 |

| batch | area | files | hits |
|---|---|---|---|
| A1 | App: numbers used as names (conformance, data, ui, ui-native, mobile) | 10 | 41 |
| A2 | App: reads past the helpers (tools, core menu, feeds worker) | 8 | 14 |
| P1 | Pipeline: review and curation tools (`test_curation_review_*`, `test_review_*`, DFO, tidal, names) | 31 | 104 |
| P2 | Pipeline: answers, export, store, release (`test_answers_*`, `test_export_*`, `test_release*`, status index, verdicts) | 31 | 127 |
| P3 | Pipeline: the live curated tree read in place (catalogue, corpus, splits, names) | 23 | 57 |
| P4 | Pipeline: atlas, reach run, tiles, registry (`GENERATED.build()` readers) | 23 | 98 |
| P5 | Pipeline: rulings on the bundle (`UI_EXPORT_BUNDLE or R.BUNDLE` readers) | 32 | 76 |

## A1 — App: numbers used as names (conformance, data, ui, ui-native, mobile)

**Recipe.** `<n> as SectionId` / `sectionLine(<n>)` / `tapSid: <n>` → `await sectionNames(openRegsFixtureWithLines()).stream(<stream key>, <start metre>)` (a lake: `.lake(item, part)`); look the name up once in `app/packages/data/dev/section-lines.json` (`[handle, key, from, to]`). A part number (`partIx: 3`, `f("gnis:2936", 2)`) → `.part(item, lo, hi)` with the part's end tokens (`part.lo_s` / `hi_s` in the fixture's water file), or `.partAt(item, key, metre)` when it has none. A test block that mixes invented sections with the fixture is split in two top-level `describe`s (the scanner scopes pins to the block that reads the release); a "no such handle" probe is derived, not typed. Keep the why-comment `tools/vintage-pins.test.ts` asks for on a line that asserts metres.
**Worked examples (3a0f624fe).** `packages/data/src/regs/line.test.ts` (`sectionLine(316756)` → `names.streams([[COQ, 0], [COQ, 312], [COQ, 6590]])`), `packages/ui-native/src/WaterMode.test.tsx` (`578 as SectionId` → `.stream(354088147, 0)`; `onMap("gnis:3062", 1)` → `.part(...)`; the fixture test moved to its own `describe`), `conformance/src/regs/decidedStory.fixture.test.ts` (Fraser part 14 → `.part("gnis:39325", "region_line:5", "region_line:3")`; its third pin, on the live release, is left for this batch).

| file | hits | kinds |
|---|---|---|
| `app/apps/mobile/src/back.test.tsx` | 2 | raw-section 2 |
| `app/conformance/src/regs/decidedStory.fixture.test.ts` | 1 | raw-part 1 |
| `app/conformance/src/regs/edge.parity.test.ts` | 1 | raw-section 1 |
| `app/conformance/src/regs/inputs.release.test.ts` | 5 | raw-part 4, raw-section 1 |
| `app/conformance/src/regs/twoRegions.fixture.test.ts` | 2 | raw-part 2 |
| `app/packages/ui-native/src/WaterScreen.unnamed.test.tsx` | 1 | raw-section 1 |
| `app/packages/ui-native/src/water/WaterMapView.test.tsx` | 5 | raw-part 5 |
| `app/packages/ui/src/frameView.test.ts` | 11 | raw-part 4, raw-section 7 |
| `app/packages/ui/src/regs.render.test.tsx` | 3 | raw-section 3 |
| `app/packages/ui/src/waterMap.test.ts` | 10 | raw-part 10 |

## A2 — App: reads past the helpers (tools, core menu, feeds worker)

**Recipe.** `process.env.RELEASE_DIR` in a Tier 2 suite → `namedRelease()` (`conformance/src/fixtureRelease.ts`: the release the run named, null when not asked, throws on a name with no release); a suite that should run whenever a release is on disk → `liveRelease()` / `liveReleaseAt()`. A `join(REPO, "data/curated|source/…")` read of a committed file has no app helper yet: these 5 files (the credits / legal page and the feeds worker checking their lists against the repo's data) need one small reader, e.g. `repoData("curated/waters/cuts.json")` beside `liveRelease`, added to the scanner's allowed helpers in the same change — decide once, in this batch, before touching the five files.
**Worked example (3a0f624fe).** `tools/release-placeable.test.ts`: `const REL = namedRelease();` (still skipped by name in `pnpm check`; run with `RELEASE_DIR=… TIER2=1`, 2 passed on r-303b4007a70a).

| file | hits | kinds |
|---|---|---|
| `app/packages/core/src/menu.test.ts` | 1 | path 1 |
| `app/packages/core/src/menu/legal.test.ts` | 6 | path 6 |
| `app/tools/release-boot.test.ts` | 1 | env 1 |
| `app/tools/release-perf.test.ts` | 1 | env 1 |
| `app/tools/search-golden.test.ts` | 2 | env 2 |
| `app/workers/feeds/test/dfo.wording.test.ts` | 1 | path 1 |
| `app/workers/feeds/test/email.test.ts` | 1 | path 1 |
| `app/workers/feeds/test/events.test.ts` | 1 | path 1 |

## P1 — Pipeline: review and curation tools (`test_curation_review_*`, `test_review_*`, DFO, tidal, names)

**Recipe.** `GENERATED.build()` → `under_test.atlas(request)` (module level: `under_test.path("atlas")`); `GENERATED.review / …`, `GENERATED.regs.dfo_salmon` → `under_test.generated().review / …` (then `need` it as today); `CURATED.x` read in place → `under_test.curated().x` — a test that WRITES keeps `curated_root(monkeypatch, tmp_path)` and reads `CURATED` after it (already allowed); `REPO / "data/curated/…"` → the `under_test.curated()` field for that file (rule 36), `REPO / "data/generated/review/…"` → `under_test.generated().review / …`. Never touch `marks.jsonl` or `dfo-suggestions.json` themselves.
**Worked example (e3c59c320).** `test_curation_review_queue.py`: `need(request, "atlas", under_test.atlas(request) / "registry.json", ATLAS_HINT)`.

| file | hits | kinds |
|---|---|---|
| `pipeline/tests/test_basin_areas.py` | 1 | generated 1 |
| `pipeline/tests/test_curation_review_api.py` | 3 | generated 2, live-const 1 |
| `pipeline/tests/test_curation_review_browser.py` | 1 | generated 1 |
| `pipeline/tests/test_curation_review_inputs_sync.py` | 1 | path 1 |
| `pipeline/tests/test_curation_review_q154.py` | 2 | path 2 |
| `pipeline/tests/test_curation_review_reports.py` | 2 | live-const 2 |
| `pipeline/tests/test_curation_review_screen.py` | 3 | generated 2, path 1 |
| `pipeline/tests/test_curation_review_slough.py` | 2 | path 2 |
| `pipeline/tests/test_curation_review_tidal.py` | 6 | generated 1, path 5 |
| `pipeline/tests/test_curation_review_tidal_unregistered.py` | 2 | generated 1, path 1 |
| `pipeline/tests/test_curation_review_vintage.py` | 2 | generated 2 |
| `pipeline/tests/test_dfo_salmon.py` | 17 | curated 3, generated 13, path 1 |
| `pipeline/tests/test_dfo_suggest.py` | 2 | path 2 |
| `pipeline/tests/test_gauge_promote.py` | 1 | generated 1 |
| `pipeline/tests/test_inseason_points.py` | 2 | generated 2 |
| `pipeline/tests/test_marks_never_orphaned.py` | 3 | curated 2, generated 1 |
| `pipeline/tests/test_name_sources_t224.py` | 4 | path 4 |
| `pipeline/tests/test_one_acceptance_gate.py` | 3 | curated 1, generated 2 |
| `pipeline/tests/test_pfma_drainage.py` | 1 | generated 1 |
| `pipeline/tests/test_review_checklist.py` | 3 | curated 3 |
| `pipeline/tests/test_review_closed_areas.py` | 3 | curated 3 |
| `pipeline/tests/test_review_dfo.py` | 1 | curated 1 |
| `pipeline/tests/test_review_export.py` | 3 | curated 3 |
| `pipeline/tests/test_review_gate.py` | 2 | generated 2 |
| `pipeline/tests/test_review_marks.py` | 10 | curated 7, live-const 3 |
| `pipeline/tests/test_review_queue.py` | 8 | curated 3, env 1, generated 4 |
| `pipeline/tests/test_review_screen.py` | 2 | curated 2 |
| `pipeline/tests/test_sync_inputs.py` | 2 | curated 2 |
| `pipeline/tests/test_tidal_gauges.py` | 8 | curated 8 |
| `pipeline/tests/test_tidal_limits.py` | 3 | curated 2, path 1 |
| `pipeline/tests/test_tidal_triage.py` | 1 | path 1 |

## P2 — Pipeline: answers, export, store, release (`test_answers_*`, `test_export_*`, `test_release*`, status index, verdicts)

**Recipe.** `BUNDLE = Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE)` → `BUNDLE = under_test.path("bundle")`; `EXPORT_DIR = Path(os.environ.get("ANSWERS_EXPORT_DIR") or …)` → `under_test.path("export_dir")`; inside a test, `need(request, "bundle", <that expression>, HINT)` → `under_test.bundle(request)` / `.answers(request)` / `.export(request)` / `.status_index(request)` / `.verdicts(request)`; `latest(release_root())` → `under_test.release(request)` (the candidate during a release). A typed digest (`test_release.py`, `test_status_index.py`, `test_method_file.py`) is read from the bundle's `meta` / the manifest instead, or, where the test builds its own store, moved out of the live block.
**Worked examples (ebb22b638).** `test_feb29.py`, `test_answers_23.py`, `test_paint_sync.py` (one of the 13 "red until promote" tests: now reads `under_test.release`).

| file | hits | kinds |
|---|---|---|
| `pipeline/tests/test_answers_25.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_26.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_27.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_28_live.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_display.py` | 2 | env 2 |
| `pipeline/tests/test_answers_keep_answers_live.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_kitimat_steelhead_lift.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_one_kind_group.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_rows_fixes.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_t127_displaced_xref.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_tidal.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_v0.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_v1.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_answers_v22.py` | 5 | env 2, live-const 2, path 1 |
| `pipeline/tests/test_answers_year_strip_live.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_card_gate_in_chain.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_consumer_report_cns.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_derived_tables.py` | 2 | env 1, generated 1 |
| `pipeline/tests/test_export_codec.py` | 10 | env 1, live-const 9 |
| `pipeline/tests/test_export_ui_rules.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_guide_cases_clean.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_guide_examples.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_method_file.py` | 7 | digest 1, env 3, generated 1, live-const 2 |
| `pipeline/tests/test_moments.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_reader_answers.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_regs_store.py` | 4 | env 2, generated 1, live-const 1 |
| `pipeline/tests/test_release.py` | 10 | digest 3, env 3, generated 1, live-const 3 |
| `pipeline/tests/test_release_command.py` | 2 | path 2 |
| `pipeline/tests/test_release_slices.py` | 2 | pointer 2 |
| `pipeline/tests/test_status_index.py` | 10 | digest 2, env 6, generated 1, live-const 1 |
| `pipeline/tests/test_verdicts.py` | 3 | env 2, generated 1 |

## P3 — Pipeline: the live curated tree read in place (catalogue, corpus, splits, names)

**Recipe.** `from pipeline.common.curated import CURATED` + `CURATED.a.b` → `under_test.curated().a.b`; `Path(__file__).resolve().parents[2] / "data/curated/regulations/synopsis"` → `under_test.curated().regulations.synopsis` (and so on per file: the field names are `pipeline/common/curated.py`). Nothing else changes: the tree is in git, so no marker. A test that only checks the path config itself (`test_build_command.py`) may keep its assertion but takes the tree from the helper.
**Worked example (e3c59c320).** `test_names.py`: `load_name_variants(under_test.curated().waters.name_variants)`.

| file | hits | kinds |
|---|---|---|
| `pipeline/atlas/waters/added_streams/tests/test_build.py` | 1 | curated 1 |
| `pipeline/tests/test_anchors.py` | 1 | curated 1 |
| `pipeline/tests/test_base_rules_in_print.py` | 2 | curated 2 |
| `pipeline/tests/test_build_command.py` | 10 | curated 9, path 1 |
| `pipeline/tests/test_catalogue.py` | 7 | curated 6, path 1 |
| `pipeline/tests/test_check_waters.py` | 1 | curated 1 |
| `pipeline/tests/test_entry_id.py` | 1 | path 1 |
| `pipeline/tests/test_extract_baseline.py` | 1 | digest 1 |
| `pipeline/tests/test_gear_model.py` | 2 | curated 2 |
| `pipeline/tests/test_gear_omission.py` | 1 | curated 1 |
| `pipeline/tests/test_licence_records.py` | 10 | path 10 |
| `pipeline/tests/test_licensing_model.py` | 2 | curated 2 |
| `pipeline/tests/test_name_variants.py` | 2 | curated 2 |
| `pipeline/tests/test_other_places_extent.py` | 2 | curated 2 |
| `pipeline/tests/test_registry_flowing.py` | 1 | path 1 |
| `pipeline/tests/test_slough_fold.py` | 1 | curated 1 |
| `pipeline/tests/test_splits_accuracy.py` | 2 | curated 2 |
| `pipeline/tests/test_steelhead_waters.py` | 1 | curated 1 |
| `pipeline/tests/test_stepcache.py` | 1 | path 1 |
| `pipeline/tests/test_t207_on_parts_is_a_note.py` | 3 | curated 3 |
| `pipeline/tests/test_t207_split_confidence.py` | 1 | curated 1 |
| `pipeline/tests/test_t207_split_use.py` | 2 | curated 2 |
| `pipeline/tests/test_validate_catalogue.py` | 2 | curated 2 |

## P4 — Pipeline: atlas, reach run, tiles, registry (`GENERATED.build()` readers)

**Recipe.** `Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())` → `under_test.atlas(request)` (module level `under_test.path("atlas")`); `GENERATED.reaches / …` → `under_test.reach_run(request)`; `GENERATED.tiles` → `under_test.tiles(request)` / `.tile_layers(request)`; `GENERATED.bundle / "bundle.sqlite"` → `under_test.bundle(request)`. A section handle typed as a number → `under_test.sections(request).stream(blk, metre)` / `.lake(wbk)` (none found today: the pipeline names sections by id string). Digests typed in `test_tiles.py` / `test_catalogue_entries.py`: check each — an invented one for a synthetic tile set moves to a non-live test or becomes a counting run; a real one is read from the sidecar.
**Worked example (e3c59c320).** `test_reach_tributaries.py`: `under_test.atlas(request) / "graph.pkl"`.

| file | hits | kinds |
|---|---|---|
| `pipeline/tests/test_added_lakes.py` | 8 | curated 7, generated 1 |
| `pipeline/tests/test_ambiguous_cut.py` | 3 | env 1, generated 1, path 1 |
| `pipeline/tests/test_area_boundary.py` | 6 | curated 3, generated 1, path 2 |
| `pipeline/tests/test_catalogue_entries.py` | 10 | curated 4, digest 4, generated 2 |
| `pipeline/tests/test_curated_paths.py` | 1 | generated 1 |
| `pipeline/tests/test_mu_regions.py` | 1 | generated 1 |
| `pipeline/tests/test_no_slivers.py` | 4 | env 2, generated 2 |
| `pipeline/tests/test_one_data_flow.py` | 4 | env 2, generated 2 |
| `pipeline/tests/test_one_water_kind.py` | 6 | env 2, generated 2, path 2 |
| `pipeline/tests/test_placement_rulings.py` | 2 | env 1, generated 1 |
| `pipeline/tests/test_reach_area_bounds.py` | 2 | env 1, generated 1 |
| `pipeline/tests/test_reach_flow_placement.py` | 2 | env 1, generated 1 |
| `pipeline/tests/test_region_homes.py` | 9 | curated 1, env 4, generated 3, live-const 1 |
| `pipeline/tests/test_section_access.py` | 2 | env 1, generated 1 |
| `pipeline/tests/test_source_pins.py` | 1 | generated 1 |
| `pipeline/tests/test_splits.py` | 3 | curated 2, generated 1 |
| `pipeline/tests/test_t89_rules_that_bind_no_water.py` | 5 | curated 1, env 2, generated 2 |
| `pipeline/tests/test_tile_mvt_ids.py` | 1 | generated 1 |
| `pipeline/tests/test_tiles.py` | 12 | curated 1, digest 7, generated 4 |
| `pipeline/tests/test_tributary_lakes.py` | 2 | env 1, generated 1 |
| `pipeline/tests/test_unnamed_trib_sweep.py` | 7 | curated 1, env 2, generated 2, pointer 2 |
| `pipeline/tests/test_unnamed_water.py` | 4 | env 2, generated 1, live-const 1 |
| `pipeline/tests/test_watershed_parts.py` | 3 | env 1, generated 1, path 1 |

## P5 — Pipeline: rulings on the bundle (`UI_EXPORT_BUNDLE or R.BUNDLE` readers)

**Recipe.** The one line `BUNDLE = str(Path(os.environ.get("UI_EXPORT_BUNDLE") or R.BUNDLE))` → `BUNDLE = str(under_test.path("bundle"))` (drop `import os` when nothing else uses it); the tests' own `need(request, "bundle", BUNDLE, BUNDLE_HINT)` and `predates(...)` stay. A `CURATED` or `data/curated` read beside it → `under_test.curated()`.
**Worked example (e3c59c320).** `test_competition.py` (100 passed).

| file | hits | kinds |
|---|---|---|
| `pipeline/tests/test_authority_scope.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_bundle_lake_area.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_displacement_rulings.py` | 3 | env 1, live-const 1, path 1 |
| `pipeline/tests/test_fix_round_rows.py` | 3 | env 1, live-const 1, path 1 |
| `pipeline/tests/test_gauge_policy.py` | 2 | live-const 2 |
| `pipeline/tests/test_lengths.py` | 3 | curated 1, env 1, live-const 1 |
| `pipeline/tests/test_lift_decisions.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_moot_size_clause.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_own_gauge_continuation.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_part_geo.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_part_labels_continue.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_part_runs.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_part_touches.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_phase3_rulings.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_requirements_g5.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_requirements_restates.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_rules_round.py` | 3 | env 1, live-const 1, path 1 |
| `pipeline/tests/test_rulings_pinned.py` | 3 | env 2, live-const 1 |
| `pipeline/tests/test_see_groups.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_see_pointers.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_snag_duty.py` | 3 | env 1, live-const 1, path 1 |
| `pipeline/tests/test_split_names.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_steelhead_stamp_waiver.py` | 3 | env 1, live-const 2 |
| `pipeline/tests/test_steelhead_streams.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_stream_polygon_panels.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_t100_area_caveat.py` | 4 | curated 2, env 1, live-const 1 |
| `pipeline/tests/test_tidal_pinned.py` | 2 | env 1, live-const 1 |
| `pipeline/tests/test_trout_scope_and_dated_releases.py` | 3 | curated 1, env 1, live-const 1 |
| `pipeline/tests/test_water_continue.py` | 4 | env 2, live-const 2 |
| `pipeline/tests/test_water_dates_any_size_chinook.py` | 3 | env 1, live-const 1, path 1 |
| `pipeline/tests/test_zone_decisions.py` | 1 | env 1 |
| `pipeline/tests/test_zone_release_by_water.py` | 2 | env 1, live-const 1 |

