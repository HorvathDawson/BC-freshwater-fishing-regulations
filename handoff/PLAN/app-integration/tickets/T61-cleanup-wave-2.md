# T61 — Cleanup wave 2: merge old pathways and duplicates

Status: OPEN. Source: handoff/PLAN/audit/PLAN.md §2 "Wave 2" (items 2.x), with the owner's corrections: keep
stocking / bathymetry / salmon code as "prepared"; prove every script unneeded before deleting it.
Includes the ONE name normaliser (owner accepts a reviewed binding diff), retiring the dev bundle + riffle fixture
(N6), finishing P20 (no regs.sqlite, N5).

## Items (audit PLAN.md §2 wave 2; checked against git 2026-10-22)
- ALREADY DONE, do not redo: 2.5 water_continue made by the bundle (a6aa4d86); N7 deliver key covers the raw gpkg and
  clim.json (b274cd3a); N8 the atlas records the matches.json it cut at (f9ff2207).
- Open: 2.1–2.3 dev-bundle pathway + riffle fixture (N6; then delete riffle.html); 2.4 no regs.sqlite (N5); 2.6–2.7
  gauge logic homes; 2.8 one name normaliser (binding diff first); 2.9 varint writer; 2.10 tolerances; 2.11 one
  pipeline entry + **climatology as a build stage (N9)**; 2.12 tools/ layout; 2.13 silent `except Exception`;
  2.14 glossary into the bundle; 2.15–2.17 app helpers and tools' CDP copy; 2.19 colour-mode split documented (until
  T74); 2.20 Engine module; 2.21 one map checker.
- With 2.3/2.17: move `pipeline/deliver/answers/reference/page_v36.*`, `convert.py`, `page_data.py` out of `pipeline/`
  (owner call 2: archive the prototype answers reference; its v35 harness already went in 36a491ac) after repointing
  `conformance/src/regs/ruleSources.ts`, `taps.py`, `tools/check.mjs` and the `test_answers_*` readers.

- From the unification audit (2026-10-22), covered here, not new tickets: 2.8 — DFO suggestions key 364 names
  differently (include them in the one normaliser; T114 adds the app search key). 2.10 — the 150/200/250 m confluence
  tolerances plus one `at_cut()` predicate. 2.16 — core `dayOfYear` is a READING day (Mar 1 = 61) while `shared.ts` and
  `leadup.ts` use the calendar day: name them `readingDay` and `calendarDayOfYear`, never merge naively (one axis shifts
  a day). 2.x old pathways — delete the dead `useHydrograph`.

Acceptance: each item's acceptance from the audit plan; binding diff reviewed where bindings move; strict + smoke.

## Outcome (lane C)

2026-10-23, commits 0a24db3a 63c54fd6 93d97039 aca74919. Items 2.4 (P20 / N5) and the page v36 archive (owner call 2).

- **2.4 no regs.sqlite.** Measured first on r-46595c88e76e: water + the nine regs-<region> shards + regs-sid
  reassemble into the live regs.sqlite table for table, and with the store's own meta the digest is the store's
  (939b036a3fa4eff6). So `pipeline/deliver/store/release.py` `province_store(release)` puts the store back together in
  memory from the release files and is every Python reader's source; the release step now writes the store's whole meta
  into water as `store_meta`, so a reassembled store is held to its `store_digest` (older releases: no digest to check;
  `answers_bytes()` still proves the answers). `deliver all` builds regs.sqlite in staging for the release and keeps
  none; `--debug-json` keeps a copy (`--out-dir`, else `store.DEBUG_OUT` = `data/generated/debug/regs.sqlite`).
  `store.OUT` is gone, `release --store` is required, `pipeline build`'s deliver outputs end at `data/release/LATEST`,
  vintage's canonical artifacts name no store. Readers switched: taps.py (`--release`), verify_map.py
  (`REGS_RELEASE`), sync.mjs, conformance `golden.ts` (pairing = taps.json `answers_sha256` vs the release's water;
  `provinceStoreAt` gone), `blob.release` / `reads.release` (`provinceStoreOf`: the reassembled store in a temp file),
  test_release (builds its own store in tmp + round-trip and mutation test), test_unnamed_trib_sweep, test_lake_facts.
  Dropped as tautological now: store.release's "every dictionary is the province store's" and packages/data
  blob.test's regs.sqlite half (both covered by the round trip / blob.release over the release). taps.json re-cut from
  the release: states identical; `meta.store` gains `answers_sha256`, `store_digest` is null until a release with
  `store_meta` is cut. Nothing live reads regs.sqlite; `deliver all` still writes it transiently in staging (the
  release step's input).
- **Page v36 archived.** `page_v36.html`, `page_v36.js`, `calendar.generated.js` → `app/design/regs-v11/reference/`
  (live tests still read them: conformance ruleSources/oracles, port tooling, regs-v11 check/snap, emit-fish's FISHART);
  `page_data.py`, `convert.py`, `page_v35_inputs.json` → `archive/pipeline/answers-reference-v36/` (nothing runs them;
  the page is frozen, so settle's ORACLE_PREDATES stays until the oracle is retired). Deleted as prototype-only (grepped
  the frozen page script or ran the archived converter): `test_reference_convert.py`; test_answers_25 f14 page line,
  test_answers_26 page-shows-card + the page half of line-count-says-rod, test_answers_27 group-line paragraph,
  test_answers_28_card checklist, test_answers_v22 d, test_feb29's reference-page Feb 29 (smoke.txt and RULINGS P1/P8
  pinned-by updated).
- **Checks.** `REGS_STORE_REQUIRED=1 pnpm -C app check` (heavy.sh): rc 0, "Test Files 365 passed | 4 skipped (369)",
  "Tests 8209 passed | 22 skipped (8231)" (the skips are tools perf/screenshot suites). Targeted pytest: test_release,
  test_vintage, test_deliver_atomic, test_card_gate_in_chain, test_regs_store, test_build_command, test_lake_facts,
  test_unnamed_trib_sweep::test_every_unnamed_section_has_a_page all pass. Seen failing, not touched here:
  test_dataflow_gates gate 4 (`app/packages/core/src/regs/edge.test.ts:100` spells a calendar table),
  test_entry_id (inline entry-id parsing in `pipeline/regs/review/`), test_fixture_release bundle-half table list.
- **Owner run owed:** the next `python -m pipeline.deliver all` (or `pipeline build`) cuts a release carrying
  `store_meta`; then `node app/design/regs-v11/port/sync.mjs` (or `.venv/bin/python app/design/regs-v11/port/taps.py`)
  fills taps.json's `store_digest` again.

## Outcome (lane A)

App-side items 2.1, 2.2, 2.3, 2.15, 2.16, 2.17, 2.19, plus `useHydrograph`, are done. Commits on the lane branch:
1193af74 (2.1–2.3), 92b31e0a (docs), a2216983 (2.15), b477529d (2.16), ea63325e (2.17), 2951fc4b (2.19),
d3d6ace7 (dev fixture schema test), f18c5634 (pipeline follow-ups).

- **2.1 deleted:** DATA_SOURCE, useDevBundle, ENDPOINTS.bundle, ui devStatus / statusIndexUrl / loadStatusIndex,
  the `dev` PairingInput, makeBundleSource (data/src/bundle/source.ts + its test), the engine.* drivers + bundle/open,
  RegulationsPlaceholder (a WaterScreen with no loader now says "Regulations unavailable: no data release is open"),
  useHydrograph (+ axisLabels), the start stage "bundle", and the serve-tiles routes /bundle.sqlite, /province.sqlite,
  /status_index.bin and /feeds/gauge. core decodeStatusIndex stays.
- **2.2:** db.ts, shared.ts, queries.ts, the node driver and releaseDir.ts now live in data/src/sql/ (layer
  `packages/data/src/sql/drivers`, node only). BundleSourceOptions is now `SourceOptions` (release/source.ts).
  `@app/data/bundle*` exports are replaced by `@app/data/sql/node` (`openSqlite`, `openInMemory`). Queries that only the
  dev reader used are deleted. The stocking / charts / releases SQL is kept and marked PREPARED, NOT FED.
- **2.3:** dev/bundle.sqlite, build-fixture.mjs, the `fixture` step of pnpm check, dev/fixture-schema.test.ts and
  design/riffle.html are deleted. Tests now read **committed fixture releases** in
  `app/packages/data/dev/fixture-release/` (plain / lakeGauged / conformance, ~110 KB each). These were cut once by
  `pipeline.tools.fixture_release` from the old dev bundle and are now frozen. Tests that need a shape no fixture has
  use a memory release (`data/src/sql/drivers/memoryRelease.ts`, built from the release contract's columns).
  release-boot and the conformance suite no longer need Python.
  - Deleted along with the reader: the bundle-vs-release parity block in conformance/release.test.ts, and
    conformance/speaker.test.ts (a Tier-2 run over the province bundle through the deleted reader). Its fixture
    version still runs in release.test.ts.
  - `pipeline/tools/fixture_release.py` and its test were orphaned (their input was the dev bundle), so they are
    deleted too. `test_bundle_clim.py` now reads sql/shared.ts.
- **2.15:** core/freshness.ts holds THE agoWords (plus Freshness, freshness(), readingAgeLine), and core/index.ts is
  a pure barrel. **Visible change:** ages under an hour now say "N min ago" everywhere (the menu and sync line said
  "just now" / "1h ago"), and the reading line now says "3h ago" / "3d ago".
- **2.16:** core/day.ts exports pad2, sameDay ({month, day}) and calendarDayOfYear. regs/names.ts `cap` replaces the
  four cap1 copies. statusIndex's `dayOfYear` (a READING day) is renamed **`readingDayOf`**, because the generated
  `readingDay(month, day)` already holds that name. The two day axes are not merged, and each one's doc says so.
- **2.17:** regs-census, emit-adipose, wide-measure and water-frame-measure use tools/regs-web/{cdp,drive}.mjs
  (drive.tap gains an opt-in scroll; tapAt is exported). They were syntax-checked but **not run** (they need Chrome
  and the app).
- **2.19:** the split is documented in the colouring.ts header. mapModes and the Shell point to it.

**Left / for other lanes:**
- Moving design/ live inputs (port/golden, states.json, fishart.js) into app/fixtures/ is not done (it touches the
  taps.py owner).
- `pipeline/deliver/release/build.py:318` docstring still names `pipeline.tools.fixture_release`.
- `pipeline/gauges/feed/publish.py` usage line still writes `--out app/packages/data/dev/feeds/gauge` (that fixture
  feed is kept for tests).
- `pipeline/stocking/README.md`, emit_gauge_policy.py and test_gauge_policy.py still mention the deleted
  bundle/queries.ts or build-fixture.mjs in prose.
- scripts/dev-build.sh: its build-fixture step was removed (the 2.11 lane may also edit this file).

`REGS_STORE_REQUIRED=1 pnpm -C app check` (via heavy.sh): `Test Files 362 passed | 4 skipped (366)`,
`Tests 8164 passed | 23 skipped (8187)`, rc=0.
