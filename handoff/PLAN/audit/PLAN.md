# Cleanup and audit plan, 2026-10-22

Branch `redesign/stream-sections`, HEAD e6754333. This audit was read-only: no tracked file was changed and no heavy job was run.
Four finders contributed: app, pipeline, docs/tickets and icons. Their IDs are kept in brackets so every item traces back to its evidence.

**The goal is a codebase a person can follow:** no dead code, no old pathways, no huge files, and one way to do each thing.
**Binding:** `handoff/PLAN/app-integration/decisions.md`. Riffle is obsolete (2026-10-21), and icons are Lucide rounded 1.75 (2026-10-22).

> **May move.** Three lanes are editing parts of the app now. Items in their areas are tagged **[T48]** (navigation: Shell, TabBar, menu, Spots, MapRail, icons), **[T47]** (status colours: map status modes, Shell colouring, status-colours study) or **[REGS]** (regs polish: TabPending, RegsScreen, pipeline `deliver/answers/`). Do those items only after the lane merges, and re-check them first.

---

## 1. The pipeline as it runs today

```
SOURCES     data/fetch_data.py (1,923 lines)
              └─► data/source/{bc_fisheries_data.gpkg, stations, places, hydat, bc.pmtiles}
CURATED     data/curated/{regulations/entries, splits, areas, gauges/matches.json, regulations/reference/*.md}

            python -m pipeline build
              stages: [atlas → parity → promote] (only with --atlas) → reach → tiles → deliver

ATLAS       pipeline.atlas.build --full        main() is a single 878-line function
              reads  gpkg + curated splits/areas + gauges/matches.json (gauge cuts; no digest recorded ⚠ F28)
              writes data/generated/atlas/full/{graph.pkl 2.8 GB, geometries.pkl, registry.json, section_handles.txt}
REACH       pipeline.atlas.reach.cli
              writes reaches/full/{rule_section.jsonl 19 GB ⚠ F17, licensing_section, steelhead_presence,
                                   rule_diagnostic.jsonl (93 ambiguous cuts; nothing reads it ⚠ F14)}
TILES       pipeline.deliver.tiles             → data/generated/tiles/{atlas.pmtiles, meta, layers/}
DELIVER     python -m pipeline.deliver all     (one process per step, built in staging, promoted atomically)
  bundle       ◄ atlas + reach + curated entries + RAW gpkg ⚠ F06 + hydat + stations
                 + gauges/feeds/clim.json ⚠ F06 (made only by scripts/dev-build.sh)
                 + imports pipeline.gauges.generate.match ⚠ F05 (the matcher, which consumers should not reach)
  verdicts     ◄ bundle                        → verdicts.sqlite
  status_index ◄ bundle + verdicts             → status_index.bin
  export       ◄ bundle (pipeline/tools/export_ui_rules.py, 6,502 lines)
                                               → ui-rules-export.json + ui-rules-guide.json
  answers      ◄ bundle + export pair + CURATED reference/*.md ⚠ F29
                                               → ui-rules-answers.json (+ .gz nobody reads, F19)
  card_check   ◄ answers + bundle              → card-check.json
  store        ◄ answers + export pair         → regs.sqlite (P20 says it should no longer be written, D23)
  release      ◄ bundle + store + status_index + ATLAS graph.pkl ⚠ F07 + tiles meta + source dates
               [+ lake_facts: prepared, not fed]
                                               → data/release/r-<sha>/ (7 accumulated; publish.py picks newest by mtime ⚠ F16)
  vintage      checks tiles ↔ bundle ↔ every artifact

GAUGE FEED  (outside `pipeline build`) scripts/dev-build.sh → gauges.feed.climatology (clim.json)
            → gauges.feed.publish (every 30 min)
ORPHANS     pipeline/runtiming (unwired), pipeline/stocking + bathymetry (sources dropped),
            deliver/answers/reference (prototype harness, 5.8 MB), pipeline/hack, added_streams/muni_graph.py

APP         release (manifest → gzip files, tiles) → @app/data ReleaseLoader → loadRegsInputs → useRegulations
            Second, dev-only path: EXPO_PUBLIC_DATA_SOURCE=bundle → province.sqlite + status_index.bin (APP-01)
            Test fixtures: riffle.html → build-fixture.mjs → dev/bundle.sqlite → fixture release → conformance (APP-03)
```

Most of the flow's weirdness comes from four problems:
- The deliver key misses two of its inputs (F06).
- The release reaches back past the bundle into the atlas (F07).
- The gauge matcher and the atlas cuts can disagree, and nothing detects it (F05, F28).
- Silent picks and silent fallbacks hide problems (F14, F15).

---

## 2. The cleanup plan

Effort is S (≤ half a day), M (1–2 days) or L (3+ days). Risk is the chance of changing behaviour.

### Wave 1: safe deletions and dead code

These need no design work. Each one is a grep-verified delete, done in its own commit.

| # | Where | What | Proposal | Effort | Risk |
|---|---|---|---|---|---|
| 1.1 | `app/tools/render-setup.js`, `.d.ts` | Committed tsc output; vitest loads the `.ts` (APP-11) | `git rm` both | S | low |
| 1.2 | `app/packages/*/dist`, `conformance/dist`, `apps/mobile/dist` (ignored) | 34 orphan JS files from deleted sources, about 10 MB, polluting every grep (APP-10) | Make typecheck emit nothing (or `tsc -b --clean`), delete dist/, add `pnpm clean` | S | low |
| 1.3 | `ui/panel.ts:255`, `core/flow.ts:291,294`, `map/trace-geometry.ts:38`, `regs/testing/census.ts:190`, `ui-native/theme.ts:266-273`, `conformance/regs/exclusions.ts:68,76`, barrels | Unused exports: clearBasinCache, NO_GAUGE, NO_READING_HERE, ATLAS_MIN_Z, paletteFaults, ui-native mapChrome, STEELHEAD_STAMP_ITEM, YOUTH_ALT_LINES, ALL_REGIONS, DEFAULT_VIEW (APP-08) | Delete | S | low |
| 1.4 | `ui-native/src/menu/BrowseBar.tsx` **[T48]** | Never imported (APP-07) | Delete, unless T48 adopts it for the Spots tab | S | low |
| 1.5 | `regs/RegsScreen.tsx:90-97`, `WaterScreen.tsx:141-147`, `core/regs/model.ts:32-34` **[REGS]** | TabPending can no longer be reached (APP-09) | Drop TabPending and WORDS.TAB_PENDING; comments become "null outside B.C." | S | low |
| 1.6 | `app/apps/web/*`, `ui/index.ts:42-47` formFactorFor, `layers.json:86` | Empty speculative app; formFactorFor has no callers. T45 already made desktop a layout class (APP-12, D10) | Delete; amend AGENTS rule 26 and app/README to "desktop = layout class in ui-native" | S | low |
| 1.7 | `ui/hooks.ts:252-271` + RegsSource, ×3 implementations | useWaterGauge plus 5 methods no screen calls: gaugeForItem, itemExists, stationsFor, lakeInfo, stockingHistory (APP-06) | Delete with their queries and suite cases. The 2026-10-21 "keep the plumbing" ruling covers lake VALUES, not these methods | M | low |
| 1.8 | `pipeline/hack/` | Stale one-off scripts; the README names modules that don't exist; paths point to archive (F10) | Delete or move to archive/ | S | low |
| 1.9 | `added_streams/muni_graph.py` | Unimported prototype whose tolerance disagrees with live code (250 vs 400 m) (F11) | Delete | S | low |
| 1.10 | `deliver/answers/cli.py:34-35`, `deliver/__main__.py:81` | `ui-rules-answers.json.gz` is written and promoted, never read (F19) | Stop writing it; remove from canonical_homes | S | low |
| 1.11 | `pipeline/tools/part_label_diff` | No references (F24) | Delete | S | low |
| 1.12 | `pipeline/stocking/`, `pipeline/bathymetry/`, `bundle/schema.sql:961-976`, `bundle/build.py:928-931`, `check_curated.py:127-145` | Sources dropped; 4 schema tables never filled; check_curated names a regenerate command that doesn't exist (F09) | Delete them; lake_facts stays as the one future landing point | M | low |
| 1.13 | `.claude/worktrees/agent-a8261f149d43eead6`, `wf_e2463c2d-be5-1` | Stale worktrees. **The second holds the only copy of a lost owner question (485677bc)** (D17) | Cherry-pick the 485677bc decisions text first (ticket N2), then remove both | S | low |
| 1.14 | `data/generated/scratch_*`, `r9live`, `r10live`, root `*.log` (about 78 GB) | Stale side builds (F18) | Owner approves a dry-run list, then deletes by hand. Memory rule: never pipe `rtk ls` into rm | S | low |
| 1.15 | `data/release/r-*` (7 dirs) | Accumulated releases (F16, D30) | Prune to live + previous after N10 (LATEST file) exists | S | med |

**Owner calls inside wave 1:** `pipeline/runtiming` (F08) and `deliver/answers/reference/` (F12) are listed in §4. Delete them only on a yes.

### Wave 2: merge old pathways and duplicates

| # | Where | What | Proposal | Effort | Risk |
|---|---|---|---|---|---|
| 2.1 | `apps/mobile/App.tsx:62-127`, `config.ts`, `data/src/bundle/*`, `ui/release.ts:42-50`, `ui/hooks.ts:480-545`, `RegulationsPlaceholder.tsx`, serve-tiles `/province.sqlite` | **The dev-bundle data path is a full second pathway** beside the release (APP-01, APP-05) | Delete DATA_SOURCE, useDevBundle, ENDPOINTS.bundle, devStatus, statusIndexUrl/loadStatusIndex, makeBundleSource, engine.* drivers and RegulationsPlaceholder. Keep core decodeStatusIndex | L | med |
| 2.2 | `data/src/release/source.ts:25-29`, `bundle/shared.ts` | The live release reader imports from the dev `bundle/` folder (APP-02) | With 2.1: move db.ts, shared.ts and the SQL into `data/src/sql/`; rename BundleSourceOptions to SourceOptions | M | low |
| 2.3 | `app/tools/build-fixture.mjs`, `dev/bundle.sqlite`, `conformance/fixtureRelease.ts`, `pnpm check` `fixture` step, bundle-schema / source / feed-contract tests | **Obsolete riffle.html still feeds the tests.** There are three fixture worlds (APP-03, D03) | Tests read the fixture release (or are deleted with 2.1). Drop build-fixture.mjs, the fixture step, dev/bundle.sqlite and the `/bundle.sqlite` route. Then delete riffle.html (2.5 MB) | M | med |
| 2.4 | regs.sqlite: deliver `store`, `design/regs-v11/port/{taps.py,verify_map.py,sync.mjs}`, conformance golden, `reads.release.test.ts:187`, test_release | P20 is only half done: regs tables exist twice (D23, APP-25) | Everything reads release files; deliver writes no regs.sqlite without `--debug-json` | M | med |
| 2.5 | `release/build.py:213-229`, `release/continuation.py` | The release unpickles atlas graph.pkl for water_continue, located through an absolute path in bundle meta (F07, F27) | Compute water_continue in the bundle build (graph already loaded); release copies it. meta.build becomes atlas-relative; fix `version` (always "full+reaches") | M | med |
| 2.6 | `bundle/build.py:540`, `gauges/generate/match.py:59` | The consumer imports the matcher; the matcher imports deliver.tiles (F05) | Move nodes_for/summarise to `gauges/consume` or `matches.py`; add an import-graph test that nothing outside generate/ imports it | S | low |
| 2.7 | `bundle/build.py:513` _gauges (281 lines) + L695-778, `atlas/gauges/*`, `gauges/{generate,consume,feed}` | Gauge logic has four homes (F04) | `atlas/gauges/*` moves to `gauges/consume/{panels,basins}.py`; the bundle's gauge tables move to `gauges/consume/bundle_tables.py:write()` | M | low |
| 2.8 | `tiles/names.py:16`, `regs/matching/matcher.py:68`, `registry/flowing.py:77`, `reach/steelhead.py:251`, dfo_salmon/*, `registry/build.py:26`, `splits/area_catalog.py:25` | 8+ name normalisers that disagree ("St. Mary's River" gives 4 different keys) (F20) | `pipeline/common/names.py` `key()`, pinned by a test. **Diff rule and gauge bindings before promoting** (keys change) | M | med |
| 2.9 | `status_index.py:271`, `lake_facts.py:121`, `release/paint.py:37` | 3 copies of the varint writer (F21) | `pipeline/deliver/binary.py` plus one round-trip test | S | low |
| 2.10 | `bundle/topology.py:36` and `tools/water_map_export.py:66` TOUCH_M, plus about 40 metre constants | Tolerances that must agree are declared separately (F22) | `pipeline/common/tolerances.py`; stamp the delivery-facing ones into bundle meta | S | low |
| 2.11 | `pipeline/__main__.py`, `pipeline/deliver/__main__.py`, `scripts/dev-build.sh` | Several ways to run the pipeline; bare `python -m pipeline` still runs the atlas (F23) | `python -m pipeline build` is the only entry; climatology becomes a build stage (see N9) | S | low |
| 2.12 | `pipeline/tools/` (17 modules) | Mixes export, generators, release ops and audits (F24) | `tools/emit/` (one `--check` for all 4 generators), publish and fixtures move to `deliver/release/`, the rest to `tools/audit/` | M | low |
| 2.13 | `registry/build.py:388`, `build_dataset.py:1408`, `splits/border.py:503`, `bundle/build.py:146`, `check_curated.py:112` | Silent `except Exception` fallbacks (F15) | Raise with the fetch command; when something is truly optional, record its absence in meta | S | low |
| 2.14 | `answers/glossary.py:53-73` | Answers read curated book text at run time, which breaks "the bundle is the only source" (F29) | The bundle copies the definitions into a `glossary_source` table | S | low |
| 2.15 | `core/index.ts:56-75` vs `core/menu.ts:200-211` | Two `agoWords` with different wording; the barrel holds logic (APP-17) | `core/freshness.ts` uses one agoWords; index.ts becomes a pure barrel | S | low |
| 2.16 | sameDay ×4, cap1 ×4, dayOfYear ×3, pad2 ×3 (APP-18) | Helpers re-declared per file | Export from `core/day.ts` and `core/regs/names.ts` | S | low |
| 2.17 | `tools/regs-census.mjs:17,75-90`, `tools/emit-adipose.mjs:53` | Tools import a CDP copy from design/; sleep/until/tap re-implemented (APP-19) | Point them at `tools/regs-web/{cdp,drive}.mjs`; move live inputs (port/golden, states.json, fishart.js) into `app/fixtures/` | M | low |
| 2.18 | `icons.tsx`, `MenuButton.tsx:22`, `RegsHeader.tsx:70-72`, glyph text **[T48]** | Icons drawn in three places plus unicode glyphs (APP-20) | Folded into ticket N1 (Lucide) | M | low |
| 2.19 | `ui/mapModes.ts:116`, `native-style.ts:105`, `Shell.tsx:289` **[T47]** | Two status colour modes split by platform (APP-22) | Document the split in one place (`colouring.ts` header) until N4 removes it | S | low |
| 2.20 | `answers/caps.py` | Imports private `card_check._bands` and `Engine` from the checker (F13, partly refuted) | Move Engine to a neutral module. Minor | S | low |
| 2.21 | `added_streams/{verify_map,mapcheck}.py` | Two map checkers for one job (F11) | Merge into one | S | low |

### Wave 3: split long files, improve readability

Do these after the lanes merge. Each split is a no-behaviour-change commit, proven by the existing tests (pipeline splits also by `build_parity` / a deliver diff).

| # | Where | Proposal | Effort | Risk |
|---|---|---|---|---|
| 3.1 | `pipeline/tools/export_ui_rules.py` (6,502 lines; `cases()` 914) (F01) | New `pipeline/deliver/export/` package: contract, read, guide_words, guide (+ ladder, examples), dictionary, checks, codec. The 1,480 lines of test-water cases move to data or test fixtures | L | med |
| 3.2 | `pipeline/atlas/build.py` main() 878 lines (F02) | A 30-line orchestrator over named phases (load_fwa, build_graph, add_waters, gauge_cuts, sectionize, build_registry, write_sidecars), each with an inputs/outputs docstring; proven by `--gnis` build_parity | L | med |
| 3.3 | `deliver/answers/{rows 2467, model 1880, display 1735}`, `bundle/{read 2011, rules 1588}` **[REGS]** (F03) | Split along existing banners into subpackages; move round/owner-date history out of docstrings into RULINGS.md | L | med |
| 3.4 | `ui-native/Shell.tsx` (727 lines, 41 hooks) **[T48][T47]** (APP-13) | useShellNav, useMapColouring, `<MapLegend>`, `<WaterModeHost>`, `<ShellSheets>`; Shell ≈150 lines. **Re-spec with P14/P17 first (ticket N13)** | L | med |
| 3.5 | `map/Map.web.tsx` (1,039 lines, 22 useEffects), `runtime-style.ts` 650, `adapters/contract.ts` 515 (APP-14) | Per-concern hooks in `map/src/web/`; runtime-style becomes endpoints, gaugeDots and style; paint constants move to paint.ts | L | med |
| 3.6 | `ui/panel.ts` 719, `ui/hooks.ts` 548, `data/index.ts` 586 (APP-15) | `panel/{feed,standings,donor,horizon,conditions}`; hooks become water, gauge and facts; data/index becomes types.ts + source.ts | M | low |
| 3.7 | `RegsSource` (APP-21) | Rename to `WaterSource`, since it reads no regulations. One mechanical pass after all lanes merge | M | low |
| 3.8 | SpotScreen 522, SearchScreen 435 **[T48]**, core/search.ts 453, conditions.ts 438, menu.ts 436 (APP-16) | Move sub-parts into folders; split search geometry from ranking; one file per menu page VM. Leave trust.ts, words.ts, generated and archive files as they are | M | low |
| 3.9 | `regs/dfo_salmon/` (15 modules, 7 overlapping docs) (F30) | One README; archive the CLIs that have no callers (whatchanged, feed, validate, splitwork) | M | low |
| 3.10 | Test debt (APP-24, APP-25) | Replace the P16 "before" snapshot and its record mode with direct assertions. `pnpm check` runs `REGS_STORE_REQUIRED=1` and prints one line of skipped tiers | S | med |
| 3.11 | Guard against regrowth (APP-26) | Add `knip` (unused exports, orphan files) to `pnpm check`, with an allowlist | S | low |

### Wave 4: docs

| # | Where | Proposal | Effort | Risk |
|---|---|---|---|---|
| 4.1 | Riffle references: `app/design/README.md`, `app/HANDOFF-ui.md`, about 18 app file headers (Shell, SearchScreen, Chrome, TabBar, MapRail, search.ts, hooks.ts, check-colours.mjs, tokens.json…), `AGENTS.md:171` (APP-04, D09, D16, F25, icons) **[T48]** | One sweep in the same change as 2.3, so no riffle reference survives. Each number gets its own reason ("40 px thumb target") | S | low |
| 4.2 | "Regulations are not integrated" headers (about 10) and done-ticket promises in comments (WaterViews P14/P15, SheetHost RegsView, fixtures.ts T36, README PlaceholderScreen) (APP-05, APP-23) | One comment sweep. Replace `appSheetKey` with core `appKeyOf` | S | low |
| 4.3 | `pipeline/gauge_match.json` named in 5 comments + DATA-MAP; `pipeline/hydro`; `06-ui-data-contract.md` (F25, D15) | Real path `data/curated/gauges/matches.json`; archive or rewrite 06 | S | low |
| 4.4 | `decisions.md` contradicts itself: D2/D24, D7 MapDay, D15 ship gate, answered "Owner questions", 10 m base, D13 (D04) | Every row shows its final state; answered questions move to an Answered block; a newest-wins rule at the top | S | low |
| 4.5 | `IMPLEMENT-HANDOFF.md`'s second "binding" list (D05) | Freeze it as history with a banner: binding = decisions.md only | S | low |
| 4.6 | Tickets T24-T27, T01/T10 rows, T47/T48/T49 texts **[T47][T48]** (D06, D07) | Mark T24-T27 superseded by T21 (3665f151) with a name map; add a "Final (owner 2026-10-22)" block in each lane's merge commit | S | low |
| 4.7 | `app/HANDOFF-ui.md` nav, rail and back sections, Riffle parity section, desktop scaffolding **[T48]** (D08) | Rewrite from the code after T48 | S | low |
| 4.8 | `AGENTS.md` rule 31 (check list), missing owner and process rules (D11) | "Owner rules (binding)" and "Process" sections; rule 31 points at package.json | S | low |
| 4.9 | `CLAUDE.md` (163 lines), `.claude/CLAUDE.md` (D12) | About 10 lines: read AGENTS.md, the parser ban, graphify-first. RTK stays global | S | low |
| 4.10 | `implement_progress.md` (676-line log) (D13) | About 80 lines: CURRENT STATE / OPEN / process / link to a dated history file | M | low |
| 4.11 | `pipeline/docs/{CURRENT-STATE,NEXT}.md` (Sept/early Oct) (D14) | Archive with dated names; 00-README points to RULINGS, 15, 18 and GOTCHAS | M | low |
| 4.12 | `data/generated/README.md` (F18) | Generate the table from the GENERATED model so it can't drift | S | low |
| 4.13 | `handoff/PLAN/app-data/*/scripts` (about 40 study scripts importing internals) (F31) **[T47]** for status-colours | Archive each study's scripts once its decision is recorded | S | low |
| 4.14 | `handoff/PLAN` (164 MB) and `app/design` (85 MB): r20*/ur5 reports, merged-ticket shots, regs-v3/v10, regs-diagram, v36snap.js; about 100 dead doc links (D30, APP-19) | Archive in dated commits with the owner's go; add a CI check that fails on a missing repo path in .md outside archive | M | low |

---

## 3. New tickets

| ID | Title | Why | Acceptance |
|---|---|---|---|
| N1 | **Icons = Lucide** (D22, APP-20) **[T48]** | One icon family, as decided 2026-10-22; today icons are drawn three ways plus glyphs | Lucide paths vendored inline (no dependency) or pinned with a deps.md line; ISC credit added to Data sources & credits. One `<Icon name/>` covers map, conditions, spots, more, layers, search, sync, back and calendar, at a 24 grid and 1.75 rounded stroke. LayersIcon and MenuIcon are deleted, and so is the RegsHeader inline Svg. A house check rejects hand-drawn chrome glyphs. Light and dark shots |
| N2 | **Settle D2/D24 vs the view strip** (D02) | The owner question was lost on an unmerged branch; Map is offered twice (strip + RegsHeader) | 485677bc text cherry-picked into decisions.md; owner picks (a) strip with header Map removed or (b) D2/D24 as written; D2/D24, IMPLEMENT-HANDOFF and APP-PREP §0 updated; stale worktree removed |
| N3 | **Ticket ledger with status** (D01) | Open vs done can't be told without reading a 676-line log | README table gains Status + Commit columns, including T44-T49; each ticket file gets a Status line; a script fails when a T-number in git log is missing from the table |
| N4 | **Native colouring engine** (D18, APP-22) **[T47]** | "One colouring pathway, 0 grey frames" holds only on web | `coloursEachTile` true on native; emulator, then phone, pan shows 0 grey frames, with numbers recorded; the push "status" mode is deleted |
| N5 | **Finish P20: no regs.sqlite** (D23) | Two copies of the regs tables | Wave item 2.4 done; `pnpm check` (strict) and pytest green |
| N6 | **Retire the dev bundle and riffle fixture** (APP-01/02/03, D03) | Second data pathway and a test chain fed by an obsolete design | Wave items 2.1–2.3 done; riffle.html deleted; `pnpm check` green |
| N7 | **Deliver key covers every input** (F06) | A gpkg refresh or new clim.json leaves the bundle wrongly "up to date" | gpkg stamp and clim.json sha in the deliver key; a test changes each and sees a rebuild |
| N8 | **Atlas records its gauge matches** (F28) | Atlas cuts and bundle placement can come from different matches.json versions | sha(matches.json) written into the atlas build dir; the bundle refuses a mismatch with a clear message |
| N9 | **Climatology is a build stage** (F06, F23) | clim.json is made only by a shell script outside `pipeline build` | `pipeline build` orders HYDAT → envelope → bundle, keyed on the hydat stamp; dev-build.sh keeps only the feed cron |
| N10 | **Release LATEST pointer + prune** (F16) | publish.py ships the newest-by-mtime of 7 dirs | deliver writes `data/release/LATEST`; publish reads it or requires `--release`; a prune command keeps published + latest |
| N11 | **Ambiguous cuts gate** (F14) | 93 closure starts picked silently (66 a month ago); nobody reads the diagnostic | Diagnostics go into a bundle table or a build gate with an allow-list in data/curated; a new ambiguous cut fails the build |
| N12 | **Determinism proof** (F26) | Docstrings say non-deterministic; the 2026-10-06 fixes may have cured it; nothing proves either | A needs_source test builds a `--gnis` area under PYTHONHASHSEED=1 and =2 and compares registry.json + section_handles.txt; the claims are updated. The first run is human-run (heavy) |
| N13 | **Re-spec P14/P17 against today's shell** (D27) **[T48]** | The P14 spec predates one-water-screen and would reintroduce superseded pathways | Short spec with paths that exist (WaterFrame, openWater) and a Shell.tsx line target; it absorbs wave item 3.4 |
| N14 | **Smaller reach hand-off** (F17) | 19 GB JSONL per run, 95 GB with copies | Reach writes SQLite or interned gzip JSONL; bundle reads it; bindings diff identical |
| N15 | **OPEN-QUESTIONS.md, asked once** (D25) | Questions are scattered across about 8 places; one was already lost | One file with id, default in place, where it bites and the "if not" cost; answers move to decisions.md |
| N16 | **Consistency auditors** (D28) | Owner asked 2026-10-22; Map is already duplicated | Route-equality, one-surface-per-action and data-vs-UI checks, pinned with mutation tests |
| N17 | **D1 phone testing** (D19) | Many device-only items unverified | D1a: dev build + OWNER-PHONE.md checklist with results. D1b: preview profile on R2 staging; P11 install/update/rollback pass |
| N18 | **P18b basemap in the release** (D24) | `manifest.basemap` is null; native can't get the ground map | Manifest names the basemap (id, bytes, sha256, url); native install/update fetch it; tile tests cover it |
| N19 | **Own gauge across a continuation** (D20) | Vedder has no leading gauge although water_continue links it to the Chilliwack | After the owner answers: build_panels treats a predecessor's gauge as own; Vedder 313838 test; the 5 constants are recorded in decisions.md |
| N20 | **Tidal / backwater disclosure on Level** (D21) | Owner asked; Level is led by tidal gauges | Pipeline flags stations; the release carries the flag; one core sentence; 08MH035, Whonock, Mission and Port Mann pinned |
| N21 | **Imagery corridor P0 + pools validation** (D26) | "Coming soon" layers have no build ticket | SPEC drops the 10 m tier, go/no-go recorded; pools precision/recall on 2–3 known rivers |
| N22 | **Small open items** (D29) | S1 photo minors, lost Kitimat witness, whole-lake search, D19 klmb_1010 | Each is a ledger line with acceptance (fixed + test, or an owner "no") |
| N23 | **Unused-code guard** (APP-26) | Dead code regrew unnoticed | knip in `pnpm check` (wave item 3.11) |

---

## 4. Data weirdness and choices that need an owner call

1. **pipeline/runtiming (F08):** about 1,400 lines plus 413 MB of output, fully unwired. It writes tables no schema has. Delete it, or ticket it as a real bundle step?
2. **Prototype answers reference (F12):** 5.8 MB of HTML/JS and 1,417 Python lines inside `deliver/answers/reference/`, kept alive by 4 tests. Archive it and turn the tests that still matter into golden JSON fixtures?
3. **D2/D24 vs the view strip (N2):** keep the strip and remove the header's Map button, or go back to D2/D24 as written?
4. **Name normaliser unification (F20):** one key will change some rule and gauge bindings. Do you accept a reviewed binding diff?
5. **Ambiguous cuts (F14/N11):** 93 closures start at a coin-flip measure. Review them once into an allow-list (curation time), or carry them as visible "uncertain start" data?
6. **The release reads the atlas graph (F07):** move water_continue into the bundle (bundle becomes the only upstream). Recommended; is that OK?
7. **Determinism (F26/N12):** the first proof run is a heavy job and must be human-run. When?
8. **Disk cleanup (F18, wave items 1.14/1.15):** about 78 GB of scratch builds plus old releases. Approve a dry-run delete list.
9. **handoff/PLAN and app/design archive (D30):** move or delete the prototype rounds, merged-ticket shots and regs-v3/v10? Git keeps history either way.
10. **Own-gauge constants and Vedder (D20/N19):** five defaults await sign-off (lead 0.7, ratio 10, far 0.2, far min 10 km, area trust 10).
11. **Pending owner questions (D25):** about 30 questions across T44-T46, T38/T42/T43, map chrome, T49 and the imagery spec. They will be gathered into N15 for one sitting.
12. **Memory notes that contradict the repo (D31):** "Riffle is the design target" and "gauge-match-one-frozen-file" (wrong path). Agents can't edit memory on another agent's say. Please update or remove them.
13. **`heavy.sh` lives outside the repo (F23):** AGENTS.md points to `~/.claude/.../handoff/tools/heavy.sh`. Should it move into the repo?

---

## 5. Bottom-bar icons

**Comparison page:** `/private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/368da779-39ee-429c-b130-eec0d72dc7dd/scratchpad/icons/compare.html`

It shows today's set and directions A, B and C. Each is drawn on a 390-px phone in light and dark with the bar Map · Conditions · Spots · More and the rail's Layers button, plus a 24-px glyph grid that includes search, sync and back. It uses real tab type and theme colours.

**Problems with today's set**
- Spots and Search have the same silhouette.
- Icons come in three sizes (18/20/22) and two drawing methods (MenuIcon is Views, not SVG).
- There is no More, sync or back glyph.

**The three directions**
- **A, "Cut square, finished":** the house style (butt caps, mitre joins, 1.8/2.2). Confluence for Map, hydrograph with a latest-reading square for Conditions, pennant for Spots, square dots for More, offset plates for Layers. No dependency.
- **B, "solid when selected":** A's drawings, with the selected tab as an ink-filled tile and the mark knocked out. It gives a second cue besides colour, which TabBar's own comment says colour-blind readers and sunlight need.
- **C, Lucide v0.460.0 (ISC), round 1.75:** map, activity, map-pin, ellipsis, layers, search, refresh-cw, chevron-left. Alternates: Lucide `waves`, Tabler `fish-hook`.

**Recommendation: C (Lucide), because it is binding.** The owner chose it on 2026-10-22 (decisions.md:175, commit e6754333). The icons finder favoured A and noted that icons.tsx had earlier rejected round caps. That preference was recorded before the ruling and does not override it.

Within C:
- **Conditions:** prefer Lucide `waves` (or a river-flow glyph) over `activity`, which reads as a fitness heartbeat at 22 px.
- **Paths:** vendor them inline. This needs no dependency and keeps deps.md clean, which matches "minimal prod packages".
- **Credits:** add the ISC notice to Credits.
- **Sizes:** one `<Icon>` at 22 px in the bar and 20 px on the rail.
- **Selected tab:** adopt B's idea of a second cue as a Lucide-compatible variant (a filled pill behind the selected tab), not a redrawn glyph, if the owner wants one. Owner call.

**Lane impact.** This is ticket N1 and lands after T48 merges, because TabBar, MapRail, MenuButton and RegsHeader are all in the T48 lane. TabBar's header comment (three tabs, Spots on the rail) and MapRail's riffle citations are already false and are fixed in the same change.

---

## What the three running lanes will move

- **T48 (navigation):** 1.4 BrowseBar, 2.18/N1 icons, 3.4 Shell split, 3.8 SearchScreen/SpotScreen, 4.1 TabBar/MapRail/SearchScreen riffle comments, 4.6 T48 ticket text, 4.7 HANDOFF-ui nav, N2 (headers), N13.
- **T47 (status colours):** 2.19 colour-mode split, 3.4 Shell colouring hook, 4.6 T47 ticket text, 4.13 status-colours scripts, N4.
- **REGS (regs polish):** 1.5 TabPending, 3.3 `deliver/answers/` splits, and any RegsScreen or RegsHeader comment work.

Everything else (all of the pipeline except answers/, the data package, tools, docs and disk) can start now.

**Suggested order**
1. Wave 1 (excluding lane items) and the doc fixes 4.3, 4.4, 4.8 and 4.9.
2. N2, N3 and N15, which are cheap and stop further loss.
3. N6 and N5, which unblock riffle deletion.
4. The pipeline wave 2 items (2.5–2.14) with N7, N8 and N10.
5. After the lanes merge: N1, N13 with 3.4, then the remaining wave 3 and the wave 4 sweep.
