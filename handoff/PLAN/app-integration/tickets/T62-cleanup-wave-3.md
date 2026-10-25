# T62 — Cleanup wave 3: split long files, make it easy for a human to follow

Status: PARTIAL — app lane b619ca7b…bb31281e merged; pipeline lane (3.1, 3.3) merged; 3.9 merged (dfo_salmon); 3.2 rides the next side atlas. Source: handoff/PLAN/audit/PLAN.md §2 "Wave 3". Includes Shell.tsx (re-spec P14/P17 against today's
shell first, N13), export_ui_rules, other files over the size line. Behaviour unchanged: tests green before/after.

## Outcome (app lane)

Wave items 3.4, 3.5, 3.6, 3.7, 3.8 and 3.10 are done. Each split is its own commit on redesign/stream-sections (from
9253f72f) and none changes behaviour. Strict check: `REGS_STORE_REQUIRED=1 pnpm -C app check`, run through heavy.sh,
ends `Test Files 366 passed | 4 skipped (370) · Tests 8178 passed | 21 skipped (8199)` with rc=0. No snapshot was
written or updated.

| Item | File | Lines before → after |
|---|---|---|
| 3.4 | `ui-native/src/Shell.tsx` | 981 → 239. The spec of today's Shell (N13) heads the file. The parts are in `shell/`: useShellNav, useMapColouring, useShellSearch, MapLegend (+PairBanner), WaterModeHost, BrowseHost, SpotHost, ShellSheets, props |
| 3.5 | `map/src/Map.web.tsx` · `runtime-style.ts` · `adapters/contract.ts` | 1214 → 135 (16 hooks/helpers in `map/src/web/`) · 728 → 495 (+ endpoints.ts, gaugeDots.ts) · 703 → 348 (paint → paint.ts) |
| 3.6 | `ui/src/panel.ts` · `ui/src/hooks.ts` · `data/src/index.ts` | 1141 → panel/{feed,standings,horizon,donor,conditions,change} · 383 → hooks/{water,gauge,facts} · 637 → 39 (barrel) + types.ts + source.ts |
| 3.7 | `RegsSource` | Renamed `WaterSource` in one pass over app/ and the live integration docs, with no alias kept. Ticket files and archives keep the old name as history |
| 3.8 | SpotScreen · SearchSheet · core/search.ts · core/menu.ts | 581 → 276 (+spot/) · 423 → 180 (+search/) · 518 → 287 (+searchGeometry.ts) · 566 → menu/ (one file per page) |
| 3.10 | ConditionsLayout.before.ts + `P16_RECORD` | Deleted. The test now names every drawn line directly |

Gates that read a moved file by path now follow the code, and no copy was added. KNOWN_COPY entries moved with
SpotScreen's and SearchSheet's parts. back-layers no longer lists `Shell#Shell: takes onBack`, because the route
layer's `onBack` is now in a hook; the Shell now shows up through the day sheet it opens. The Layers and Day sheets are
still drawn by Shell itself, next to their open state, so the gate can see that each open state reaches a sheet that
registers with back.

Not done:
- `core/conditions.ts`: two gates pin that exact file, and moving HORIZONS out would make one of them pass vacuously.
- 3.10's second half, "pnpm check prints one line of skipped tiers": the brief did not include it.
- No dead code turned up in any of the split files.

## Outcome (pipeline lane)

Items 3.1 and 3.3 of the audit plan, on `redesign/stream-sections` (15 commits, 7b946345..3fbbd839). No behaviour change.

**3.1. The UI export is now the package `pipeline/deliver/export/`.** `pipeline/tools/export_ui_rules.py` (6,605 lines) was
split into contract, read, steelhead, guide_words, guide, cases, verdicts, closures, dictionary, model (`build`),
checks (`problems`), files (`encode`, `load`) and `__main__`. Run it with `python -m pipeline.deliver.export`.
`export_codec`, `guide_ladder` and `guide_examples` moved in as codec, ladder and examples. Every caller, test, doc and
command was updated. The test-water words (CASE_MECHANISMS and WHAT_TO_SHOW, 75 mechanisms) now live in `cases.json`.
`cases()` used to be one 914-line function; it is now 31 `_find_<mechanism>` finders, called in their old order, and
`cases()` itself is 75 lines. The finders are code that ships in `guide.cases`, so they could not become test fixtures.
The largest modules now are cases.py (1,233 lines), guide.py (1,204) and guide_words.py (1,079).

**3.3. Five modules became subpackages, each split along its own banners.** Each package's `__init__` re-exports every
name, so `read.X`, `rules.X` and the rest still work:

| module | lines before | now |
|---|---|---|
| `bundle/read` | 2,000 | records, source, facts, ladder (1,287), licensing |
| `bundle/rules` | 1,718 | predicates, row, lifts, see, regions, presence, diagnostics, tables |
| `answers/rows` | 2,523 | decisions, rule, evaluate, card_model, conds, part, wire, groups, assemble, presence, weekdays, section |
| `answers/display` | 1,807 | rule, part, decisions, builders, unnamed, subsets, strip, decided, section |
| `answers/model` | 1,927 | base, rows (1,142), gear, licence, display, top, frames, emit, `__main__` |

- **Tests that switch a value now set it on the module that reads it.** Examples: `read.ladder.WATER_CLOSURE_DOMINANT`,
  `rules.lifts.AGGREGATE_GROUPS`, `rows.groups.A_ROW_HELD_BY_ITS_TOTALS_GROUP`, `display.rule._plain`.
- **Source scans walk the packages recursively.**
- **The answers model's generated header now names `model/`.** `answers.generated.{json,ts}` were regenerated; the
  header line is the only change.
- **Rulings are cited by RULINGS id, not by date.** Comments and docstrings in read/, rules/, rows/ and display/ cited
  rulings by who said them and when. They now cite the RULINGS id. Rulings the code cited that RULINGS.md lacked are
  added to it: Z13, Z14, Q24-Q26, Li12, K9, O4 and P39-P46.

**Bug fixed: the card gate's `code_sha` ignored the new subpackages.** It now covers them.

**Dead code deleted:** `rows.quota_lines`, `display.STEELHEAD_LINES`, `display.STATUS_NAMES` and
`display.INHERITED_KINDS`.

**Identity proof: every output matches except a code stamp.** I ran `pipeline.deliver all --out-dir` twice, into side
directories through heavy.sh. The before run used a `git archive` of 9253f72. The after run used a `git archive` of
HEAD, placed at the same path.
- **Byte-identical:** ui-rules-export.json, ui-rules-guide.json, ui-rules-answers.json, status_index.bin,
  bundle.sqlite (all 64 tables) and verdicts.sqlite. In the release, 17 of its 18 files have the same sha256.
- **Different:** `card-check.json` `code_sha`, which is a digest of the answers code, and the release's `water` file,
  which carries that stamp in its meta. So the release id changed (r-5b998806c5b9 became r-7f3b22890b01). Any edit to
  the answers code moves this stamp, so the id cannot match after a refactor. Every answer stayed identical.
- **Generated files are current:** `python -m pipeline.tools.emit --check` and `python -m pipeline.deliver.answers.model --check`
  both pass.

**pytest: 7 failures remain (3 failed, 4 errors), none from this ticket.** The full fast suite ran through heavy.sh, unfiltered: 6 failed,
5,349 passed, 6 errors.
- I fixed 5 of those 12 in 3fbbd839 (the alias rewrite); both files then passed, 101 tests.
- **Still failing, and failing on the base too:**
  - 3 tests in `test_dfo_salmon` (another lane's area).
  - The 4 `test_synopsis_pages` errors, which depend on test order.

**Left for the owner:** five shipped strings still name the old paths:
- `about.what`
- the guide's `about` and its examples note
- the codec's encoding text
- the answers spec's schemas line

Each carries a comment saying so. Rewording them changes the release.

## Outcome (3.9, dfo_salmon)

- `whatchanged.py` had no callers: moved (history kept) to `archive/pipeline/regs/dfo_salmon/`; the one GOTCHAS.md mention now says so.
- Kept `feed`, `validate`, `splitwork`: tests import them, so they are not dead.
- `pipeline/regs/dfo_salmon/README.md` gained an index linking the folder's docs (CASCADE, CURATION, DATAFLOW, DESIGN, MAPPING, PLAN) and `pipeline/docs/19-dfo-salmon-pipeline.md`. No doc was rewritten; no links broke.
- 3.2 (atlas/build.py) needs a build: rides the next side atlas.
