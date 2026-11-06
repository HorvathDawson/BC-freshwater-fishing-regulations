# T441 — One re-cut of every release-backed test fixture after a release

Status: DONE (deef924da; raw ids out of tests: 23c9aed6a).

Source: 2026-11-06, release r-4230bc0c1dd5. `pnpm check` needed four rounds before the 0.6.3 build, each finding the
next frozen set still cut from the previous release (handles 822db… → 269777…):
1. `app/fixtures/golden/taps.json` (`node app/tools/regs-recut/sync.mjs`, the documented step);
2. the committed regs fixture release `app/packages/data/dev/regs-release/` (`pnpm -C app regs-fixture`);
3. the regs VM fixtures `app/packages/ui-native/src/regs/fixtures/*.json` (`UPDATE_REGS_FIXTURES=1 … harness`);
4. `app/packages/data/dev/section-lines.json` (no writer: re-mapped by hand through `section_handles.txt`, matching
   `blk:start`) plus section ids and handles written into tests (`waterGroup.fixture`, `waterMap`, `WaterMapView`,
   `oneLoadPath`, `unnamed.release` H/H_ATLAS), mapped by water + part + position (all 1,480 mapped, no part changed size).
The fixes this time: taps.json (2026-11-06 "fixtures: taps.json re-cut"), fixture release + VM fixtures f94ffaf58, re-pins 55cff9e74.

## Work
1. `regs-recut/sync.mjs` runs all of it in order: taps → hydrate → regs-fixture → VM fixtures → section-lines (a real
   writer from the release's own `lines` file, not the hand-made spans).
2. Tests stop writing raw section ids: name the section by water + part + index (or stream key + start) and look the id
   up in the fixture, so a renumbering is not a test edit.
3. The release runbook says to run it after a promote, before an APK build.
Size: M.

## Done in part (2026-11-06, chat 4)
Work 1 and 3 done: `node app/tools/regs-recut/sync.mjs` runs taps → hydrate → regs-fixture → VM fixtures in one step,
and `regs_fixture` now WRITES `dev/section-lines.json` from the release's own `lines` file (`fixture_lines`): the
fixture's stream sections plus the one unnamed reach, named by `unnamed_at` = [blue_line_key, from_m] and looked up, so
a renumbering finds it again. Against the hand-made file (r-67b456b5e64b): every handle, line and start identical; ends
are now the release's (903 open ends instead of the hand's "2 m" guesses); 10 more rows. The in-memory `lines` writer
for tests (`releaseDir.ts` `sectionLineDb`) learned to fill a gap after an open line. Run end to end on LATEST: fixture
release re-cut (r-313224f5908f → r-5afbbf9bec7a), taps/hydrate/VM fixtures unchanged; data, ui, ui-native, conformance
suites 4290/4290. Runbook + AGENTS rule 46 say the one command.
Left (Work 2): section ids still written raw in tests (`waterGroup.fixture`, `waterMap`, `WaterMapView`, `oneLoadPath`,
`unnamed.release`) — re-mapped by hand on a renumbering.

## Done (2026-11-06, Work 2)
Tests no longer write raw section ids. `sectionNames(loader)` (`releaseDir.ts`, `@app/data/release/node`) names a stream
section by stream key + START metre (`stream` / `streams`, through the release's own `lines` file; throws naming the
place when no section starts there) and a lake's section by water + part (`lake`: the part's one non-stream section).
Converted: `waterGroup.fixture`, `waterMap`, `WaterMapView`, `oneLoadPath`, `WaterScreen.unnamed`, and
`unnamed.release` (live release; the `H_ATLAS` guard is gone — the three sections are looked up, not pinned). Same
sections, same order; mutation-checked (a swapped start fails the Chilliwack and Coquitlam tests). A re-cut that only
renumbers needs no test edit; one that moves a section's start or part fails by name.
