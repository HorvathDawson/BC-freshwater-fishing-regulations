# T441 — One re-cut of every release-backed test fixture after a release

Status: OPEN.

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
