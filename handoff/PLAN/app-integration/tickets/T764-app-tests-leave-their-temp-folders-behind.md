# T764 — App tests leave their temp folders behind (184 GB in three days; the disk filled on 2026-11-15)

Status: IN PROGRESS (on the shared branch as e62331249 since 2026-11-15 23:54, `check:fast` clean; left (Claude): the whole app test suite with the run folder in place (the fast-fail before the Play build), clear the Maestro output after the benches).

Source: the main chat, 2026-11-15 23:50, after the Mac's disk filled at about 23:38 (1.3 GB free) and stopped a
night's work for an hour. Area: the app chat (app tests and tools).

## What happens
- The system temp folder (`$TMPDIR`) held 184 GB. 151 GB of it was about 31,800 `release-open-*` folders made by app
  test runs over three days (macOS only purges temp files after three days unused).
- Cause of those: `app/packages/data/src/sql/drivers/releaseDir.ts` `nodeOpener()` makes one temp folder per opened
  release (`mkdtempSync(…"release-open-")`), writes each file's bytes into it (the regs fixture is 15 MB, a live
  release far more) and never removes it. Every test file that opens a release makes at least one.
- The same pattern, smaller, at 23:50 after the main chat's clean-up: `fixture-release-*` 3,265 folders (456 MB),
  `gauges-*` 871 (1.1 GB, `app/workers/feeds/test/*.test.ts`), `regs-web-*` 13 (1.7 GB, the headless Chrome profile of
  `app/tools/regs-web/cdp.mjs`), and about thirty other `mkdtempSync(tmpdir(), …)` sites in `app/tools/*.test.ts` that
  never remove their folder.
- Not the app's: `province-store-*` (pipeline, a `TemporaryDirectory` that is only left behind when its run is killed)
  and `maestro_xctestrunner_xcodebuild_output*` (13 GB, 103 folders, written by Maestro's iOS runner).

## Fix
One mechanism for every app test, not thirty edits:
1. A vitest global setup (`app/tools/test-tmp.ts`, used by `app/vitest.config.ts` and
   `app/workers/feeds/vitest.config.ts`): each run gets its own temp folder `<system temp>/app-tests/run-<pid>`,
   `TMPDIR` points at it for the run's workers and every process they start, and the run's teardown removes it. At
   start it also removes the folders of runs whose process no longer exists (a killed run cannot clean up itself).
   A test pins that the workers really see the run's folder (a vitest upgrade that stops passing the variable on must
   fail, not leak again).
2. `nodeOpener()` removes its folders when its process ends, for the node tools that open a release outside vitest.
(`cdp.mjs` `close()` already removes its Chrome profile folder; its 13 left-over folders are from runs that were
killed, and under vitest they now lie in the run's folder.)

## Built (branch `app-fix/t764-test-temp`), 2026-11-15 23:52
- `app/tools/test-tmp.ts` + `test-tmp.test.ts` (3 tests), `globalSetup` in both vitest configs, `nodeOpener()` exit
  clean-up.
- Measured on this Mac: five test files that open the regs fixture (67 tests, both projects): loose `release-open-*`
  folders 2,618 before and 2,618 after, `app-tests/` empty after. Two feeds worker test files (28 tests): `gauges-*`
  871 before and after. A run killed with `kill -9` after 2.5 s left `app-tests/run-<pid>` (16 MB); the next run
  removed it.

## Done when
- A run of a test file that opens a release leaves no new folder in the system temp folder (counted before and after).
- A killed run's folder is gone after the next run starts.
- The app test suite passes with the run folder in place.
- The 13 GB of Maestro output is cleared once no run uses it (one-off, by the app chat).
