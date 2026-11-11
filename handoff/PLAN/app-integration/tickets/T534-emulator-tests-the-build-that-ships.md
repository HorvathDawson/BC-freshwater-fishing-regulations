# T534 — The emulator tests a build it can name: commit stamp, stale-APK refusal, patched MapLibre by default

Status: IN PROGRESS — tools lane; 4b6f79411 (built + unit-proved 2026-11-10); left (Claude): one real `session.sh --build` + `run.mjs install` to see the stamp and build id 7bf3dc29 (command in "Built" below).

Source: workflow review 2026-11-09 (verified against the code). Rank 2.

## Today
- `app/tools/testbed/session.sh:68` rebuilds only with `--build` or when the APK file is missing; nothing compares the
  kept APK with HEAD, and the build (`:99-100`) sets no `EXPO_PUBLIC_GIT_SHA`, so a run cannot say which commit it tested.
- It builds in the live checkout (`:70-77`, every lane's uncommitted edits included); `deploy/staging/build-apk.sh:155-162`
  builds a clean worktree of HEAD.
- `session.sh:45-48` takes the patched MapLibre only when two env vars are set, so the default test-bed APK is stock while
  every shipped non-production APK is patched (`build-apk.sh:74-76`, owner rule 11-09).
- So "emulator green" can describe an older commit, a dirty tree and a different map library than the build on Play.
- The test-bed APK differs from the shipped one on purpose (test update channel, pan probe, reports: `session.sh:89,99`),
  so installing the Play artifact itself is not the fix.

## Work
1. Stamp the test-bed APK with its commit and whether the tree was dirty (in the app + a sidecar file beside the APK).
2. `session.sh` refuses, in its pre-lock check, a kept APK whose commit is not HEAD or whose tree was dirty, unless a
   reason is given.
3. Default the test bed to the same patched MapLibre as `build-apk.sh` (same `--stock-maplibre` escape).
4. `run.mjs` prints commit + libmaplibre build id at the top of every report; the STATUS line for a build quotes them.

## Acceptance
- A stale APK is refused with the commit named; the reason escape works and is printed.
- Report header shows commit and build id 7bf3dc29 (or the current patched id) on a default run.

Depends: —. Size: M. Owner: no.

## Built (2026-11-10, 4b6f79411)
- `app/tools/testbed/lib/build-stamp.mjs`: `out/<apk>.stamp.json` beside every test-bed APK (commit, dirty paths under app/, libmaplibre build id read from the APK, patched or stock + reason). The build also sets `EXPO_PUBLIC_GIT_SHA=HEAD` (the app's own version string carries it, as build-apk.sh).
- `session.sh`: a kept APK whose stamp commit is not HEAD, whose tree was dirty, or that has no stamp is refused in the pre-lock part (before `HEAVY_PRECHECK` exits), naming the commit; `--allow-stale "<reason>"` lifts it and prints the reason. Default build = patched MapLibre (MLN_LOCAL_MAVEN defaults to cache/maplibre/fslean-m2, one AAR checked; the built APK's libmaplibre.so id must equal the AAR's or the APK is removed); `--stock-maplibre="<reason>"` = stock under a `-stock` name; `--opengl` needs the stock escape (patched is Vulkan only).
- `run.mjs`/`report.mjs`: console, report.md and index.html open with "Build: commit … · libmaplibre build id … (patched, T420 lean)".
- Proof: `app/tools/testbed-stamp.test.ts` (17 tests: refusal names the commit, the reason escape prints, header line, script order and defaults); `HEAVY_PRECHECK=1 bash session.sh` by hand: the pre-T534 APK is refused with "no stamp", `--allow-stale` lifts it and prints the reason, `--opengl` is refused without the stock escape.
- Not verified (needs the emulator build, heavy): a real `--build` writing the stamp and reading the id from the APK (7bf3dc29 expected). Command: `scripts/heavy.sh testbed 30 -- bash app/tools/testbed/session.sh --build -- node app/tools/testbed/run.mjs install`, then check the report header and `out/app-testbed.apk.stamp.json`.
- Not done: the build still reads the live checkout (the stamp says dirty); a clean-worktree build like build-apk.sh is not made. Status is IN PROGRESS, not IN REVIEW, because the only step left is Claude's (T539 rule).
