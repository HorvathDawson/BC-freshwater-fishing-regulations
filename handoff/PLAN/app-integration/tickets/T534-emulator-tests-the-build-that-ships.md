# T534 — The emulator tests a build it can name: commit stamp, stale-APK refusal, patched MapLibre by default

Status: OPEN.

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
