# T538 — build-apk.sh: cheap checks before the lock, changelog required, MapLibre proof on both paths

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the code). Rank 6.

## Today (`deploy/staging/build-apk.sh`)
- **Changelog only warns.** `:169-171` prints a WARNING when the version has no entry in
  `app/packages/core/src/changelog.ts`, then publishes `notes: []` (`:172,242-244,281`). The header (`:26-32`) says the
  entry comes first. No check anywhere reads it. `expo.version` is bumped by hand; nothing compares it with the last
  published one.
- **Play notes and the tester email are commit subjects** (`:237-238,247-248`); the header says "never commit subjects"
  (`:26-27`) and the changelog bullets are already read.
- **Checks after the lock wait or the build.** No `heavy-precheck` block, so keystores and certificate (`:134`), clean
  app.json (`:135`), Play API reachable (`:140`), debug keystore (`:203`) run after the wait; an expired Cloudflare login
  is found after the Play upload and leaves `play.json` stale with no way to redo only that step.
- **Patched-MapLibre proof is `--play` only** (`:218-229`). The staging-page APK (`:257-262,285-288`) ships unchecked.
  T512 covers the Play refusal, not this path. The expected build id lives only in STATUS.
- **Uncommitted app edits are silently left out.** fast-fail checks the working tree (`fast-fail.sh:42`); the build is a
  clean worktree of HEAD (`:155-162`).
- **versionCode commit precedes the build** (`:142-153`), no undo on failure. Harmless to Play; leaves bump commits
  (ab9e25a5c and 6ef99a4e5 are two minutes apart).

## Work
1. Sub-second check in fast-fail and `pnpm check`: `expo.version` has a changelog entry with ≥ 1 bullet and it is the
   newest. build-apk refuses without one (`--no-notes=<reason>`); `--play` refuses a version equal to the published one
   unless a reason is given.
2. Play notes + email from the changelog bullets; fix the header's two statements.
3. A `heavy-precheck` block before the lock: keystores, certificate, service account + one Play call, wrangler login,
   run token, readelf found, exactly one patched AAR, app.json clean, uncommitted `app/` paths listed (refuse or reason).
4. One build-id function for both paths; expected id recorded beside the patch; id printed on the staging page + email.
5. `--publish-only` redoes play.json + email for an uploaded build.
6. Bump versionCode in the worktree; commit after the artifact passes its checks.

## Acceptance
- A missing changelog entry fails fast-fail in < 1 s. A stock-MapLibre sideload APK is refused. Each precheck has a
  failing case shown before the lock is taken.

Depends: —. Size: M. Owner: no.
