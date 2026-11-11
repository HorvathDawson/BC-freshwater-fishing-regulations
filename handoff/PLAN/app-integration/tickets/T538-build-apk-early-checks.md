# T538 — build-apk.sh: cheap checks before the lock, changelog required, MapLibre proof on both paths

Status: IN PROGRESS — fcbcda2ba (built; prechecks, stock-APK refusal and changelog shown); left (Claude): one real build-apk.sh --play run (bump commit after the checks, play.json + email), and the expected build id beside the patch in app/tools/maplibre-patch (tools lane).

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

## Built (2026-11-10, fcbcda2ba)
1. `scripts/check-changelog.mjs` (0.09 s): app.json `expo.version` has an entry with ≥ 1 bullet and it is the newest.
   In fast-fail (early step) and `pnpm check:fast` (`changelog`). build-apk.sh refuses without it (`--no-notes=<reason>`);
   `--play` refuses the version Play builds already read (staging `play.json`; `--same-version=<reason>`).
2. Play release notes and both emails = this version's changelog bullets (+ the MapLibre build id line in the email);
   the header no longer says "git log"; `--format=%s` is gone.
3. The precheck runs before the lock (outer process, before CI / fast-fail / `exec heavy.sh`), lists every failure, then
   stops: changelog, Play version, upload keystore + its storeFile + certificate (or the debug keystore), Play service
   account + one `--max-version-code` call, wrangler login (by its words: `whoami` exits 0 when logged out), run token,
   llvm-readelf, the patched AAR's build id, app.json clean (`--play`), uncommitted app/ paths listed (`--uncommitted=
   <reason>`). `--precheck` runs only that.
4. One `mln_check` for both paths: the sideload APK is now proved too; the id is on the staging page, in play.json
   (`maplibre`) and the emails. `--check-maplibre=<apk|aab>` runs that proof alone.
5. `--play --publish-only` redoes play.json + the email from `app/dist/aab/play.json` (now written before the Play
   upload); it refuses a local file older than the published build. A failed play.json put after the upload names it.
6. versionCode bumped in the build worktree; committed (app.json only) after the AAB's signature + MapLibre checks.

Proof: `node scripts/check-changelog.mjs --version 9.9.9` → rc 1 in 0.09 s (and 0.7.3 → "not the newest").
`--check-maplibre` on an APK zip of the stock 13.6.1 .so → refused (aab47813… ≠ 7bf3dc29…), rc 1; the patched one → ok.
`--precheck` (real tree): sideload FAIL 3 uncommitted app/ paths; `--play` also FAIL "0.7.4 is the one Play builds already
read". In a scratch worktree with a fake HOME and no NDK: upload signing, Play service account, run token, debug
keystore, readelf, AAR build id, wrangler login each FAIL — all before the lock. `--dry-run` / `--play --no-upload
--dry-run` print the new order (bump in the worktree, commit after the checks, changelog notes). `--play --publish-only
--dry-run` refused the local build-25 play.json (published 26). `test_staging_scripts_fail_safe.py` pins the order.

Not verified / left: a real `build-apk.sh` and `--play` run (bump-after-checks, the APK id check on a real APK, the
publish-only put + email); "expected id recorded beside the patch" (app/tools/maplibre-patch/, the tools lane — the
expected id is still read from the AAR itself).
