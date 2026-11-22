# T978 — The release build's checks never fail on this machine's state, only on code

Status: IN PROGRESS (main chat 2026-11-21, with T993).

Source: owner 2026-11-20: "Add a ticket to clean up all of these test to make these sort of failure impossible". The 0.8.4
production build stopped four times in fast-fail, never on a code defect:
1. A worktree without `cache/` (the patched MapLibre AAR), so the AAR lookup came back empty and the build exited with no message.
2. No feeds run token in a worktree. The email precheck failed; it was built with `--no-email`.
3. `data/generated/inseason/resolved.json` exists only in the main checkout, so `test_mini_release` raised.
4. The committed mini release was stale after merges. It needed `python -m pipeline.tools.mini_release --write`.
5. "review marks have landed" passes only where the owner's marks are (the main checkout).
6. A stale live export (no `grp_runs`) failed `test_answers_27` and `test_t801_mainstem_tag`. The same two tests fail on
   clean dev until the export is rebuilt.

## Work
- Split the tests into code tests and live-data tests.
  - Code tests use fixtures only (the mini release or invented data) and are the build's gate.
  - Live-data tests run in the data release step against the release they check, never in the app build.
- A test that needs generated or live data builds its own (as the mini chain does), or is a live-data test.
- Merges that change mini-release inputs re-cut it, or CI fails at merge, not at build time.
- build-apk / build-ios-tf in a worktree: one precheck lists every missing link (cache, .venv, resolved.json, run token)
  with the fix. No silent exits.
- The marks-landed check moves to the data release (it is about data, not the app).
- MapLibre (owner 2026-11-20): the owner's PR merged upstream (https://github.com/maplibre/maplibre-native/pull/4919).
  Build MapLibre from upstream `main` (pinned to a commit) instead of the local lean T420 patch in `cache/maplibre`.
  Then no worktree needs the patched AAR link. Keep that until a MapLibre release includes the change, then use the
  release. Check the AAR build-id step and the "MapLibre patched always" rule (T893) against it.

## Acceptance
- A fresh worktree of a green dev runs the build's fast-fail clean with no data rebuilt.
- A stale export or marks state fails the data release step, not the app build.

Depends: —. Size: M. Owner: no.

## Lane W4, 2026-11-20 (lane/w4-ci): pipeline-ci + the release's fresh-copy run green again
Eleven no-data tests read this machine's state, so CI (and the fresh clone) failed where this checkout passed:
- 9 (test_part_touches 7, test_part_runs 1, test_mini_release 1): `export.read` reads the in-season notices' end
  tokens, whose resolution is generated since T679 #11 (`data/generated/inseason/resolved.json`, absent on CI). The
  hand-made-bundle tests now point the export at a notice file with no matches (`bundle_fixture.no_notices`); the mini
  test reads the mini tree's notices, as its chain does. The export's refusal of a missing resolution is unchanged.
- 2 (test_answers_27 t517, test_t801_mainstem_tag): hand-made waters lacked the export's `grp_runs` (T679 #17 moved the
  join into the export); each test now gives its water `export.read.group_runs(parts)`, as the export writes it.

## Owner 2026-11-21 12:40
Asked for the 0.8.5 batch: "No, next chat". Waits for the chat after 0.8.5.

## Owner 2026-11-21 (new chat after 0.8.5)
Picked for this chat together with T993 (build/audit streamlining; no release in this chat). See T993's note of the
same date for the verbatim answer.

## Owner 2026-11-21, dialog: MapLibre 0002
Asked: post 0002 upstream / build main + 0002 / leave it. Owner, verbatim: "Can we measure the different of patch 2 I
think we thought it wasn't worth it". So: measure 0001-only (upstream main) against the lean patch (0001 + 0002) on the
stressed emulator, as T420 measured lean vs full; the owner decides on the numbers. (T420's "lean vs full" run compared
the lean patch with the full one, not 0001 alone, so this is not measured yet.)

## Built (lane/t978-build-checks, 2026-11-21)
Commit: a323ef2af (code + tests). Not merged; the main chat reviews and merges.
1. One precheck lists every missing link with its fix (`deploy/lib/build-links.sh`, sourced by both builds). From a worktree
   the fix is a link or copy of the main checkout's (`ln -s <main>/.venv .venv`, `mkdir -p cache/maplibre && ln -s
   <main>/cache/maplibre/fslean-m2 …`, `cp <main>/app/workers/feeds/.run-token …`, the debug keystore); in the main
   checkout it is the command that makes it. build-apk.sh: .venv, app/node_modules, JDK, Android SDK, the review app's
   node_modules (only when fast-fail will typecheck it), keystore, run token, the MapLibre repo — the silent exit is gone
   (the `find` no longer runs on a missing dir under `set -e`; `mln_check` refuses "no AAR" instead of reading it as
   stock). build-ios-testflight.sh: a new precheck before the lock (xcodebuild, pod, node, team id, API key + its .p8,
   node_modules + wrangler login when uploading; `--precheck` runs only it; it no longer stops at the first missing
   secret after the lock wait). session.sh's AAR message names the same fix. `.gitignore`: `/.venv`, `/cache` (a link is a
   file; `.venv/` did not match it, so a linked .venv showed as a changed path).
2. Marks-landed (and wrong saves, override ids on the live atlas, goldens vs LATEST, /changes budget) are `data: …` steps
   in fast-fail: run only with `FAST_FAIL_DATA=1`, which `release.run_fast_fail` sets; elsewhere each says "left to the data
   release" and the pass line counts them. The release preflight still runs marks / wrong saves / classified itself.
3. Gate tests on machine state: W4 (above) fixed the six tests; fast-fail on a fresh worktree (no data/generated,
   no data/release, only .venv linked and `pnpm install --offline`) passed with the default base (172 s) and with the build's ship base (FAST_FAIL_BASE = the last Play build 558c527c3: the whole no-data tier, 35 chunks, 923 s, through heavy.sh).
   No T777 work; overlap: none of the moved steps is a test file, so T777's curated-root sandbox is still the open part.
4. Mini release: CI already failed on it (`mini_release --check`, prints `--write`); it now also puts the command in a
   `::error` annotation. fast-fail's trigger was four folders (grown one per miss); it is now every input of the mini
   chain (any pipeline source outside tests, its fixtures, data/curated, config, pins), so the lane's run and the run after
   a merge (default base = every unpushed commit) re-check it before any build.
Tests: test_release_follows.py (data steps left without the variable, run with it; every machine-data command is a data
step; the release sets it), test_staging_scripts_fail_safe.py (missing MapLibre repo listed, never silent; the link fixes
in a scratch repo + worktree; TestFlight precheck lists both missing secrets before the lock). AGENTS 20a has one line.

## MapLibre item: STOPPED (gap)
Upstream PR 4919 (merged 2026-11-20, merge commit 9cf8446d) is our 0001 only (source_state, render_tile, tile.hpp,
geometry_tile + tests). The lean patch is 0001 + 0002 (`upstream/0002-…`: `updateVertexVectors` returns whether a vertex
was rewritten; a bucket is marked modified / re-uploaded only then; 5 bucket files + paint_property_binder.hpp). Upstream
main at 3a6b9557 (2026-11-21) still has `void updateVertexVectors` and `updateModified(true)` unconditionally. So a build
from upstream main drops half of what the shipped AAR carries; per the brief, not built. Options for the owner: post 0002
as its own upstream PR (it was drafted for that) and build from main once it lands; or build main + 0002 as a small local
patch (no longer the whole lean patch). iOS: the patch was never in the iOS build (T893); unchanged.

Left (Claude): the MapLibre switch once the owner picks; the TestFlight build still runs no fast-fail of its own (AGENTS
20a says it should; not added here); `pnpm check`'s vitest tests that use `liveRelease()` still read LATEST when a checkout
has one (a fresh worktree has none; T777's ground).

## MapLibre 0002 measured (lane/t978-maplibre-measure, e69df5274, 2026-11-22)
Owner, verbatim: "Can we measure the different of patch 2 I think we thought it wasn't worth it". Render-thread CPU,
seconds, mean [range], 3 blocks per build alternated in one boot (Android emulator; 1 core at the 60 fps cap: the Mac
was at load 18-40 from six orphaned headless Chrome trees, so T420's E-core stress was not usable):

| place | lean (shipped: 0001 + 0002 on the release) | upstream main, 0001 only | main + 0002 |
|---|---|---|---|
| Coquitlam z13, held | 25.5 [25.3-25.7] | 20.5 [19.7-22.1] | 20.1 [18.6-21.6] |
| Coquitlam z13, 160 flings | 96.7 | 67.1 [66.8-67.3] | 67.5 [59.3-79.4] |
| Fraser Valley z11, held | 22.6 | 15.4 [14.1-16.2] | 14.6 [12.9-15.5] |

0002 is within noise (-2% / -5%, ranges overlap; upload share 19.5-21.7% vs 21.4-23.0%). Upstream main is 20-32%
cheaper than the shipped lean AAR from upstream's own changes. On main: colours after Map -> Conditions -> Map correct,
0 ANRs in tap-glide, build-id proof PASS. Build: full AAR 703 s, rebuild 264 s, APK 67-93 s; `build.sh` needs only the
tag -> a pinned upstream commit, no patch. Owner decides (OWNER-TODO): build from a pinned upstream main commit with no
patch (recommended), or keep the lean AAR.

## Owner 2026-11-22, dialog: MapLibre for 0.8.6
Owner, verbatim: "Pinned upstream main (Recommended)": build from one pinned upstream main commit, no patch of ours;
switch back to a release tag once upstream releases one containing 0001.

## Built: the pinned upstream MapLibre (build/T978-maplibre-upstream-main, 2026-11-22)
- **The pin, in one place:** `app/tools/maplibre-android/pin.env`: `MLN_COMMIT=3a6b955796b3509f184e9bf719e82cb2b432b958`
  (upstream main, 2026-11-21; holds PR 4919 = 9cf8446d), `MLN_VERSION=13.7.1-main.3a6b955` (the local artifact's version,
  naming the commit). Its header says why main (no android-v* release holds 9cf8446d; the newest, android-v13.7.1 of
  2026-10-15, does not) and when to move (the first release tag that contains it: `git merge-base --is-ancestor`).
- **No patch:** `app/tools/maplibre-patch/` became `app/tools/maplibre-android/`. `build.sh` clones the pinned commit,
  refuses a source tree with any local change (the version file aside), applies nothing, and publishes to
  `cache/maplibre/<MLN_VERSION>/` with `SOURCE.txt` (the commit). The T420 patches, their upstream drafts and
  `tests.sh` moved to `archive-t420/` (no build reads it; its README says so).
- **Selection + proof:** `deploy/staging/build-apk.sh` (every mode, `--production` too) and `app/tools/testbed/session.sh`
  default `MLN_LOCAL_MAVEN` to `cache/maplibre/<MLN_VERSION>`; build-apk's precheck (`deploy/lib/build-links.sh`
  `build_links_mln`) refuses an AAR that is not the pinned version (the old `fslean-m2`, an experiment), so nothing can
  silently ship the lean patch; the APK/AAB build-id proof is unchanged and its line now reads `libmaplibre.so build id
  <id> (pinned upstream 13.7.1-main.3a6b955)`. The test-bed stamp says `pinned upstream <version>`, an experiment repo
  (with `TESTBED_APK_TAG`) `LOCAL AAR …, not the pin`; `patched` is only read back from old stamps.
  `--stock-maplibre=<reason>` stays (refused with `--production`).
- **iOS: unchanged.** The iOS build never had the patch (T893): it takes MapLibre iOS 6.31.0 from the binding's pod;
  `build-ios-testflight.sh` reads no `MLN_*`, and app.config.js only touches the Android native version and Gradle
  repositories. The fix reaches iOS only with an upstream iOS release.
- **Docs:** `app/tools/maplibre-android/README.md` (new), `app/tools/testbed/README.md`, `app/deps.md`,
  `handoff/PLAN/ios/START-HERE-ios.md` (its commands read the pin), comments in app.config.js / with-local-maplibre.js.
  AGENTS has no MapLibre line. The auto-memory note "MapLibre patched always" is the main chat's to update.
- **Tests:** `app/tools/maplibre-pin.test.ts` (new: the pin's form, build.sh applies nothing and refuses a changed tree,
  every build path reads the pin, no build file names fslean / maplibre-patch / the patches), `testbed-stamp.test.ts`
  (pinned/local/stock stamps), `test_staging_scripts_fail_safe.py` (a lean AAR refused by the precheck and by
  `--check-maplibre`; the default repo named from the pin; build-links' worktree link and pinned check).
- **Cache:** the main checkout's `cache/maplibre/13.7.1-main.3a6b955/` (built by the new build.sh). `fslean-m2` is left
  in place, unused (refused by every build); delete it when 0.8.6 is out.
- **Fixed on the way:** the worktree fix the precheck prints (`ln -s <main>/cache/maplibre/… cache/maplibre/…`) never
  worked: a bare `find` does not descend into a symlinked folder, so the link still read "found: none" (latent since
  the fslean link). `find -L` in build-links.sh, build-apk.sh and session.sh; a test links a worktree's repo.
- **Proven (2026-11-22):** `build.sh` on the pin: 187 s (a reused source tree; full build measured 703 s), AAR build id
  adb668b692a7… = the measured m0001 build's (same source). Test-bed APK by the default `session.sh --build` (commit
  06179d9fe clean; stamp "libmaplibre build id adb668b6… (pinned upstream 13.7.1-main.3a6b955)"), emulator: 
  `check-loaded.sh` PASS (installed APK = the pinned AAR's build id, `SOURCE.txt` names 3a6b9557, mapped executable in
  the app); colours after Map → Conditions → Map at Coquitlam z13 and Fraser Valley z11: coloured, nothing grey
  (`out/runs/t978-pin-verify/`). Tests: `maplibre-pin.test.ts` + `testbed-stamp.test.ts` + `r8-config.test.ts` 40/40;
  `test_staging_scripts_fail_safe.py` + `test_deploy_gates.py` + the heavy-precheck test 66/66; check-tickets green.
  Not run: a build-apk.sh build (the Play/production paths are covered by the precheck and `--check-maplibre` tests only).

Main line, same dialog (the coordinator's note):
"Pinned upstream main (Recommended)": build from one pinned upstream main commit, no patch of ours; move to a release
tag once upstream releases one containing 0001. The switch: branch `build/T978-maplibre-upstream-main` (in progress).

## Review hardening (review/T978-maplibre, 2026-11-22): "every Android build uses the pinned build"
- **app.config.js:** `plugins/with-pinned-maplibre.js` (an Android gradle.properties mod, so `expo prebuild -p ios` never
  meets it): a PRODUCTION Android config (EXPO_PUBLIC_APP_ENV=production or EAS_BUILD_PROFILE=production) with no
  MLN_LOCAL_MAVEN, or with a repo of another version, is REFUSED, the fix named. Chosen for dev: any other Android
  config (`pnpm android`, a dev prebuild) only WARNS — a fresh checkout has no built AAR, and everything that ships goes
  through build-apk.sh, which refuses on its own.
- **build-apk.sh:** `--stock-maplibre` is refused in every mode (the staging page, Play, production all reach a phone);
  it is the test bed's only. The stock code path is gone.
- **build.sh:** untracked files count as a change too (`--untracked-files=all --ignore-submodules=none`); every run
  brings all submodules up, nested ones included (`submodule update --init --recursive`), not only after the first
  clone; a commit whose `Versions.kt` asks for another NDK is refused. pin.env's move steps name build.sh's NDK/CMAKE.
- **build-links.sh:** `mln_pinned_aar` (used by build-apk.sh's outer and inner runs) and `build_links_mln` take the AAR
  only when its file name is the pinned version AND the repo's `SOURCE.txt` names `MLN_COMMIT`.
- Tests: r8-config.test.ts (production refused without / with another repo, EAS profile too; dev made; the check wired),
  maplibre-pin.test.ts (build.sh, build-apk.sh, build-links.sh lines), test_staging_scripts_fail_safe.py (no mode takes
  `--stock-maplibre`; SOURCE.txt missing / another commit refused; the gate tree builds against a stand-in pinned repo);
  a mutation of the SOURCE.txt check fails the build-links test. build.sh rerun on the real tree: submodules recursive,
  clean, built.

## Independent review of the hardening (2026-11-22)
Verdict sound: no production or Play path can ship a non-pinned or stock AAR (iOS prebuild unaffected, checked with
expo's mod compiler; `deploy/prod/build-apk.sh` takes the pinned default). Fixed (58c40ed91): three guards no test ran,
`mln_check`'s build-id comparison, `mln_pinned_aar`'s file-name half and the inner no-AAR stop, now run by
`test_check_maplibre_proves_the_archive_against_the_pinned_aar` (each mutation killed); the test bed stamped "pinned" from
the file name alone and now uses `mln_pinned_aar`. Open by design: EAS preview/development profiles only warn (EAS unused);
the plugin checks the version name, not SOURCE.txt (only a non-build-apk path such as `eas build --local`). build.sh does
not check CMake (a too-old one fails Gradle loudly).
