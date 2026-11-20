# T894 — The app's version files per track and per version (after 0.8.2)

Status: DONE (7bc81d896, 8e433846a; in 0.8.2 on app/integrate-082, ledger closed 2026-11-19 night).

From the second independent review of T888 (2026-11-19 ~00:27):
1. Medium, Play: play.json names whatever was uploaded last, on any track (appUpdater.native.ts ~L89). With 0.8.3 in
   closed testing and 0.8.2 offered to a production user, the popup says "Version 0.8.3 is out", Update installs 0.8.2,
   and `popped=0.8.3` stops the real 0.8.3 from popping. Fix: compare play.json's `build` with the version code Play
   offers (if the in-app update module reports it), or write the file per track.
2. Low, iPhone: production appstore.json is written at the TestFlight upload; a newer upload ahead of the store's version
   leaves store users with "no notes published". Fix: one file per version (`app/ios/<ver>.json`).
3. Low, tests: the App Store port's version-equality guard; the iOS dry-run's bucket (staging by default,
   bc-fishing-regulations with --production).

Built (lane `app/t894-version-files`, from app/integrate-082 57be9eb3b, 2026-11-19 ~00:52 MST/UTC-7):
1. Play: ONE FILE PER BUILD, named by the versionCode Play offers, not per track. Play's check reports the offered
   build's code (`expo-in-app-updates` `storeVersion` = `availableVersionCode`), and the owner PROMOTES an internal build
   to closed testing / production in Play Console (T371), so a track baked in at upload would be wrong after a
   promotion; the code follows the build. build-apk.sh --play puts `app/android/play/<versionCode>.json`, then
   play.json (same content; 0.8.1 installs, the precheck and --publish-only still read it); --publish-only redoes both.
   The app (core `storeUpdateCheck` / `storeVersionFileUrl`, called by appUpdater.native.ts) reads
   `play/<offered code>.json` and says its version only when its `build` is that code; else (missing file, another
   build) it reports and leaves the notice in the bell with no version (no popup). The once-key is that version.
2. iPhone: build-ios-testflight.sh puts `app/ios/<version>.json`, then appstore.json; the app reads
   `app/ios/<the App Store's version>.json` and uses its bullets only when it names that version (reported otherwise).
   The script refuses an app.json version that is not x.y.z (it names the file).
3. Tests: core appUpdate.test.ts "(T894)…" (Play reads the offered build's file, not play.json's 0.8.3; a file naming
   another build; missing file / in progress; App Store reads <ver>.json; the version-equality guard; path safety),
   each failing with its fix mutated out; test_staging_scripts_fail_safe.py: the iOS dry run's bucket (staging by
   default, bc-fishing-regulations with --production), one file per version with the redo hint, Play's own file
   before play.json in both paths.
No build command changes: the same scripts write the new files. The pruner (prod_prune) never touches `app/`.

## Review of 7bc81d896 (2026-11-19 ~00:58): nothing blocks; two older holes fixed in 0.8.2
- deploy/staging/upload.sh's staging clear now spares `app/ios/**` (it wiped appstore.json and the per-version files).
- build-apk.sh writes the build's environment (`env`) into play.json; `--publish-only` refuses a file of another (or no)
  environment before reading the published file or putting anything. Tests in test_staging_scripts_fail_safe.py.
