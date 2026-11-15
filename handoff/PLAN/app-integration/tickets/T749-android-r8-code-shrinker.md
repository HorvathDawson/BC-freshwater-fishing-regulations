# T749 — Android: turn the code shrinker (R8) back on, with keep rules, behind a device proof

Status: IN REVIEW (2026-11-15: trial on branch `perf/r8` passes all six emulator benches behind `CANIFISHTHIS_R8=1`, off by default; left (Claude): the build script keeps mapping.txt; owner: OWNER-TODO "T749, Android's code shrinker R8")

Source: T741 survey 2026-11-14 (owner, verbatim: "can you make sure to keep working on optimizations? make app faster
and lighter for both ios and android"). History: T250, where the first APK built with R8 died at start and R8 was
turned off.

## Why
The Android bundle's compiled Java/Kotlin code (dex) is 30.4 MB of the 38.8 MB AAB (0.7.10 build 32) because R8 is off.
R8 typically removes 50 to 70 % of dex; estimated 6 to 10 MB off the Play download. Estimated, not measured.

## Work
1. On a branch (`perf/r8`): R8 on for release builds with keep rules for what reflection reaches: WorkManager and Room,
   the patched MapLibre, pdfium, the background-download module, expo modules, Hermes/JNI entry points. Find the T250
   crash cause first (its stack is in T250) and pin it with a keep rule and a note.
2. Measure: AAB and per-device download size before and after (`bundletool` is not installed: install it or say the
   figure is an estimate).
3. Device proof on the emulator: `run.mjs install crash-bed bg-install regs-open tap-stalls memory`, all as on the
   build without R8; then a Play internal build on the owner's phone before it reaches closed testing.

## Acceptance
- The app starts and every bench above passes with R8 on; the size before and after is in this ticket.
- The owner's phone check on a Play internal build is recorded.

Risk: high (a missing keep rule crashes only on the path that needs it). Size: M. Owner: phone check.

## Phase 1 (2026-11-15, branch `perf/r8`; no build run yet)
**The T250 crash, exactly.** AGP 8+ runs R8 in full mode. Room 2.5.0 (WorkManager 2.9.1's, i.e. expo-background-task's)
ships only `-keep class * extends androidx.room.RoomDatabase`: in full mode that keeps the class name but NOT the no-arg
constructor of `androidx.work.impl.WorkDatabase_Impl`. Room builds it by reflection (`Class.forName(..._Impl).newInstance()`),
gets InstantiationException and throws "Failed to create an instance of class androidx.work.impl.WorkDatabase"; that runs
inside androidx.startup's InitializationProvider (WorkManagerInitializer), so the app dies before its first frame.

**The flag.** `CANIFISHTHIS_R8=1` in the environment of `expo prebuild` (session.sh / build-apk.sh pass their environment
through). `plugins/with-no-minify.js` then writes `android.enableMinifyInReleaseBuilds=true` and appends
`plugins/r8-keep-rules.pro` to android/app/proguard-rules.pro. Unset / "" / "0" = today's build (R8 off); any other value
is refused; app.config.js refuses it in a production build. Resource shrinking stays off (RN finds images, the
notification icon and the splash by resource name). `tools/r8-config.test.ts` pins it through the real
`expo config --type introspect`: flag off byte-identical to flag absent (and to the config before this change, checked by
hand), flag on differs only in that property; mutation-checked (forcing "true" fails 3 of 7).

**Keep rules** (`app/apps/mobile/plugins/r8-keep-rules.pro`, each with its reason): 1 Room `<init>()` (the T250 crash);
2 `org.maplibre.android.maps.MapView` name + `onLowMemory` (MemoryTrim compares the class name and calls it by
getMethod); 3 MemoryTrimModule's JNI `purge` (also in the default optimize file; pinned); 4 MapLibreSurfaceView's
`detachedListener` / `renderThread` fields (maplibre-react-native's detach guard, getDeclaredField; failure is caught,
so it would vanish silently); 5 RN's `HasChildPressedStateDelay` (gesture-handler's Class.forName + getMethod);
6 `**$$ReactModuleInfoProvider` + `@ReactModule` (RN's own reactnative.pro rules, not shipped in the react-android AAR:
the fallback reads the annotation with `!!`); 7 SourceFile/LineNumberTable. Already covered by shipped consumer rules
(read from the AARs/packages): expo-modules-core (modules, records, JNI), expo, expo-background-task, expo-task-manager,
expo-notifications, react-android (DoNotStrip, native methods, ReactProp), the patched MapLibre AAR (its rules are
upstream's; the patch is C++ only), pdfiumandroid (`io.legere.pdfiumandroid.** { *; }`, which also covers the Gson
table-of-contents reflection in react-native-pdf), WorkManager's workers. Manifest classes (bg-download's receiver) are
kept by AAPT.

## Phase 2 plan (heavy lane 2; nothing uploaded, nothing merged)
Run from this worktree, through the MAIN checkout's heavy.sh. The worktree has no `cache/`, so MLN_LOCAL_MAVEN names the
main checkout's lean repo (session.sh then needs TESTBED_APK_TAG, which also keeps the two APKs apart). Both builds use
`--crash-stub` (crash-bed needs it; crash reports never reach staging) and `run.mjs --no-r2` (nothing written to staging).
```
M=/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations
export HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock MLN_LOCAL_MAVEN=$M/cache/maplibre/fslean-m2
# A. flag OFF control: build only
TESTBED_APK_TAG=nor8 $M/scripts/heavy.sh t749-nor8 30 -- bash app/tools/testbed/session.sh --build --crash-stub
# B. flag ON: build + the benches
CANIFISHTHIS_R8=1 TESTBED_APK_TAG=r8 $M/scripts/heavy.sh t749-r8 30 -- bash app/tools/testbed/session.sh --build \
  --crash-stub --max-min 75 -- node app/tools/testbed/run.mjs --no-r2 install crash-bed bg-install regs-open tap-stalls memory
# C. same boot comparison: one session that runs regs-open + memory on the R8 APK, then installs the nor8 APK and repeats
```
Sizes: `stat` of each APK; dex = sum of `classes*.dex` in `unzip -lv`; per-device download: bundletool is not installed,
so the Play figure is an ESTIMATE (APK delta) unless its jar is fetched from github.com/google/bundletool/releases (said if so).

**Pass lines (R8 APK):** the app starts (no `FATAL EXCEPTION` / `AndroidRuntime` for ca.canifishthis.app in logcat);
install, crash-bed, bg-install, regs-open, tap-stalls, memory PASS as on the flag-off APK; regs-open cue within
OPEN_CUE_MAX (150 ms) and tap-stalls under STALL_MAX (300 ms), with the load average and `heavy.sh --status` beside each
timed number; memory's trim step logs `MemoryTrim … N map view(s) told` with N ≥ 1 (rule 2) and logcat holds no
"Failed to install surface view detach guard" (rule 4); the rule book opens in memory's viewer step (pdfium); the
background task is registered (bg-install; WorkManager's DB opened = rule 1). A crash: read the stack, add the rule with
its reason, rebuild; at most 4 rounds. Dex and APK sizes before/after in this ticket; R8's mapping.txt kept with the APK.

## Phase 2, part A (2026-11-15 06:47–06:56, heavy lane 2): the two APKs, MEASURED
Same commit 9fe5e2d6c (clean), both with the patched MapLibre (libmaplibre.so build id 7bf3dc29, T420 lean), arm64,
test-bed env (staging, test channel, `--crash-stub` crash URL, cleartext + profileable patches), release build.

| | flag off (`app-testbed-stub-nor8.apk`) | `CANIFISHTHIS_R8=1` (`app-testbed-stub-r8.apk`) | change |
|---|---|---|---|
| APK | 62,424,108 B | 54,970,637 B | −7,453,471 B (−11.9 %) |
| dex, uncompressed (`classes*.dex`) | 30,358,616 B in 5 files | 7,892,984 B in 1 file | −74 % |
| dex, as stored in the APK | 11,024,320 B | 3,577,539 B | −7.45 MB |
| native libs (`lib/`, stored) | 39,662,720 B | 39,662,720 B | 0 |
| assets | 5,009,184 B | 5,009,167 B | ~0 |

Measured with `stat` and `unzip -lv` on the two APKs. Prebuild printed `android.enableMinifyInReleaseBuilds=false` / `=true`
and the R8 build's proguard-rules.pro held the T749 block; R8's `mapping.txt` (65 MB) is kept beside the APK
(`app/tools/testbed/out/mapping-r8.txt`, gitignored). The Play download is an ESTIMATE: bundletool is not installed, and
Play compresses dex in its own way, so the stored-dex delta (~7.4 MB) is the best figure for the Play download saving. Not
measured: an AAB.

How built: a build-only copy of session.sh's step 1 (`--crash-stub`), the same prebuild, manifest patches, env, Gradle line,
MapLibre build-id check and `build-stamp.mjs write`, without the emulator boot that session.sh always does after a build
(part A was builds only, while the simulator lane ran). The APK names are session.sh's own (`--crash-stub` +
`TESTBED_APK_TAG=nor8|r8`), so part B reuses them via session.sh with `--allow-stale` (this ticket commit moved HEAD; the
app code is 9fe5e2d6c's). `pnpm -C app check:fast` green after the builds.

## Phase 2, part B (2026-11-15 08:34–10:04, n1_pixel, heavy lane 2, `--crash-stub`, `run.mjs --no-r2`: nothing written to staging)
R8 APK (`app-testbed-stub-r8.apk`), report `out/runs/2026-11-15T15-35-44`, load 4.1 at start, 4.0 at end:

| bench | R8 | notes |
|---|---|---|
| install | PASS 4/4 | first-run download (DownloadManager, bg-download module) → map in 245 s; no FATAL at start |
| crash-bed | PASS 5/5 | promise, JS error, native SIGSEGV each reached the local stub |
| regs-open | PASS 26/26 | cue 31–86 ms (limit 150), page 338–373 ms |
| bg-install | PASS 7/7 | WorkManager/Room opened (the T250 path); "opened by launch, the tap did not open it": same wording in the earlier non-R8 runs `apk-0400`, `overnight-1030` |
| tap-stalls | PASS 5/5 | worst stall 141 ms (Run timing; limit 300) |
| memory | PASS 14/14 | rule book (pdfium) opens at p.25; trim: "1 map view(s) told, purge 2" (rules 2 and 3 work) |

**One boot, both APKs** (report `16-12-28` = R8, `16-31-17` = flag off; load 3.3–4.4 throughout):
- Cold start, `Start proc` → first ReactNativeJS `[drawn]`, 6 each, `am force-stop` between: R8 1197/1700/1468/1491/1467/1452 ms
  (median 1468); flag off 1657/1733/1487/1687/1526/1657 (median 1657); R8 again after the flag-off run 1699/1549/1572/1655/1542/1710
  (median 1614). The second R8 set is within ~45 ms of flag off: no cold-start difference shown beyond drift on this boot.
- regs-open: both 26/26; cue R8 31–86 ms vs off 40–97 ms, page 334–361 vs 330–372 ms: the same.
- memory: R8 13/14 on that boot: the Salmon step's Maestro `tap-text` failed with no driver output while the screen showed
  the map and the SALMON button (no FATAL, the next step passed); rerun alone on a fresh boot (`16-53-01`, load 3.9): 14/14.
  dumpsys "Code" at boot: R8 51.9 / 38.3 MB (rerun) vs off 70.6 MB; boot PSS R8 247.7 / 221.4 vs off 282.3 MB.
  Rule book open (first/held): R8 8405/8041 and 8297/8041 ms vs off 7907/7823 ms (R8 3–6 % slower on this path; two R8
  samples vs one off, unconfirmed).
- R8's mapping.txt shows the reflection targets kept by name: `MapLibreSurfaceView`'s `detachedListener` / `renderThread`
  (the class itself is renamed; the guard uses the class literal), `MapView`, `MemoryTrimModule`, `HasChildPressedStateDelay`,
  `WorkDatabase_Impl`. The detach guard's failure log line could not be grepped (the scenarios clear logcat).

No keep-rule round was needed: the first R8 APK passed. Left: a Play internal AAB with the flag (Play's own processing,
the download size from Play) and the owner's phone check.

## Owner decision (2026-11-15 10:32)
Owner, verbatim: "if that build passes the tests then it should be good to release". Asked which build and which
release, he picked: the R8 build to Play INTERNAL. So: build from the shared branch as it is today plus this branch's
switch and keep rules, switch on; keep `mapping.txt` beside the Hermes source map; re-run the six emulator benches on
that exact build; if all pass, upload to Play internal testing for the owner's phone walk (update, notifications, a spot
photo, location, the rule book). Production stays the owner's.

### How it runs (agreed with the Android chat, 2026-11-15 10:37)
- Pin: shared head d29dffccc (app code = 8d823e7b9, app CI green; pipeline CI green at 6a12b63cc). The Android chat
  holds `app/` pushes until the upload.
- iOS chat: branch `perf/r8-play` = the pin + `perf/r8` + the `mapping.txt` step in `deploy/staging/build-apk.sh`
  (flag off = today's script; flag on = mapping kept beside the Hermes source map under `app/dist/aab/`, fail if none)
  + version 0.7.11 and a notes line. Test-bed APK with the flag on, then install, crash-bed, bg-install, regs-open,
  tap-stalls, memory (`--no-r2`). All pass -> merge into `redesign/stream-sections` (flag default off).
- Android chat: reads the script diff, then runs `CANIFISHTHIS_R8=1 deploy/staging/build-apk.sh --play` on that head
  (it bumps and commits versionCode, uploads to the internal track). Build 33.
- The build also carries the shared branch since build 32: T737, T747, T750.
