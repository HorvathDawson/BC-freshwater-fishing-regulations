# T749 — Android: turn the code shrinker (R8) back on, with keep rules, behind a device proof

Status: OPEN (proposal from T741; needs an emulator run and then a phone before it can ship).

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
