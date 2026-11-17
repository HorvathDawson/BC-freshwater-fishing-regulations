# T749 — Android: turn the code shrinker (R8) back on, with keep rules, behind a device proof

Status: IN REVIEW (2026-11-15: merged into `redesign/stream-sections` 35dce9bf3 behind `CANIFISHTHIS_R8=1`, off by default; six benches pass on the merged code; both CIs green; left (Claude): after the Android chat's Play internal build (it waits for the owner's "regenerate the two proposal rows"), put the phone walk on the owner's row and record his result; owner: OWNER-TODO "T749, Android's code shrinker R8")

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

## The R8 Play internal build: mapping step, sequence, phone walk
Owner, verbatim: "if that build passes the tests then it should be good to release"; picked "R8 build to Play internal":
the shared branch as it is, R8 on, the six emulator benches on that build, then Play INTERNAL for the owner's phone walk.
The coordinator uploads; this lane never does.

**Mapping kept (on `perf/r8`).** `deploy/staging/build-apk.sh` `keep_map` now also runs `sourcemaps.mjs keep-r8` when
`CANIFISHTHIS_R8=1`: R8's `android/app/build/outputs/mapping/release/mapping.txt` is copied to
`data/sourcemaps/<version>-b<code>-<sha7>/android/r8-mapping.txt`, beside `index.android.bundle.map`. A missing or empty
mapping FAILS the build there, before the AAB is checked, committed (versionCode), copied or uploaded (and before the
sideload APK is copied). The precheck (before the heavy lock) says "R8 ON" / "R8 off", refuses a value other than unset/0/1
and refuses the flag with `--production`. Tests: `app/tools/sourcemaps.test.ts` (keepR8 keeps / refuses missing / refuses
empty), `app/tools/r8-config.test.ts` (build-apk.sh: `bash -n`; keep-r8 under the flag with no `|| true`; kept before
`commit_bump`, the Play upload and the APK copy; precheck before the lock; mutation-checked: an added `|| true` fails it);
`pipeline/tests/test_staging_scripts_fail_safe.py -k build_apk` still passes.

**Is a Play internal build "production" for the refusal?** No. `app.config.js` calls a build production only when
`EXPO_PUBLIC_APP_ENV=production` or `EAS_BUILD_PROFILE=production`; `build-apk.sh --play` (internal) exports
`EXPO_PUBLIC_APP_ENV=staging` and only `--play --production` (deploy/prod/build-apk.sh) sets production. So the flag is
allowed on the Play internal build as written, and refused on the production build (app.config.js and now the precheck).
Nothing blocks the internal build. Later, a closed-testing/production R8 build needs that refusal lifted on purpose, after
the phone walk is recorded here (not part of this step).

**Sequence** (the Android chat owns the build scripts and names the commit; nothing here runs before the coordinator's go):
1. Merge `perf/r8` into `redesign/stream-sections` (flag off = no behaviour change; it already holds today's shared branch).
   Pin that merge commit `<C>`.
2. Emulator benches on `<C>` with the flag (a test-bed APK: same Java/Kotlin and keep rules as the Play AAB; the JS env
   differs: test channel, crash stub, map lab):
   `cd <checkout at C>; export HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock CANIFISHTHIS_R8=1 TESTBED_APK_TAG=r8`
   `$M/scripts/heavy.sh t749-r8-c 30 -- bash app/tools/testbed/session.sh --build --crash-stub --max-min 80 -- node app/tools/testbed/run.mjs --no-r2 install crash-bed bg-install regs-open tap-stalls memory`
   (`MLN_LOCAL_MAVEN=$M/cache/maplibre/fslean-m2` too when the checkout is a worktree; pass lines as in Phase 2 above.)
3. If all six pass: the Play internal build, from the checkout at `<C>` (clean app/, app.json committed):
   `CANIFISHTHIS_R8=1 HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock deploy/staging/build-apk.sh --play --precheck` (no lock;
   expect "R8 ON"), then `CANIFISHTHIS_R8=1 HEAVY_LOCK=$HOME/.claude/heavy-job-2.lock deploy/staging/build-apk.sh --play`.
   That one run: changelog/version precheck (app.json's version needs a changelog entry and must not be the version Play
   builds already read, else `--same-version=<reason>`), CI + fast-fail, then under the lock: prebuild with the flag →
   `bundleRelease` (arm64, upload key) → Hermes map + r8-mapping.txt kept → upload-key signature check → patched-MapLibre
   build-id check → versionCode bump committed (app.json only) → AAB to `app/dist/aab/` → Play INTERNAL upload with the
   changelog bullets → `play.json` to the staging bucket → email. `--no-upload` stops after the AAB but still commits the
   versionCode bump, and there is no "upload this AAB" mode that also writes play.json, so the one-run form is the clean one.
   NOT the exact bytes the benches ran: the AAB is a separate Gradle run of the same commit (same R8 input, other JS env).
   Benching the AAB itself needs bundletool (not installed: a universal APK from the AAB with the upload key), say if wanted.
4. Owner's phone walk (below) on the Play internal build; record it here.

## Phone walk for the owner (Play internal R8 build)
Each item: what to do, and what a failure looks like. Any app closing by itself ("Can I Fish? keeps stopping") is a fail.
1. **Update through Play.** Install the R8 build from the Play internal link over the build you have. Open it. Fail: Play
   says it cannot install, or the app closes on its first open. Later, when the next internal build is out: the app's
   "Update" button opens Play's update sheet inside the app. Fail: the button does nothing or the app closes.
2. **First open and the map.** The map draws in colour, pan and zoom, tap a river: its regulations open. Fail: blank or grey
   map, a closed app, or a tap that opens nothing.
3. **A notification, and tapping it.** More → Notifications: turn on an alert on a gauge (or "new data ready"), close the
   app (swipe it away), wait until it fires (can take hours; the background check runs at most every ~15 min when Android
   allows). Tap the notification. Fail: nothing ever arrives while the same alert fires on the old build, or the tap
   closes the app / opens a blank screen.
4. **Background check after the app sat closed** (same as 3, read the other way): after a few hours closed, More →
   Notifications (or the Updates log) shows background runs since you closed it. Fail: no run listed at all.
5. **Spot photo from the picker.** Spots → a spot → add a photo → choose one from the gallery. Fail: the picker does not
   open, the app closes on return, or the photo is not on the spot.
6. **Spot photo from the camera.** Same, take a new photo. Fail: the camera does not open, the app closes after the shot,
   or no photo.
7. **Location.** The locate button on the map, and "At my location" when saving a spot. Fail: the permission prompt
   never shows, the app closes, or the dot never appears outdoors.
8. **The rule book.** On a water's page tap "Rule book p.N ↗": the book opens at that page, highlighted; swipe pages,
   pinch to zoom, back closes it. (The app shows no table of contents; the viewer builds it on every open, so a failure there
   would show as this open failing.) Fail: a blank viewer, an error line instead of the page, or the app closes.
9. **Leave it in the background a while, come back** (memory trim): open other apps for a few minutes, come back to the
   map. Fail: the app restarted from scratch with an error, or the map stays blank.
10. **Saved spot after a restart.** Your spots and settings are still there after a force stop. Fail: anything missing.

**Plan as changed (coordinator, 2026-11-15 10:40; supersedes the "Sequence" above where they differ).** Branch
`perf/r8-play` = `redesign/stream-sections` at d29dffccc + `perf/r8` (merge) + version 0.7.11 (app.json, package.json,
changelog draft entry, `app_reads.json` row = 0.7.10's reads). This lane: the test-bed APK of that commit with the flag and
the six benches. The coordinator merges `perf/r8-play` into the shared branch; the Android chat then runs
`CANIFISHTHIS_R8=1 deploy/staging/build-apk.sh --play` in the main checkout (it bumps and commits versionCode itself).
How the flag reaches the build: it is in the environment of build-apk.sh → kept by its `exec scripts/heavy.sh … "$0" "$@"`
(heavy.sh runs the command with the same environment, no `env -i`) → the inner run's `npx expo prebuild` in the clean
`git worktree` (`cd "$WT"` changes the directory, not the environment) → app.config.js loads `plugins/with-no-minify.js`,
which writes `android.enableMinifyInReleaseBuilds=true` into gradle.properties and the keep rules into proguard-rules.pro;
Gradle reads those files, so it needs no variable of its own. The same path made the trial's minified test-bed APK.
Flag off: build-apk.sh prints and does exactly what it did (the R8 lines are all behind `== 1`). "R8 on" is said in the
precheck, the dry run and one line of the owner's email ("Code shrinker (R8): on (T749)"); play.json is left as it is (the
app reads it; no field added).

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

### State 2026-11-15 12:49: five of six pass; bg-install unproven (the internet line); not merged, not built
Flag-ON test-bed APK of `perf/r8-play` 164c928c2 (app CI 43742632555 and pipeline CI 43742632584 green), `--no-r2`,
load 14.9 at the start, 5.4 at the end: install PASS 4/4 (map up 353 s), crash-bed PASS 5/5, regs-open PASS 26/26 (cue
33–89 ms, page 333–370 ms), tap-stalls PASS 5/5 (worst 138 ms), memory PASS 14/14 (boot PSS 255.8 MB, rule book 8659 /
8655 ms). APK 54,973,373 B, dex 7,892,984 B.
bg-install: run 1 failed step 5 (no download notification with the app closed); runs 2 and 3 passed step 5 and failed
step 6 (setup not finished after the 15-minute wait; at 12:11 the app was in front at 468.2 of 479.3 MB). CONTROL,
flag-OFF APK of the same commit, same session: the same failure at step 6. The line from the host at 12:46–12:49: the
25 KB staging manifest 6.7–19.8 s, www.cloudflare.com not resolved in 8 s, an 8 MB range of the staging basemap 0 bytes
in 20 s. So it is not R8; on this line the shared head cannot be judged either. The reopen also restarts the big files
from zero: T756 (old, T298's rule; fix on `fix/t756-keep-running-job`, not in this build).
Next: re-run bg-install (flag on, and flag off beside it) when the line is fast again (a background probe watches for
three 8 MB samples in a row above 1.5 MB/s). Pass -> merge `perf/r8-play` (onto the then head; flag default off) ->
both CIs green -> the Android chat runs the Play build. The full lane report, the script diff, how the flag reaches
Gradle and the owner's ten-step phone walk are in this ticket on the branch.

### State 2026-11-15 14:50: build planned for 2026-11-16; branch now carries the shared head 9199345a3
The line stayed down all afternoon (30–46 B/s, 13:04–14:39). The Android chat landed its desktop split (T752, d3c7376a0)
on the shared branch, so `perf/r8-play` now has that head merged in. App code since the 164c928c2 bench: T752 (five
files under `app/packages/ui-native`) and one worker test fixture. ALL SIX benches are re-run on this merge with the
flag on before the build; the 164c928c2 results above stand only for that commit.

### CORRECTION and result, 2026-11-15 (written 17:24)
- **The line was not down.** The "30–46 B/s" above was my probe timing a 9-byte 404 (its address lacked `release/`).
  Measured properly at 15:16: 8 MB in 4.7 s. The failures up to 12:49 were real, but their cause was very likely not
  the line either: see the next point.
- **Why bg-install failed in the morning, and the 15:17 run at `install`:** the system downloader on the emulator held
  half-finished atlas and basemap rows from runs cut short by their time cap; `pm clear` keeps them (same uid), the
  cleared app adopts them and waits for ever ("paused (reason 1), 18436761 of 197196706 bytes", no job scheduled).
  Not R8. Ticket T760 (the app must not wait silently; the bench must clear the downloader).
- **Clean run, flag-ON test-bed APK `app-testbed-stub-r8play2.apk` of 6a2dc612d (= 164c928c2 + the shared head with
  T752), after an uninstall + install, `--no-r2`, 15:54–16:43, lane 2: 6 of 6 scenarios, 61 steps pass, 0 fail.**
  install (map up 481 s; peak total PSS 125 MB during the download), crash-bed 5/5, regs-open (cue 36–71 ms, page
  344–360 ms), bg-install (every step, with the app closed mid-download and the reopen), memory, tap-stalls. Report:
  the R8 lane's worktree, `app/tools/testbed/out/runs/2026-11-15T22-54-45/index.html`.
- **Merged 16:50:** `redesign/stream-sections` 35dce9bf3 (merge of `perf/r8-play`; `app/` and
  `deploy/staging/build-apk.sh` byte-identical to 6a2dc612d). App CI 43776985369 and pipeline CI 43776985305 green
  (GitHub's app run on 6a2dc612d itself had failed once on the flaky T751 test).
- **Play build: not made.** `CANIFISHTHIS_R8=1 deploy/staging/build-apk.sh --play` (Android chat, 17:20) passed its
  precheck and stopped in `scripts/fast-fail.sh`: `test_dfo_suggest.py::test_the_hunt_file_covers_its_targets_and_
  names_real_cuts` reads the owner's uncommitted `data/generated/review/dfo-suggestions.json` in the main checkout,
  where two rows lack their evidence. No skip (`--no-fast-fail` is forbidden); regenerating the rows was refused by the
  automatic permission check, so it is the owner's (his To-Do row "regenerate the two proposal rows"). Build 33 follows.

## Benches for the 0.7.11 Play build head, 2026-11-15 23:19 to 2026-11-16 00:38 (app chat, lane 2, R8 on)
- APK built 23:21 from 5aac6932e (last app code change 4e793d61f, T762) with `CANIFISHTHIS_R8=1`; the build wrote
  R8's `mapping.txt` (65 MB), `usage.txt`, `seeds.txt`; one 7.9 MB `classes.dex`. `--crash-stub`, `run.mjs --no-r2`.
- Run 1 (23:19 to 00:27): crash-bed 5/5, regs-open 26/26 (worst tap: cue 87 ms, page 414 ms), bg-install 7/7 (the
  notification tap did not open the app, the bench launched it: the known old-bench behaviour), memory 14/14 (boot
  249.8 MB, peak 439.5 MB with the rule book), tap-stalls 5/5 (worst 169 ms of 300). install FAILED at 23:39, 0.6 of
  479.3 MB: the Mac's disk was full from about 23:38 (T764), so not an app verdict; no probe verdict exists for it.
- Run 2 (00:28 to 00:38, same APK): install 3/3 (up in 389 s), crash-bed 5/5.
- All six pass. They stand for the Play build head if `git diff --stat 5aac6932e <head> -- app
  deploy/staging/build-apk.sh` shows only the release's fixture re-cuts, test files and T764's files.
- Not verified: no minified stack frame was decoded in this round.


## The Play internal build is MADE: 0.7.11, versionCode 33, 2026-11-16 04:21 to 04:37 (app chat, lane 3, R8 on)
- Command: `HEAVY_LOCK=$HOME/.claude/heavy-job-3.lock CANIFISHTHIS_R8=1 deploy/staging/build-apk.sh --play`, no escape
  flag. Built from 0c9988033 (the shared branch after staging release r-46e4355a7277; last app code change 4e793d61f,
  the same as the benched APK's). Precheck ok ("R8 ON"), CI ok (both newest finished runs green), fast-fail ok.
- Result: `app/dist/aab/canifishthis-0.7.11-33-0c9988033.aab`, 35.0 MB, signed by the upload key; Gradle ran
  `:app:minifyReleaseWithR8`; uploaded to Play track `internal` as completed; `play.json` on staging says version
  0.7.11, build 33; the owner's email was sent (HTTP 200). The bump commit is 88681f9f0 (app.json only).
- R8's mapping file is KEPT: `data/sourcemaps/0.7.11-b33-0c99880/android/r8-mapping.txt` (62.1 MB), beside the Hermes
  map (13.4 MB). A crash from this build needs that file to be read.
- One stop on the way, not the app's: the first launch (04:16) stopped in fast-fail on the docs guard, which read an
  uncommitted owner-list edit of the main chat's citing a file by a short path. Fixed by the main chat; relaunched 04:21.
- Bench-diff before the build: `git diff --name-only 5aac6932e d0d584159 -- app deploy/staging/build-apk.sh` outside the
  re-cut fixtures and test files = `releaseDir.ts` (node-only test driver), `section-lines.json` (read only by that
  driver, two tools and a test), `app/tools/regs-recut/taps.py`, `app/tools/test-tmp.ts`, the two vitest configs. So
  the six benches of the section above stand for this build.
- Left: the owner's ten-step phone walk (the section "Phone walk for the owner"; To-Do row `t749-r8-phone-walk`).
  Not verified by anyone yet: the build on a real phone.
