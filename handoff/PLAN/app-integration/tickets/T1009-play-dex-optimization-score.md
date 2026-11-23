# T1009 — Android: lift Play's "DEX code optimization" score from Low; measure all three categories every build

Status: IN PROGRESS — lane L1, branch perf/T1003-r8-release (6f0be8f4d, c34b990ba): the build prints R8's three
scores and fails on a -dont* pass or under the floors (70/70/70; measured 72.0 / 72.3 / 72.5); no library rule narrowed
("Measured" below); left (Claude): the independent review and merge, Play's figures and the card's word after the
0.9.0 Play internal build.

Source: owner 2026-11-22, pasting the Play Console card verbatim: "DEX code optimization — Low — Optimize your scores to
improve your memory usage and overall performance for your users. Learn more", with "we want to make this better?
new ticket".

## How this differs from T1003
T1003 answers Play's notice about **one** category ("Obfuscation (1%)"): it turns R8 on for every release build,
production included, and uploads the mapping file. That alone takes the score off the floor. This ticket is the rest:
Play scores the bundle in **three** categories, **optimization, shrinking and obfuscation** (developer.android.com,
"DEX code optimization", Android vitals), and the card says "Low" overall. The goal here is a score that is good, not
just over the alert line, and a build that tells us the three numbers before Play does.

## Why it is Low today
Every production AAB since 0.8.x ships with R8 off (`android.enableMinifyInReleaseBuilds=false`;
`plugins/with-no-minify.js`, T250 / T749). With R8 off nothing is optimized, shrunk or renamed. T749 measured R8 on
build 33: dex 11.0 MB → 3.6 MB stored in the APK, boot "Code" memory 70.6 → 38–52 MB. That last figure is what the card
means by "memory usage".

## Work
1. **Measure locally, per build.** Run Google's R8 Configuration Analyzer (or the `unzip` + mapping measurement the
   Android vitals page gives) on the release AAB and print the three percentages. Add them to the build output of
   `build-apk.sh --play` and to the owner's release email, next to "Code shrinker (R8): on".
2. **Find what keeps the score down.** From R8's merged configuration (`-printconfiguration`) and `-whyareyoukeeping`,
   list the broad library keep rules (React Native, expo-modules-core, MapLibre, pdfium `io.legere.pdfiumandroid.** { *; }`,
   Gson) and how much each one keeps. For each: narrow it where the library allows (our own `r8-keep-rules.pro` is
   already narrow, each rule with its reason), or record why it stays. Never drop a rule the T749 benches proved
   needed (Room `<init>()` is the T250 crash).
3. **Optimization category.** Confirm `proguard-android-optimize.txt` stays the default file (it is today), no
   `-dontoptimize` / `-dontshrink` / `-dontobfuscate` from us or a library's consumer rules, and R8 full mode (AGP 8
   default). One build check fails when the merged configuration holds any of the three.
4. **Floor in CI-side tooling.** After the first measured build, set a floor per category (a little under the
   measured value) in the build check, so a library update that adds a `{ *; }` rule shows up as a failed build, not
   as a Play notice.
5. **Proof:** the same emulator benches as T1003 step 4 on the build with any narrowed rule (`run.mjs install crash-bed
   bg-install regs-open tap-stalls memory`). A narrowed keep rule crashes only on the path that needs it, so a rule
   is narrowed only when a bench walks that path.
6. **Record Play's figures** after the Play internal upload: the three percentages and the card's word (Low / Medium /
   High) in this ticket, before and after.

## Acceptance
- The build prints the three percentages; a check fails on `-dontoptimize`/`-dontshrink`/`-dontobfuscate` and under
  the floors.
- Play's card no longer says Low; its three figures are in this ticket.
- Every bench passes on the build with narrowed rules.

Out of scope: resource shrinking (stays off, `r8-keep-rules.pro` says why), Hermes JS bytecode (Play scores DEX only).

Risk: medium (narrowed keep rules; each one bench-proved). Size: M. Depends on: T1003. Owner: none until Play's figures.

## Measured (lane L1, 2026-11-22; branch perf/T1003-r8-release)
The build measures itself now: `app/tools/r8-report.mjs check` (6f0be8f4d, wired into `build-apk.sh` in c34b990ba) reads
`BUNDLE-METADATA/com.android.tools/r8.json` from the AAB, the file Play reads since AGP 8.10 (we are on AGP 9.2.1, R8
9.2.14). R8 writes the share it could NOT process (`stats.noOptimizationPercentage` …); the score is 100 minus that.
It also fails on `-dontoptimize` / `-dontshrink` / `-dontobfuscate` anywhere in R8's merged `configuration.txt` (source
file named), on an AAB with no `proguard.map` or no `r8.json`, and under the floors; it prints one line that the build
output and the owner's Play email carry ("Code shrinker (R8): on; optimization …").

| build (arm64, staging env, debug-signed unless said) | AAB | APK | DEX (uncompressed) | optimization | shrinking | obfuscation |
|---|---|---|---|---|---|---|
| 0.8.5 production AAB (versionCode 43, R8 off, upload-signed) | 41.49 MB | — | 30.40 MB (5 dex files) | — | — | Play: 1% |
| this head, R8 OFF (`CANIFISHTHIS_NO_R8`, the baseline) | 41.56 MB | 65.50 MB | 30.41 MB (5 dex files) | — | — | — |
| this head, R8 ON | **39.46 MB** (−2.10) | **58.06 MB** (−7.44) | **7.92 MB** (1 dex file, −74%) | **72.0%** | **72.3%** | **72.5%** |
(MB = 10^6 bytes; the APK is the test-bed build, the same code with two test-only manifest lines; T749 measured −7.45 MB
on build 33.)

Classes renamed in the mapping: 6,909 of 8,661 (79.8%). Floors set at 70 / 70 / 70 (`FLOORS` in r8-report.mjs; Play's
alert line is 25). Boot "Code" memory on the emulator with R8 on: 46.1 MB (the `memory` bench; T749 measured 70.6 MB
with R8 off). Optimization pass: AGP's default `proguard-android-optimize.txt`, R8 full mode (`isProGuardCompatibility
ModeEnabled: false` in r8.json), `-allowaccessmodification` and repackaging on; no `-dont*` pass from any source.

### What keeps the other ~28% (`r8-report.mjs rules`: classes / members each broad rule matches among R8's seeds)
| rule (source) | classes | members | what it is |
|---|---|---|---|
| `-keep class expo.modules.notifications.** {*;}` (expo-notifications 58.1.6) | 279 | 2,028 | the notifications module (local notifications on Android) |
| `-keep class io.github.expo.modules.v2.** { *; }` (expo-modules-core) | 199 | 1,700 | the Expo module runtime |
| `-keep,includedescriptorclasses class com.facebook.react.bridge.** { *; }` (react-android 0.88) | 157 | 1,262 | the React Native bridge |
| `-keep class io.legere.pdfiumandroid.** { *; }` + 2 more (pdfiumandroid 1.0.32) | 110 | 1,320 | the PDF reader (react-native-pdf) |
| `-keep public class com.horcrux.svg.** {*;}` (react-native-svg) | 54 | 1,524 | the SVG views |
| `-keep class org.maplibre.geojson.** { *; }` (MapLibre pinned main) | 43 | 421 | MapLibre's GeoJSON types |
| `-keep class expo.modules.taskmanager.** {*;}` / `backgroundtask.**` | 41 + 34 | 303 + 187 | background tasks (WorkManager) |
| `-keep class io.github.expo.kolibri.** { *; }`, `com.squareup.zstd.**` (expo-modules-core, expo) | 29 + 14 | 223 + 69 | Expo's native glue, zstd |
| member-conditional (`com.facebook.** { native <methods>; }`, `** { @OnLifecycleEvent *; }`, MapLibre enums) | upper bound | | keep only the matching members |
Our own `r8-keep-rules.pro` is already narrow (7 rules, each with its reason); the template's
`-keep class com.facebook.react.turbomodule.** { *; }` matches 7 classes. Every broad rule above is a LIBRARY's consumer
rule: none offers a narrower switch. Narrowing one means dropping that library's whole rule file
(AGP `optimization { keepRules { ignoreFrom(...) } }`) and re-stating the rest by hand, each re-stated rule walked by a
bench; at 72% in every category (Play's line 25) that was not done here (owner question in the report).

Bench note: parallel emulator lanes need their own AVD and console port (`TESTBED_AVD=n1_pixel_b TESTBED_PORT=5584`):
this lane's first batch shared `emulator-5580` with another lane's session, which reinstalled its APK mid-run
(`app/tools/testbed/README.md` says so now).

State 2026-11-22 (stopped for credits): done: R8 on for every release build, mapping to Play, r8-report scores + floors, benches, docs (step 6), review fixes; left: independent re-review + merge of PR #44, the 0.9.0 Play internal build and Play's DEX figures, the owed R8 device checks (T1003); branch perf/T1003-r8-release, head d64401b74 + this line (last fast-fail run whole on 6fd805936; the final one stopped for credits).
