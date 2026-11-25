# T1003 — Android: R8 on for every release build, production included (Play: "Obfuscation 1%")

Status: IN PROGRESS — lane L1, branch perf/T1003-r8-release (c34b990ba, 6f0be8f4d): R8 on for every release build,
the mapping sent to Play, the benches green on this head ("Built" below); left (Claude): the independent review and
merge, the 0.9.0 Play internal build, Play's DEX figures recorded here.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1003 | Android: R8 on for every release build, production included; mapping file uploaded to Play (Play Console: "Obfuscation 1%", under its 25% threshold) | IN PROGRESS — L1 perf/T1003-r8-release: built + benched + docs; left: review/merge, Play internal build + Play's figures | c34b990ba, 6f0be8f4d (after T749) | owner 2026-11-22 (Play Console notice) |

Source: owner 2026-11-22, pasting the Play Console notice verbatim: "DEX code optimization is below our threshold.
Improve your percentages in the following categories: Obfuscation (1%). Percentages under 25% in any category of your
app may impact your visibility and publishing capabilities on Google Play. To prevent this, optimize your app by the
deadline." The deadline date is not needed (owner 2026-11-22: it goes in the next release regardless).

## Why it is 1%
T749 proved R8 (shrink + obfuscate) on the emulator benches and on Play internal build 33 (0.7.11; dex 30.4 MB → 7.9 MB,
APK −7.45 MB). It was closed 2026-11-20 on the owner's word, but the switch stayed a trial: `CANIFISHTHIS_R8=1` is off by
default, and production builds refuse it in two places, `app/apps/mobile/app.config.js` (the T749 refusal) and the
`deploy/staging/build-apk.sh` precheck ("refused for --production"). T749 said this refusal had to be lifted on purpose
later, and it never was. So every production AAB (0.8.x) ships un-minified, un-obfuscated dex, and Play measures that.

## Work
1. **R8 is the default for release builds.** Make `plugins/with-no-minify.js` turn minify on for every release build
   (rename it, e.g. `with-r8.js`) and append `r8-keep-rules.pro` always. Remove `CANIFISHTHIS_R8` and both production
   refusals; an escape for a test-bed comparison build only (refused with `--production`, like `--stock-maplibre`).
   Resource shrinking stays off (r8-keep-rules.pro says why). Update `tools/r8-config.test.ts`; the old "same config as
   with the flag absent" pin becomes "a production config has minify on and the keep rules".
2. **Obfuscation is really on.** No `-dontobfuscate` from us or from a library's consumer rules (read R8's
   `configuration.txt`, `-printconfiguration`, from the build). List the broad `-keep` rules that come from libraries
   (React Native, expo-modules, MapLibre) with how much they keep; Play's percentage is the share of names renamed, so
   it cannot reach 100%. Add a build check that fails when the merged configuration holds `-dontobfuscate`.
3. **The mapping file goes to Play with every upload** (Android vitals + Play's crash reports show readable stacks).
   `deploy/staging/play-upload.py` uploads `r8-mapping.txt` with the Play Developer API
   `edits.deobfuscationfiles.upload` (type `proguard`) for that versionCode, in the same edit as the AAB; a missing
   mapping fails the upload. The local copy under `data/sourcemaps/<key>/android/` stays (T749).
4. **Proof on this build, not on 0.7.11's.** The six emulator benches on the release head (`run.mjs install crash-bed
   bg-install regs-open tap-stalls memory`), since the app changed a lot since build 33 (trout redesign, push, new
   modules, the MapLibre switch). The new native modules since 0.7.11 each get a keep-rule review (what they reach by
   reflection or JNI). Then a Play internal build, and the release goes on as usual.
5. **Read Play's figure.** After the internal upload, record Play's obfuscation percentage (and the other DEX
   optimization categories it lists) in this ticket. Target: every category ≥ 25%, the notice gone.
6. **Docs:** AGENTS.md / the release runbook (T993 plan 11) say R8 is always on and the mapping file goes to Play; the
   memory note on R8 updated.

## Acceptance
- A production prebuild has `android.enableMinifyInReleaseBuilds=true` and the keep rules; `app.config.js` and the
  precheck no longer refuse it (tests pinned, mutation-checked: putting the old refusal back fails a test).
- The merged R8 configuration has no `-dontobfuscate` (the build check fails on a planted one).
- The Play upload sends the mapping file; a missing mapping stops the upload (test).
- Six benches pass on the release head with R8 on; Play's obfuscation figure for the build is ≥ 25% and recorded here.

Risk: medium (a missing keep rule crashes only on the path that needs it; T749's benches and build 33 lower it, the
modules added since raise it). Size: M. Depends: T749 (done), T978 (the MapLibre switch, also edits `app.config.js`:
land after it or merge carefully). Owner: no decision needed;  the usual phone
check on the release build.

Coupling (T978 review, 2026-11-22): `app/tools/r8-config.test.ts` also holds the pinned-MapLibre tests. Today the R8
refusal throws before the mods run; once production allows R8, every test that builds a production config through expo
must give `MLN_LOCAL_MAVEN` a pinned stand-in (`repoOf(pin.MLN_VERSION)` in that file), or the pinned plugin refuses the
config instead of R8 being tested.

## Related
T1009 (owner 2026-11-22, Play card "DEX code optimization: Low"): the other two categories Play scores (optimization,
shrinking), the per-build measurement and the floors; it starts once this ticket has R8 on.

## Built (lane L1, 2026-11-22; branch perf/T1003-r8-release)
Code: c34b990ba (this ticket) + 6f0be8f4d (T1009's `r8-report.mjs`, which the build runs).
1. **R8 is the default.** `plugins/with-no-minify.js` → `plugins/with-r8.js`: `android.enableMinifyInReleaseBuilds=true`
   and `r8-keep-rules.pro` appended for every release build. `CANIFISHTHIS_R8` is gone and REFUSED at any value (an old
   `=0` would otherwise read as "off" and build with R8 on). The escape is `CANIFISHTHIS_NO_R8="<reason>"`, set by the
   test bed's `session.sh --no-r8="<reason>"` (own APK name `-nor8`); `app.config.js` refuses it in production and
   `build-apk.sh`'s precheck refuses it for every build it makes (as `--stock-maplibre`). Both T749 production refusals
   are removed. `tools/r8-config.test.ts` pins it through the real `expo config --type introspect` (production config
   made with minify on, the keep-rules mod registered, the escape, the refusals); mutation-checked: the old
   `app.config.js` refusal put back fails 4 tests, the old precheck refusal fails 1. Production configs in that file
   get the pinned-MapLibre stand-in (`repoOf(pin.MLN_VERSION)`, now shared at the top).
2. **Obfuscation is really on.** R8's merged `configuration.txt` of this head has no `-dontobfuscate` / `-dontoptimize`
   / `-dontshrink` from any source; r8.json says `isObfuscationEnabled: true` (and optimization, shrinking). The build
   check (`r8-report.mjs check`, run by `build-apk.sh` for the APK and the AAB, before anything is copied, committed or
   uploaded) fails on a planted one, its source named (test). Broad library rules and how much they keep: T1009.
3. **The mapping goes to Play.** `play-upload.py --mapping <r8-mapping.txt>` (required with `--aab`) uploads it with
   `edits.deobfuscationfiles.upload` (type `proguard`, the AAB's versionCode) in the same edit, after the bundle and
   before the track; a missing or empty mapping stops before any Play call, a refused one before the track/commit
   (`pipeline/tests/test_play_upload.py`, against a stand-in Play session). `build-apk.sh --play` passes the copy it
   keeps under `data/sourcemaps/<key>/android/r8-mapping.txt`. The AAB also carries it itself
   (`BUNDLE-METADATA/com.android.tools.build.obfuscation/proguard.map`, AGP; the check requires it).
4. **Proof on this head** (R8 on, release, pinned MapLibre 13.7.1-main.3a6b955, fresh install each batch; the first batch
   built from the uncommitted tree that became c34b990ba, the others from c34b990ba clean):
   - `install` 4/4, `report` 6/6 (More → Report a problem, offline queue, flushed on reconnect), `regs-open` 26/26
     (search → the map framed → a tap on the water → its regulations page, every case), `memory` 14/14 (boot "Code" 46.1 MB;
     a water's page, satellite), `mode-switch` (Trout → Salmon → Conditions → Runs → Map → Trout, all painted): emulator
     `n1_pixel_b`.
   - `spots` 14/14 (a photo from the system picker: expo-image-manipulator, the one native module added since 0.7.11),
     `bg-install` 8/9 (mobile-data ask, wait for Wi-Fi, the system download with the app closed, notification tap
     reopens; the one FAIL is T738's bar-vs-bytes check, open in T738), `tap-stalls` 5/5 (worst 276 ms): emulator
     `n1_pixel`. (On `n1_pixel_b` bg-install could not reach mobile data / the bench proxy and tap-stalls saw a 1.2 s
     stall with three other heavy jobs on the machine; on `n1_pixel` both passed.)
   - keep-rule review of what changed since build 33: the app's own modules (bg-download, exit-info, memory-trim) have
     no Android change since 0.7.11; expo-image-manipulator is the only new native dependency (spots bench above).
     The mapping proves the reflective names hold on the pinned MapLibre: `MapView.onLowMemory`,
     `MapLibreSurfaceView.detachedListener` / `renderThread`, `WorkDatabase_Impl.<init>()`, `MemoryTrimModule`,
     `HasChildPressedStateDelay` keep their names.
   - `crash-bed` 5/5 on the stub APK (`session.sh --crash-stub`, R8 on, `n1_pixel`): an unhandled promise, an uncaught
     JS error and a native SIGSEGV each reach the worker handler (the exit-info module reads Android's exit record).
   - **Device checks still owed with R8 on** (independent review 2026-11-22; they go into the 0.9.0 test pass, not this
     lane): expo-location's locate (`locate` bench / "At my location" with a real fix); the expo-background-task /
     expo-task-manager task actually running in the background (`bg-update`); an expo-notifications local alert firing;
     the in-app update prompt and Play Core on the Play Internal build (only a Play-installed build can show it).
   - **Reading the mapping back:** `sourcemaps.mjs symbolicate` does not read `r8-mapping.txt`, and that is correct: our
     crash store holds no Java/Kotlin frames (JS stacks are Hermes's; exit-info sends Android's exit reason, never a
     trace). Play reads Java crashes and ANRs back with the uploaded mapping; a Java stack from Play Console or logcat
     is read by hand with `$ANDROID_HOME/cmdline-tools/latest/bin/retrace <r8-mapping.txt> <stack.txt>` (the
     `keep-r8` comment in `sourcemaps.mjs` says so now; it claimed the kept copy read our own crashes' frames back).
5. **Play's figure:** not read yet: no Play upload was made from this lane (left: the Play internal build of the 0.9.0
   release, then Play's "DEX code optimization" figures into this ticket and T1009). R8's own figures for this head,
   which Play reads from the AAB: obfuscation 72.5%, optimization 72.0%, shrinking 72.3% (was obfuscation 1%).
6. **Docs:** AGENTS.md rule 34b, `runbooks/release-by-hand.md` step 6 (R8 always on, the scores, the mapping to Play),
   `app/tools/testbed/README.md` (`--no-r8`, and own AVD + port per parallel emulator lane), the iOS restart page's
   commands without `CANIFISHTHIS_R8`, `parity-audit.md`'s plugin name. Closed tickets that name the old flag are left as
   the record of what ran. The memory note on R8 is the main chat's.

State 2026-11-22 (stopped for credits): done: R8 on for every release build, mapping to Play, r8-report scores + floors, benches, docs (step 6), review fixes; left: independent re-review + merge of PR #44, the 0.9.0 Play internal build and Play's DEX figures, the owed R8 device checks (T1003); branch perf/T1003-r8-release, head d64401b74 + this line (last fast-fail run whole on 6fd805936; the final one stopped for credits).
