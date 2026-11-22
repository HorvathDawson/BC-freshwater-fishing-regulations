# T1003 — Android: R8 on for every release build, production included (Play: "Obfuscation 1%")

Status: OPEN, for the next release (0.9.0; owner 2026-11-22: it waits for the next chat's work).

Source: owner 2026-11-22, pasting the Play Console notice verbatim: "DEX code optimization is below our threshold.
Improve your percentages in the following categories: Obfuscation (1%). Percentages under 25% in any category of your
app may impact your visibility and publishing capabilities on Google Play. To prevent this, optimize your app by the
deadline." The deadline date was not in the paste (read it in Play Console → the notice, and write it here).

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
land after it or merge carefully). Owner: no decision needed; the deadline date from Play Console, and the usual phone
check on the release build.

Coupling (T978 review, 2026-11-22): `app/tools/r8-config.test.ts` also holds the pinned-MapLibre tests. Today the R8
refusal throws before the mods run; once production allows R8, every test that builds a production config through expo
must give `MLN_LOCAL_MAVEN` a pinned stand-in (`repoOf(pin.MLN_VERSION)` in that file), or the pinned plugin refuses the
config instead of R8 being tested.

## Related
T1009 (owner 2026-11-22, Play card "DEX code optimization: Low"): the other two categories Play scores (optimization,
shrinking), the per-build measurement and the floors; it starts once this ticket has R8 on.
