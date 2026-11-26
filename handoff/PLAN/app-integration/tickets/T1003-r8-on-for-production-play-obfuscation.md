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

## What
Why it was 1%: T749's `CANIFISHTHIS_R8=1` stayed a trial, refused for production in `app/apps/mobile/app.config.js` and the
`deploy/staging/build-apk.sh` precheck, so every production AAB (0.8.x) shipped un-minified, un-obfuscated dex.

Done (lane L1, c34b990ba + 6f0be8f4d, T1009's `r8-report.mjs`):
1. R8 the default: `plugins/with-r8.js` (`android.enableMinifyInReleaseBuilds=true` + `r8-keep-rules.pro`);
   `CANIFISHTHIS_R8` REFUSED at any value; escape `CANIFISHTHIS_NO_R8="<reason>"` (`session.sh --no-r8`, APK `-nor8`),
   refused in production and by `build-apk.sh`; pinned in `tools/r8-config.test.ts` (mutation-checked). Production configs
   there get the pinned-MapLibre stand-in (`repoOf(pin.MLN_VERSION)`).
2. No `-dontobfuscate` / `-dontoptimize` / `-dontshrink` in the merged `configuration.txt`; `r8-report.mjs check` (run by
   `build-apk.sh` for APK and AAB) fails on a planted one.
3. `play-upload.py --mapping <r8-mapping.txt>` (required with `--aab`) uploads via `edits.deobfuscationfiles.upload` (type
   `proguard`) in the same edit; missing or empty stops before any Play call (`pipeline/tests/test_play_upload.py`). The AAB
   also carries it (`BUNDLE-METADATA/com.android.tools.build.obfuscation/proguard.map`).
4. Benches on this head (R8 on, pinned MapLibre 13.7.1-main.3a6b955): install, report, regs-open, memory, mode-switch,
   spots, tap-stalls, crash-bed all pass; `bg-install` 8/9 (the FAIL is T738's bar-vs-bytes check, open in T738).
   Keep-rule review: expo-image-manipulator is the only new native dependency; reflective names hold on the pinned MapLibre.
   Reading a Java stack back: `$ANDROID_HOME/cmdline-tools/latest/bin/retrace <r8-mapping.txt> <stack.txt>` (our crash
   store holds no Java frames).
5. R8's own figures for this head, which Play reads from the AAB: obfuscation 72.5%, optimization 72.0%, shrinking 72.3%
   (was obfuscation 1%).
6. Docs: AGENTS.md rule 34b, `runbooks/release-by-hand.md` step 6, `app/tools/testbed/README.md`. The memory note on R8 is
   the main chat's.

## Work (left)
1. Independent re-review + merge of PR #44.
2. The 0.9.0 Play internal build; then record Play's "DEX code optimization" figures here and in T1009 (target: every
   category ≥ 25%, the notice gone).
3. **Device checks still owed with R8 on** (independent review 2026-11-22; they go into the 0.9.0 test pass): expo-location's
   locate (`locate` bench / "At my location" with a real fix); the expo-background-task / expo-task-manager task actually
   running in the background (`bg-update`); an expo-notifications local alert firing; the in-app update prompt and Play
   Core on the Play Internal build (only a Play-installed build can show it).

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

Related: T1009 (owner 2026-11-22, Play card "DEX code optimization: Low"): the other two categories Play scores
(optimization, shrinking), the per-build measurement and the floors.

State 2026-11-22 (stopped for credits): done: R8 on for every release build, mapping to Play, r8-report scores + floors, benches, docs (step 6), review fixes; left: independent re-review + merge of PR #44, the 0.9.0 Play internal build and Play's DEX figures, the owed R8 device checks (T1003); branch perf/T1003-r8-release, head d64401b74 + this line (last fast-fail run whole on 6fd805936; the final one stopped for credits).
