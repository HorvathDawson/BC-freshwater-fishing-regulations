# T1027 — Android R8: push the optimization / shrinking / obfuscation scores past ~72% (future)

Status: IN PROGRESS — lane L2, branch perf/T1027-r8-scores (c933e56a1, 3f8764802, fbe207d98): scores 72.0/72.3/72.5 →
76.0/76.4/76.5, floors 75, walked ("Built" below); left (Claude): the independent review and merge; Play's figures
after the next Play internal build. Rule 1b (WorkManager) is a defect of every R8 build since T1003, 0.9.0 included.

Source: owner 2026-11-22 23:45 (dialog on T1009): "Do 1 but make a ticket to push higher in future" — 72% accepted for
0.9.0 (T1003/T1009), this ticket to go higher later.

## Work
1. Per library with a broad keep rule (T1009's list: expo-notifications 279 classes, expo-modules-core 199, RN bridge
   157, pdfium 110, …): drop its rule file (AGP `keepRules.ignoreFrom`) and restate only what reflection needs.
2. Each restated library walked by an emulator bench with R8 on (the T1003 walks + the module's own feature).
3. Raise the floors in `app/tools/r8-report.mjs` to the new scores.

Depends: T1003, T1009. Size: M–L. Owner: no.

## Built (lane L2, 2026-11-24; branch perf/T1027-r8-scores)
Code: c933e56a1 (the skips + restated rules), 9540bd6db (the `notify-fire` bench), 3f8764802 (WorkManager's
InputMerger constructors), fbe207d98 (floors 75/75/75, docs), 4d4f9aa87 + the bench's last edit (notify-fire).

**How.** `plugins/with-r8.js` `IGNORED_RULES` writes AGP's `optimization { keepRules { ignoreFrom(...) } }` into the
release build type (refused, never skipped, when the template's `minifyEnabled` line moves); `r8-keep-rules.pro`
rules 8-10 restate what each library reaches by name, each with its reason. AGP's `ignoreFrom` filters only EXTERNAL
dependencies (AARs): `':expo-modules-core'` and `':react-native-svg'` (built from node_modules as Gradle projects)
measured no change, so their broad rules stay (below).

**Scores** (R8's own `r8.json`, what Play reads; arm64 release AAB, staging test-bed env; `r8-report.mjs check`).
"Dropped" = that library's rules ignored with NOTHING restated: an upper bound, measured only, never walked.
| build | optimization | shrinking | obfuscation |
|---|---|---|---|
| main before T1027 (T1009's 72.0 / 72.3 / 72.5) | 72.03 | 72.32 | 72.46 |
| dropped: expo-notifications (279 classes, 2,028 members) | 74.27 | 74.53 | 74.73 |
| dropped: pdfiumandroid (110 / 1,320) | 73.27 | 73.57 | 73.71 |
| dropped: expo-task-manager + expo-background-task (75 / 490) | 72.63 | 72.92 | 73.07 |
| dropped: expo-modules-core, react-native-svg (project deps: ignoreFrom has no effect) | 72.03 | 72.32 | 72.46 |
| **all four skipped, rules 8-10 restated (this branch)** | **76.04** | **76.35** | **76.51** |
Floors raised 70 → 75 in every category (`FLOORS`, pinned by a test); the old 72% AAB now fails the check.

**Left as they are** (with why): expo-modules-core's `io.github.expo.modules.v2.** { *; }` / `kolibri.**` (199 + 29
classes) and react-native-svg (54) are project dependencies (no ignoreFrom; a pnpm patch of their rule files would be
the way, not done); react-android's `com.facebook.react.bridge.** { *; }` (157) is reached by name from C++ through
fbjni lazily, path by path, so no walk can prove a narrowing; MapLibre's `org.maplibre.geojson.**` (43) is small.
The member-conditional rules (`@OnLifecycleEvent`, `native <methods>`) keep only matching members.

**Walks** (R8 on, release test-bed APK, pinned MapLibre, AVD `n1_pixel_b` on emulator-5556, `run.mjs --no-r2`, each
batch a FRESH install; reports under `app/tools/testbed/out/runs/t1027*`):
| batch (build) | scenarios | result |
|---|---|---|
| A (9540bd6db: rules 8-10, before rule 1b) | `install` 4/4, `offline` 14/15, `spots` 14/14, `saved-spots` 13/13, `regs-open` 26/26, `memory` 14/14, `tap-stalls` 4/5, `mode-switch` 6/6, `notify-fire` 2/5 | notify-fire found the WorkManager defect below (pre-existing) |
| C (fbe207d98, clean: rules 8-10 + 1b) | `install` 4/4, `memory` 14/14 (the PDF viewer: "Page 25 of 88", pdfium), `notify-fire` 2/5 (the bench's job parser, fixed in 4d4f9aa87) | |
| D (fbe207d98's APK; only the bench changed since) | `install` 4/4, `saved-spots` 13/13, `tap-stalls` 4/5, **`notify-fire` 5/5** | the check ran twice in the background (WorkManager "Worker result SUCCESS"), raised "Coquitlam River: flow alert … has not reported since", and its tap opened the water's Conditions |
Per library: **pdfiumandroid** by `memory` (A, C: the rule book opened in the in-app viewer at the cited page, drawn, closed,
reopened); **expo-notifications** by `saved-spots` (permission ask, channels, alert saved/listed/removed) and `notify-fire`
(a notice raised through `scheduleNotificationAsync` and its tap read back); **expo-task-manager / expo-background-task**
by `notify-fire` (task registered at the save, run by WorkManager in the background, headless JS task finished). The
other T1003 walks (`install`, `regs-open`, `spots`, `mode-switch`, `offline`) cover start-up and the screens.
Not green, and not from these rules: `offline`'s "rule book opens offline" step (the search opened Cultus Lake on its
Map tab, so the step never reached a synopsis link: the scenario does not pick the Regulations tab as `openWater`
does); `tap-stalls`' cold start (one JS stall of 315 / 544 ms in "decode paint", Hermes work, with three other heavy
lanes on the machine; T1003 saw the same on `n1_pixel_b` under load). Not walked: `bg-install` (`n1_pixel_b` has no
mobile-data path, T1009 note), `crash-bed` (needs the stub APK), `report` (writes to staging), the cross-update cases
the rules keep names for (a notification raised before an app update and tapped after it; a task restored after an
update), and a Play-installed build.

**Found by the walk (in every R8 build since T1003, 0.9.0 included):** WorkManager's own rule keeps
`androidx.work.OverwritingInputMerger`'s name but not its constructor, so every WorkManager run failed ("Could not
create Input Merger"): the background check (gauge alerts, the update and new-version notices) never ran with R8 on.
Rule 1b (`-keep class * extends androidx.work.InputMerger { <init>(); }`) fixes it; the base and T1027 mappings both
show the constructor gone without it.

0.9.0 has the same bug; this commit is the one to cherry-pick onto release/0.9.0: 448b31d3d (rule 1b,
`-keep class * extends androidx.work.InputMerger { <init>(); }`, branch fix/T1027-workmanager-input-merger). Evidence
(lane L2, 2026-11-24, arm64 release AAB/APK built from main 6c03d5b20 + T1027 on emulator n1_pixel_b): R8's mapping
WITHOUT the rule lists `androidx.work.OverwritingInputMerger` with no `<init>` (the same in the main-only build), WITH
it `void <init>():24 -> <init>`; logcat without it, at every forced run of the background check: "WM-InputMerger:
java.lang.NoSuchMethodException: androidx.work.OverwritingInputMerger.<init> []" then "WM-WorkerWrapper: Could not
create Input Merger androidx.work.OverwritingInputMerger"; with it "BackgroundTaskWork: doWork: Running worker" and
"Worker result SUCCESS" (notify-fire 5/5).
