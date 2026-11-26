# T1027 — Android R8: push the optimization / shrinking / obfuscation scores past ~72% (future)

Status: IN PROGRESS — lane L2, branch perf/T1027-r8-scores (c933e56a1, 3f8764802, fbe207d98): scores 72.0/72.3/72.5 →
76.0/76.4/76.5, floors 75, walked ("Built" below); left (Claude): the independent review and merge; Play's figures
after the next Play internal build. Rule 1b (WorkManager) is a defect of every R8 build since T1003, 0.9.0 included.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1027 | Android R8: push the optimization / shrinking / obfuscation scores past ~72% (restate library keep rules, each walked) | IN PROGRESS — 76.0/76.4/76.5, floors 75, walked; WorkManager InputMerger fix (every R8 build since T1003); left (Claude): independent review + merge | T1003, T1009 | owner 2026-11-22 |

Source: owner 2026-11-22 23:45 (dialog on T1009): "Do 1 but make a ticket to push higher in future" — 72% accepted for
0.9.0 (T1003/T1009), this ticket to go higher later.

## Work
Per library with a broad keep rule: drop its rule file (AGP `keepRules.ignoreFrom`) and restate only what reflection
needs; each restated library walked by an emulator bench with R8 on; raise the floors in `app/tools/r8-report.mjs`.

Done (lane L2, 2026-11-24, branch perf/T1027-r8-scores): c933e56a1 (the skips + restated rules: `plugins/with-r8.js`
`IGNORED_RULES`, `r8-keep-rules.pro` rules 8-10), 9540bd6db + 4d4f9aa87 (the `notify-fire` bench), 3f8764802 (rule 1b),
fbe207d98 (floors 70 → 75/75/75, pinned by a test). Scores (R8's own `r8.json`, arm64 release AAB): 72.03 / 72.32 /
72.46 → **76.04 / 76.35 / 76.51** with expo-notifications, pdfiumandroid, expo-task-manager + expo-background-task
skipped. Review fixes 7e5366c04 (`r8-report.mjs check` fails by name when a skipped library's package-wide rule is
back), 19bfd447d (notify-fire puts the clock back on SIGINT / SIGTERM too). Walks (R8 on, AVD `n1_pixel_b`, reports
`app/tools/testbed/out/runs/t1027*`): `install`, `saved-spots`, `regs-open`, `memory` (pdfium), `mode-switch`, `spots`
all green; **`notify-fire` 5/5** on fbe207d98's APK.

## Left
- The independent review and merge; Play's figures after the next Play internal build.
- **Rule 1b, every R8 build since T1003, 0.9.0 included:** WorkManager's own rule keeps
  `androidx.work.OverwritingInputMerger`'s name but not its constructor, so every WorkManager run failed ("Could not
  create Input Merger"): the background check (gauge alerts, the update and new-version notices) never ran with R8 on.
  The commit to cherry-pick onto release/0.9.0: 448b31d3d (rule 1b,
  `-keep class * extends androidx.work.InputMerger { <init>(); }`, branch fix/T1027-workmanager-input-merger). Logcat
  without it: "WM-InputMerger: java.lang.NoSuchMethodException: androidx.work.OverwritingInputMerger.<init> []"; with it
  "BackgroundTaskWork: doWork: Running worker" and "Worker result SUCCESS".
- Left as they are (with why): expo-modules-core's `io.github.expo.modules.v2.** { *; }` / `kolibri.**` (199 + 29
  classes) and react-native-svg (54) are project dependencies (AGP's `ignoreFrom` filters only external AARs; a pnpm
  patch of their rule files would be the way, not done); react-android's `com.facebook.react.bridge.** { *; }` (157) is
  reached by name from C++ through fbjni lazily, so no walk can prove a narrowing; MapLibre's `org.maplibre.geojson.**`
  (43) is small.
- Latent gap (rule 8b + AGENTS 34b): ChannelAwareTrigger and TimeIntervalTrigger have no serialVersionUID, so a
  notification SCHEDULED before an update would not load after it; the app schedules none.
- Not green, not from these rules: `offline`'s "rule book opens offline" step (the scenario does not pick the
  Regulations tab as `openWater` does); `tap-stalls`' cold start (one JS stall of 315 / 544 ms in "decode paint", under
  load). Not walked: `bg-install`, `crash-bed`, `report`, the cross-update cases, a Play-installed build.

Depends: T1003, T1009. Size: M–L. Owner: no.
