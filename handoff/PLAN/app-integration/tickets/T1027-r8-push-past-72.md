# T1027 — Android R8: push the optimization / shrinking / obfuscation scores past ~72% (future)

Status: OPEN — after 0.9.0.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1027 | Android R8: push the optimization / shrinking / obfuscation scores past ~72% (restate library keep rules, each walked) | OPEN — after 0.9.0 | T1003, T1009 | owner 2026-11-22 |

Source: owner 2026-11-22 23:45 (dialog on T1009): "Do 1 but make a ticket to push higher in future" — 72% accepted for
0.9.0 (T1003/T1009), this ticket to go higher later.

## Work
1. Per library with a broad keep rule (T1009's list: expo-notifications 279 classes, expo-modules-core 199, RN bridge
   157, pdfium 110, …): drop its rule file (AGP `keepRules.ignoreFrom`) and restate only what reflection needs.
2. Each restated library walked by an emulator bench with R8 on (the T1003 walks + the module's own feature).
3. Raise the floors in `app/tools/r8-report.mjs` to the new scores.

Depends: T1003, T1009. Size: M–L. Owner: no.

0.9.0 has the same bug; this commit is the one to cherry-pick onto release/0.9.0: 448b31d3d (rule 1b,
`-keep class * extends androidx.work.InputMerger { <init>(); }`, branch fix/T1027-workmanager-input-merger). Evidence
(lane L2, 2026-11-24, arm64 release AAB/APK built from main 6c03d5b20 + T1027 on emulator n1_pixel_b): R8's mapping
WITHOUT the rule lists `androidx.work.OverwritingInputMerger` with no `<init>` (the same in the main-only build), WITH
it `void <init>():24 -> <init>`; logcat without it, at every forced run of the background check: "WM-InputMerger:
java.lang.NoSuchMethodException: androidx.work.OverwritingInputMerger.<init> []" then "WM-WorkerWrapper: Could not
create Input Merger androidx.work.OverwritingInputMerger"; with it "BackgroundTaskWork: doWork: Running worker" and
"Worker result SUCCESS" (notify-fire 5/5).
