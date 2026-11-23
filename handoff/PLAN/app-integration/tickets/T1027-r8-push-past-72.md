# T1027 — Android R8: push the optimization / shrinking / obfuscation scores past ~72% (future)

Status: OPEN — after 0.9.0.

Source: owner 2026-11-22 23:45 (dialog on T1009): "Do 1 but make a ticket to push higher in future" — 72% accepted for
0.9.0 (T1003/T1009), this ticket to go higher later.

## Work
1. Per library with a broad keep rule (T1009's list: expo-notifications 279 classes, expo-modules-core 199, RN bridge
   157, pdfium 110, …): drop its rule file (AGP `keepRules.ignoreFrom`) and restate only what reflection needs.
2. Each restated library walked by an emulator bench with R8 on (the T1003 walks + the module's own feature).
3. Raise the floors in `app/tools/r8-report.mjs` to the new scores.

Depends: T1003, T1009. Size: M–L. Owner: no.
