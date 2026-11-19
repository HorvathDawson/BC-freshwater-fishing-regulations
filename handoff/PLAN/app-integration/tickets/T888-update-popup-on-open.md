# T888 — A newer app version: one popup on opening, with the swimming coho and what's new, then the bell

Status: IN PROGRESS (app chat, lane `app/t888-update-popup`; built, lands after the owner sees the pictures).

Owner 2026-11-18 ~23:25, verbatim: "When a user opens the app and a new app version is available it should give a popup
with the swimming fish and the new update list. That they need to dismiss (then it goes to Bell) for both apple and
android ."

To build:
- On opening, when the existing check (Play on Android, the App Store on iOS, the staging APK page) finds a newer
  version: ONE popup — the swimming coho on top, "Version X.Y.Z is out", that version's "What's new" bullets (Android-only
  notes on Android only), Update (the store, the existing action) and Not now.
- Not now records that version as shown: the popup does not come back for it; the update stays in the bell's App update
  section as today. A newer version later shows the popup again.
- Never over the first-run notice, the walkthrough or the first download (it waits for them); never on the web (the
  site updates itself).
- Tests: once per version; dismiss keeps the bell; not shown when current or on the web; waits behind the first-run
  notice / walkthrough; Android vs iOS notes.
- Pictures at 390 px, dark and light, with 0.8.2's draft notes: `t888-*.png`.
