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

Built (lane `app/t888-update-popup`, from `redesign/stream-sections`, 2026-11-18 23:38 MST):
- core `appUpdate.ts`: `updatePopupVM` (from the bell's own `appUpdateVM`, so both say the same version, bullets and
  button), words in `APP_UPDATE_WORDS.popup` ("Version 0.8.2 is out", "What's new", "Not now", "If you wait, I keep it
  under the bell."); `appUpdateOs` (Play / APK page = Android, App Store = iOS, none = no popup: the web, a local build);
  `notesFor(…, os)` drops a note the changelog tags for the other platform, from the version file's list and the bundled
  one (the bell's section reads it too).
- ui `useAppUpdate`: `popup`, `popupUpdate`, `dismiss`; the answered version is kept in the same small file (`popped`),
  so a later check keeps it. Not now, Done, the scrim and back all count as Not now. Update closes it once the start is
  under way (a failed start stays up and says why).
- ui-native `AppUpdatePopup.tsx`: the app's sheet (a centred dialog on a wide screen), the swimming coho on top (hidden
  from a screen reader), the bullets, Update, Not now. Shell holds it while the first-run notice, the walkthrough (due or
  under way), the bell's sheet or the applying curtain is up. The first download is before the Shell exists.
- "Needed" (a release this app is too old for): its rule is unchanged (it stands until the app is updated), so its popup
  ("New data needs the latest app") comes up on every opening; Not now hides it for that run only.
- A newer version the check cannot name (Play's version code when our play.json is not that build yet): the bell only.
- Guard: `tools/one-spinner.test.ts` lets the coho be a picture in this one file (`COHO_PICTURE`, hidden from a screen
  reader); everywhere else it is still the start screen's and the curtain's only.

Pictures (390 px, light and dark; the real Shell on the fixture with the map stubbed, a faked play.json naming 0.8.2 with
five draft bullets drafted from the 0.8.2 tickets; no 0.8.2 changelog entry exists on this branch yet):
`t888-popup-390-{light,dark}.png`, in the lane's scratchpad (`t888-shots/`).

