# T888 — A newer app version: one popup on opening, with the swimming coho and what's new, then the bell

Status: DONE (app chat; merged into 0.8.2 at de20dba04 after two independent reviews; follow-ups in T894).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T888 | A newer app version: one popup on opening (the swimming coho, "Version X.Y.Z is out", what's new, Update / Not now), then the bell; Android and iOS (owner) | DONE | de20dba04 (merge into app/integrate-082) | owner 2026-11-18 ~23:25; approved ~23:42; two reviews; follow-ups T894 |

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


Review fixes (2026-11-19 ~00:20 BC time, UTC-7; independent review, five findings):
1. iPhone list + platform filter: a newer version is never in the bundled changelog, so the filter moved to the build
   scripts (core `notesOn`, changelog.ts): build-apk.sh writes Android's list into latest.json / play.json (and the Play
   notes); build-ios-testflight.sh now writes the iPhone's into `app/ios/appstore.json` after a successful upload (staging
   or production bucket), and bakes `EXPO_PUBLIC_APP_STORE_URL` from `~/.canifishthis/app-store-url`. The iOS check reads
   appstore.json and uses its bullets when it names the App Store's version. `notesFor` shows the file's list as is.
   OWNER: create `~/.canifishthis/app-store-url` (https://apps.apple.com/app/id<Apple ID>) — without it an iPhone build
   has no update check at all (no popup, no bell row); the script warns.
2. A kept "available 0.8.2" on an installed 0.8.2 is dropped on opening (ui `stillNewer`), and the popup needs a version
   ABOVE the installed one (`port.installed`).
3. The bell's sheet shown while a version is due (opened from the new-version notification, say) answers it (`bell`):
   closing the bell pops nothing.
4. The popup is also held while anything else is up: a water / spot / search / pin being added (the release's `safe`),
   a More page, Layers, the date sheet, a tapped mark's sheet, the drop sheet, picking a point, the sync error sheet.
5. Same-version newer builds (staging) are the bell's only: the popup's title and once-key are the version, and only a
   version above the installed one pops.
Tests: core appUpdate.test.ts (logs WITHOUT the incoming version), ui appUpdate.render.test.tsx, ui-native
AppUpdatePopup.shell.test.tsx (bell from the notification; a late check while Layers is open); each new test fails with
its fix removed.
