# T727 — iOS: store pack, everything except what needs the Apple account (IOS-10)

Status: IN PROGRESS (iOS chat, docs lane; screenshots after T718).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T727 | iOS: store pack, everything except what needs the Apple account (IOS-10) | IN PROGRESS (iOS chat, docs lane; screenshots after T718) | — | owner 2026-11-14 (iOS reopened, simulator only); stub IOS-10 in docs/ios/START-HERE-ios.md |

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-10 in
`docs/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.
Owner answers 2026-11-14 mid-day, verbatim: "all privacy and descriptions and stuff could use playstore items. like icon
and name etc it is can i fish? on the icon but can i fish this? on everything else. we want precise. it is the id mainly for
for analytics and bug report etc etc choose best category"

## What
**IOS-10. Store pack for iOS.** Everything for the App Store listing except what needs the account: iPhone 6.9"/6.5"
screenshots, the iOS store video (T709 step 2), App Privacy answers, review notes, age-rating answers, the name applied.

Done (docs lane, 2026-11-14, all PROPOSALS, nothing entered in App Store Connect): `docs/ios/store/`
`listing.md` and, in git history since 2026-11-25 (superseded by `docs/ios/APP-STORE-SUBMISSION.md`), `app-privacy-answers.md`, `age-rating-answers.md`, `review-notes.md`, `checklist.md` (each App Store Connect
field from the Play texts; name, subtitle, keywords exactly 30/30/100; description 3924 of 4000). Decided: home-screen label
"Can I Fish?" stays (`expo.name`), "Can I Fish This?" everywhere else (no app.json change); Precise (and Coarse) location;
install id = Analytics + App Functionality; category Navigation then Sports (Claude's call, matching Play's "Maps &
Navigation"). Open with default: email is "linked to the user". `Info.plist` of 8ba815a23 checked (`UIBackgroundModes`
fetch + processing, `UIDeviceFamily` 1, `ITSAppUsesNonExemptEncryption` false, `PrivacyInfo.xcprivacy` in). Play's
`data-safety.md` stale lines fixed (fa0d0a45c). T365's background-mode line is wrong (the background task never downloads a
release, `app/apps/mobile/src/notify.native.ts:8-9`); review notes use the corrected wording. G7 fixed: expo-location
`locationWhenInUsePermission` names both real uses.

## Work (left)
1. Screenshots 6.9"/6.5" (after T718, from `store-shots.mjs`) and the iOS store video (T709; `store-video`, 886 x 1920, 1200 x 1600).
2. Icon: `icon-1024.png` is 1024 x 1024 but hasAlpha = yes, so a flattened copy is needed for Apple (not done).
3. Not verified: Apple's live form wording; tap paths in the review notes (not walked on iOS); whether a ~100 m report
   centre counts as Apple "precise".
4. **G12 proposed, not made.** In `apps/mobile/app.json` change `["expo-notifications", { "icon": "./assets/icon/adaptive-monochrome.png" }]`
   to `["expo-notifications", { "icon": "./assets/icon/adaptive-monochrome.png", "enableRemoteNotifications": false }]`.
   The installed plugin (`withNotificationsIOS.js`) then skips `aps-environment`; the app schedules only local
   notifications (`notify.native.ts`); background-modes the plugin adds only for remote pushes are not touched (we keep
   `expo-background-task`'s own). Check: `expo prebuild -p ios`, `*.entitlements` has no `aps-environment`; Android:
   nothing in the manifest changes.
5. **G15.** OWNER ANSWER (coordinator, 2026-11-14): sentence on About only, no FIRST_RUN_VERSION bump. The sentence: " It is
   not made by, affiliated with or endorsed by the Province of British Columbia or Fisheries and Oceans Canada." under
   `MENU_WORDS.about.notOfficial` (`menu/words.ts:95`). Already said in the Play description
   (`app/store-listing/listing.json:65`), the review notes (now in `docs/ios/APP-STORE-SUBMISSION.md`) and two Sources
   lines (`core/src/menu/licences.ts:91,98`). The privacy page text (`tools/seo/html.ts`): next to `NOT_LEGAL` if wanted.
   Tests pin the whole strings. G14 is T732.
6. Owner needs: Apple account, App Store Connect entries, production data (T86) before the review build.

## Acceptance
a folder + a checklist the owner pastes from.

Depends: T718 for the screenshots. Size: M. Owner: yes (enters the answers in App Store Connect). Name answered: "Can I Fish This?".
