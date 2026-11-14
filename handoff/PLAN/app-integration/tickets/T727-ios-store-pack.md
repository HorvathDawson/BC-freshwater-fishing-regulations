# T727 — iOS: store pack, everything except what needs the Apple account (IOS-10)

Status: IN PROGRESS (iOS chat, docs lane; screenshots after T718).

Source: owner 2026-11-14, verbatim: "Maybe we could make a handoff to start improving the ios versions? Making sure it is
rock solid too?" and "We want to do as much work as possible without the Apple account". Stub IOS-10 in
`handoff/PLAN/ios/START-HERE-ios.md` (approved to start there, "Unattended day"). Simulator only; no Apple account.

## Work
**IOS-10. Store pack for iOS.** Goal: everything for the App Store listing except what needs the account. Do: iPhone
6.9"/6.5" screenshots from `store-shots`, the iOS store video (T709 step 2), App Privacy answers drafted from
`privacy-labels-draft.md`, review notes, age-rating answers, name decision applied. Acceptance: a folder + a checklist the
owner pastes from. Size: M.
  Sub-steps: 6.9"/6.5" shots from the simulator through `store-shots.mjs` (iphone67 / iphone55); iOS store video via
  `store-video` (886 x 1920, 1200 x 1600); review notes; age-rating answers; App Privacy answers cross-checked with
  `play/data-safety.md` and the manifest; apply the name "Can I Fish This?" where `app.json` differs. Mirrors: T319, T709,
  T365 missing list.

## Acceptance
a folder + a checklist the owner pastes from.

Depends: T718 for the screenshots. Size: M. Owner: yes (enters the answers in App Store Connect). Name answered: "Can I Fish This?".

## Results
Text half drafted 2026-11-14 (docs lane). All PROPOSALS; nothing entered in App Store Connect; screenshots and video are another lane's.
Pages in `handoff/PLAN/ios/store/`: `app-privacy-answers.md` (questionnaire + 8-row three-way Mismatches table), `age-rating-answers.md`,
`review-notes.md` (paste block + category), `checklist.md` (ordered fields, who does each, the name differences).

Open points:
- Mismatches to decide: Product Interaction missing from the manifest and draft; Precise vs Coarse location (Play says approximate only); Analytics purpose on the install id; email linked or not; Performance Data not sent; `play/data-safety.md` is stale on two points (production DOES send reports, `app/apps/mobile/src/env.ts:44`; crash switch is More, Settings, Privacy).
- T365's background-mode line is wrong: the background task never downloads a release (`app/apps/mobile/src/notify.native.ts:8-9`); review notes use the corrected wording.
- Name: `expo.name` "Can I Fish?" (home-screen label) is the only differing string; proposal keep it, store name "Can I Fish This?". No app.json edit made.
- Not verified: Apple's live form wording; tap paths in the review notes (not walked on iOS); `fetch`/`processing` in a fresh build's Info.plist (the `ios/` folder here is stale); whether a ~100 m report centre counts as Apple "precise".
- Owner needs: Apple account, App Store Connect entries, production data (T86) before the review build.

- **Checked on the 2026-11-14 simulator build (HEAD 8ba815a23), from its `Info.plist`:** `UIBackgroundModes` = `fetch`,
  `processing` (the text pack's "not verified" point: now verified); `BGTaskSchedulerPermittedIdentifiers` =
  `com.expo.modules.backgroundtask.processing`; `UIDeviceFamily` = 1 (iPhone only); the three permission prompts say
  "Can I Fish This?"; `CFBundleDisplayName` = "Can I Fish?"; `ITSAppUsesNonExemptEncryption` = false;
  `PrivacyInfo.xcprivacy` is in the app. Orientations: Portrait and PortraitUpsideDown (see T726).

## Owner answers (2026-11-14, mid-day)
Verbatim: "all privacy and descriptions and stuff could use playstore items. like icon and name etc it is can i fish? on
the icon but can i fish this? on everything else. we want precise. it is the id mainly for for analytics and bug report
etc etc choose best category"
- Listing text, privacy answers and icon: reuse the Play items (`handoff/PLAN/play/`); the Play page is the source.
- Name: home-screen label "Can I Fish?" stays (`expo.name`); "Can I Fish This?" everywhere else. No app.json change.
- Location: declare Precise.
- Install id: Analytics and bug reports (so purposes = Analytics + App Functionality).
- Category ("choose best", Claude's call): Navigation primary, Sports secondary, matching Play's "Maps & Navigation".
- Not answered, default kept: email is "linked to the user" (a report carries the email and the install id).

### Results, round 2 (docs lane, 2026-11-14)
Owner answers applied to `handoff/PLAN/ios/store/`: Play texts are the source; new `listing.md` maps each App Store Connect field to them (all fit the Apple limits; name, subtitle and keywords sit exactly at 30/30/100; description 3924 of 4000). Decided and stated as decided: home-screen label "Can I Fish?", Precise (and Coarse) location, install id = Analytics + App Functionality, category Navigation then Sports. Open with default: email linked. Flagged (Play-side, not edited): `play/data-safety.md` says production sends no reports; `env.ts:44` says it does. Icon check: `icon-1024.png` is 1024 x 1024 but hasAlpha = yes, so a flattened copy is needed for Apple (not done). UIBackgroundModes fetch + processing verified (coordinator), "not verified" line dropped.
- 2026-11-14: the two stale lines the pack found in `handoff/PLAN/play/data-safety.md` (production sends no reports; the
  crash switch under More, Data) were corrected by the main chat (fa0d0a45c). The owner relayed: nothing submitted to Play
  needs changing, the form's answers were already written for the build that sends reports.

## Audit fixes (branch ios/audit-fixes, 2026-11-14)
- **G7 fixed (a string).** `apps/mobile/app.json` expo-location `locationWhenInUsePermission` is now "Allow Can I Fish This? to centre the map on where you are and to pin a spot where you are standing. It stays on this device." Both real uses named (the first-open map centre, T303/`useOpenAtMe`, and pinning), the app name kept. Sim: fresh install, accept the notice, read the prompt. And: Android shows no purpose string; nothing changes.
- **G12 proposed, not made.** Least machinery: in `apps/mobile/app.json` change `["expo-notifications", { "icon": "./assets/icon/adaptive-monochrome.png" }]` to `["expo-notifications", { "icon": "./assets/icon/adaptive-monochrome.png", "enableRemoteNotifications": false }]`. The installed plugin (`withNotificationsIOS.js`) then skips `aps-environment`; the app schedules only local notifications (`notify.native.ts`), so nothing else changes, and the background-modes the plugin adds only for remote pushes are not touched (we keep `expo-background-task`'s own). Android path of the plugin ignores the flag. Check: `expo prebuild -p ios`, `*.entitlements` has no `aps-environment`; Android: nothing in the manifest changes.
- **G15 proposed, not made.** Already said: Play description (`handoff/PLAN/play/store-description.md:65`), the iOS review notes (`handoff/PLAN/ios/store/review-notes.md:21`), and in app only in two Sources lines: the Province's "not the official version and is not endorsed by the Province" and DFO's "not made in affiliation with ... the Government of Canada" (`core/src/menu/licences.ts:91,98`). Missing: the first-run notice and About. Exact change: `core/src/menu/legal.ts` first point body, after "...onto a map." add " It is not made by, affiliated with or endorsed by the Province of British Columbia or Fisheries and Oceans Canada." and bump `FIRST_RUN_VERSION` to "disclaimer-2" (re-asks everyone once: owner call); the same sentence under `MENU_WORDS.about.notOfficial` (`menu/words.ts:95`). The privacy page text (`tools/seo/html.ts`) is the Android chat's file: add the sentence next to `NOT_LEGAL` there if wanted. Tests pin the whole strings. OWNER ANSWER (coordinator, 2026-11-14): sentence on About only, no FIRST_RUN_VERSION bump. G14 is T732 (sibling lane), dropped from this note.
