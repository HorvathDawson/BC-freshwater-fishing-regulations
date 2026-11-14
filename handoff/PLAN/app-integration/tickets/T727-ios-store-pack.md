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
