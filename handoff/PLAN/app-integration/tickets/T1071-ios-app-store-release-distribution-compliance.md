# T1071 — iOS App Store full release: distribution and compliance information

Status: IN PROGRESS (iOS release chat: the paste-from page is written, `handoff/PLAN/ios/APP-STORE-SUBMISSION.md`; the submission waits on the new pictures, T1072, and on the owner's open picks).

Source: owner 2026-11-24 (planning chat), verbatim: "another item to add is preparing the ios store distribution and
compliance info ... IOS can have full release soon." Builds on T727 (the store pack drafted without the Apple account:
App Privacy answers, review notes, age rating) and T863 (TestFlight external, approved since 0.8.2).

## Work
1. One checklist of everything App Store Connect asks before "Submit for Review", each line with the drafted answer or
   "owner": App Privacy (nutrition labels, from T727's draft, re-checked against what 0.9.x sends: reports, crash
   reports, push token), export compliance (encryption: HTTPS only; `ITSAppUsesNonExemptEncryption`), age rating,
   content rights (BC synopsis, DFO, WSC, Esri imagery, map data: licences and attribution), pricing (free) and
   availability (which countries), category, support URL, marketing URL, privacy policy URL, copyright line, review
   notes (no sign-in; how to reach each feature), trader status (EU Digital Services Act, only if sold in the EU).
2. Listing text: name, subtitle, promotional text, description, keywords, "What's New" (shared with T1072).
3. Screenshots and preview video in the required sizes (from T1072 / T319).
4. Push (T739, T822, T996): production APNs proved before the build that goes to review.
5. A runbook step list for the release itself: the build, Submit for Review, phased release, and what the owner
   clicks. Not touched: the Play production-access wait (T1046), which is separate.

## Acceptance
A page under `handoff/PLAN/ios/` the owner pastes from, with no unanswered line; the App Store version submitted by
the owner; the release recorded in STATUS.

Depends: T727 (draft), T1072 (shots, text). Severity: launch work, no defect. Release: its own chat (roadmap 5i), the first
chat after 0.9.1 (owner 2026-11-24, picked "iOS full release first"). Model: Sonnet (docs), Opus only if a privacy answer needs the code read. Size: M.
Owner: yes (App Store Connect entries, the submit, any legal answers).

## Owner answers (iOS release chat)
- 2026-11-24 23:05, on the store pictures, verbatim: "we want the android and iphone images to use the same set ideally.
  if it is possible. we should generate new images too." Reading (the chat's): one set of frames for both stores, cut to
  each store's sizes (the T319 tool already does this: the 0.8.2 set has the same seven frames for Play and the App
  Store), and a new set made from the 0.9.x app. The new set is T1072's work (store content chat S, with T1076 first).
- 2026-11-24 23:04, multiple choice, "Should the App Store submission wait for the new set?": **"Wait for the new set"**. So the
  submission waits on T1072 (chat S); this chat finishes every other answer and the push proof meanwhile. Why it
  matters: App Store pictures change only with a new app version; Play pictures change any time.
- 2026-11-24 23:12, four multiple-choice answers:
  - App Privacy "linked to you": **"Strict: linked"**. Linked = Yes for email, report text, the report's map centre,
    install id, crash data, product interaction and diagnostics; no code change.
  - Countries: **"All 175 countries"**. Follows: the EU trader status is shown on EU store pages, so that statement
    (trader or not) is still his to make.
  - Build for review: **"Newest by then"**. When the pictures are ready: 0.9.1's iPhone build if uploaded and push
    proven on it, otherwise 0.9.0 (45).
  - Production push proof: **"Soon, on build 45"**. He sets the three APNs secrets on the production worker and does
    the phone steps; the chat reads the status and watches an hour.

## Results (iOS release chat, 2026-11-24)
- The page: `handoff/PLAN/ios/APP-STORE-SUBMISSION.md`. Restart page: `handoff/PLAN/ios/RESTART-ios-release-chat.md`.
- Read against the uploaded build's code (0.9.0 (45), 65f84020c, an Opus lane, read only). What changed against the
  T727 drafts: the push token is new (sent as `{platform, token, env}` only, never with the install id; kept until
  unregistered or dead, no age limit; declared under Device ID); Apple's "Precise" means 3 or more decimals, so the
  report's map centre is Precise and the weather and satellite calls are Coarse (the draft had the reasons the other
  way round); a stored report holds email and install id together, so the strict "linked" answer is Yes for the
  report-borne types; the More menu has no "Legal" row (the disclaimer is in About, the first-run notice and each
  regulations page); the server sends only silent pushes; `remote-notification` is a background mode now.
- Review risk read: no tester switch in a production build; every iPhone location call catches a refusal (T1093 has
  no iPhone twin found); export compliance is exempt (OS TLS, SHA-256 file checks only).
- Not verified: the generated Info.plist / entitlements / PrivacyInfo of build 45 (source read only); the 0.9.0
  first-download size (the app and the listing both say about 500 MB); Apple's exact age-rating wording and whether
  the screenshot screen demands a second iPhone size (Apple's help page was read through a summary).
- Production push (T996): not proven on 2026-11-24, `push.missing.ios` on production lists the three APNs secrets.
- Finds sent to the planning chat for numbering: privacy page wording (six points), privacy manifest gaps, open-source
  notices not credited, About says "build 44" on iPhone build 45, the screenshot push script's default folder.
- Review round (one, independent, Sonnet, against the 0.9.0 code): counts and paths right; fixed from it: the review
  notes said a data update is always downloaded in the background (only a silent-push wake of a running app does,
  and only if the download setting allows), "the GPS position is never sent to us" (a report's map centre can be where
  the reader is), the walkthrough after the first-run notice, the manifest's "not linked" flags named. Kept against
  the review: the Analytics purpose on Device ID (owner 2026-11-14), `fetch` in the background modes (read from a
  built Info.plist on 2026-11-14, T727; not re-read on build 45).
