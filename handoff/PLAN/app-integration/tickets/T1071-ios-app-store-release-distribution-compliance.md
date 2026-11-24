# T1071 — iOS App Store full release: distribution and compliance information

Status: OPEN.

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
