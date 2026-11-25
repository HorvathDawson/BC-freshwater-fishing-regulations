# T1122 — The stores' data forms say what the privacy page now says

Status: OPEN.

Source: the 0.9.1 chat's T1098 lane, sent to the planning chat 2026-11-25. Related: T1098 (privacy page), T1099
(iPhone privacy manifest), T1071 (App Store submission), T1046 (Play production access).

1. Google Play's own update check: the privacy page never mentioned it (T1098 adds the sentence). Play's Data Safety
   form has not been checked against the page. Read the form's current answers (the owner, in Play Console, or the
   API if it gives them) and list every difference from the page after T1098.
2. `handoff/PLAN/ios/APP-STORE-SUBMISSION.md` (about line 274) says the manifest marks every type "not linked"; that
   is true of build 45 only, not of the 0.9.1 build after T1099. The iOS chat corrects the page for the build that is
   submitted.
3. Rule for later: a change to the privacy page, the manifest or either store form names the other three in its PR.

Acceptance: one table, each data type against the page, the iPhone manifest, the App Privacy labels and Play's Data
Safety form, no row that disagrees. Severity: medium (a store form that disagrees with the app is a rejection or a
policy strike). Release: before the App Store submission for item 2; item 1 before the Play production application
(2026-12-07). Model: Sonnet. Size: S. Owner: yes (reads the Data Safety form out, or changes it; about 10 minutes).
