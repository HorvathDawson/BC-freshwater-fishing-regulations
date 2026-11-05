# T407 — Report a problem: "App bug" and "Feature request" of their own

Status: DROPPED — merged into T242 (2026-11-05 audit; the work continues there) — was: IN PROGRESS — built (app, worker, review app); ships with 0.6.0 (the staging feeds worker deploys with the
release, not ahead of the app).

Source: owner 2026-11-05: "in the app we should add a separate button for bug in report a problem and feature request so
I could use those when testing".

## Done
- `REPORT_REASONS` gains `bug` ("App bug") and `feature` ("Feature request") beside the data reasons
  (core menu/report.ts, menu/words.ts). The form's question and example follow the choice: "What happened?" /
  "What would you like?" (`MENU_WORDS.report.whatFor`).
- The feeds worker takes both (`REASONS`, digest titles "App bug: …" / "Feature request: …"); the review app's Reports
  view names them.
- Tests: ReportSend.test.tsx (choice → question, sent reason), reports.test.ts (worker takes both), menu.test.ts (order).

## Left
- Deploy the staging feeds worker with 0.6.0 (an older worker refuses the new reasons with "bad reason", shown, not lost
  silently — the form keeps the words).
- Phone check with the 0.6.0 APK: send one of each; both reach the digest.
