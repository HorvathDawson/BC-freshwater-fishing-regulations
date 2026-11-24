order: 730

## 2026-11-23: Google Play production access, the closed-test clock (T1046)
- **Clock:** every tester opted in on 2026-11-23 (you). 14 days → **earliest apply 2026-12-07**, if at least 12 stay
  opted in the whole time (one who opts out and back in restarts their own 14 days).
- **Check (you): T1046 opted-in count** — in Play Console (Testing > Closed testing > Testers) check that ≥12 are opted
  in; write the count, the 12th tester's opt-in time and any opt-out in the clock table of
  `handoff/PLAN/play-production-access/README.md` (or tell Claude).
- **Tell Claude (you): T1046 Play testing feedback** — Play has no API for it: when Play Console > Monitor and improve >
  Ratings and reviews > Testing feedback shows anything, say so; Claude logs it in `feedback-log.md` and tickets it.
- **Fill (you): T1046 questionnaire TODOs** — `handoff/PLAN/play-production-access/questionnaire-draft.md` ends with
  the list (recruiting pick, usage numbers, age group, installs range, vitals crash / ANR rates, …).
- **Apply (you): T1046 production access** — on or after 2026-12-07, Play Console > Dashboard > "Apply for production",
  pasting the answers in your own words. Then tell Claude the date and, within about 7 days, Google's answer.
