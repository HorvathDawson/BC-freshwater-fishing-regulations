# T1046 — Play production access: closed-test record, feedback log linked to releases, questionnaire

Status: OPEN — the record is set up (`handoff/PLAN/play-production-access/`); stays open until Google approves production
access. Clock: all testers opted in 2026-11-23, earliest apply 2026-12-07.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1046 | Play production access: closed-test record, feedback log linked to releases, questionnaire | OPEN — until Google approves (apply on/after 2026-12-07) | T371, T863 | owner 2026-11-23; record in `handoff/PLAN/play-production-access/` |

Source: owner 2026-11-23, verbatim: "can we add stuff to handoff and as a general ticket we now have all users opted
into closed testing so 14 days start now. we want to make sure all feedback is recording and formatted and linked to new
releases to show we have been working on it. it says to start preparing for the questionaire following the closed
testing here (https://support.google.com/googleplay/android-developer/answer/14151465) can we setup a place for us to
record this so we are rerady for everything we need for after closed testing? make sure future chats will also keep
this up to date"

Follows T371 (the Closed track and the 12 × 14 rule) and T863 (production builds on Closed / TestFlight External).

## Work

1. **The record** (done, lane L31): `handoff/PLAN/play-production-access/`
   - `README.md`: Google's rules with the source link; the clock (owner fields for the opted-in count and times); the
     checklist to apply; how each file is kept current.
   - `feedback-log.md`: one row per piece of feedback, report or crash kind, anonymous tags only (public repo), each
     with its ticket and "Fixed in". Backfilled 2026-11-23: 39 rows (16 tester, 9 in-app report, 14 crash) plus 7 of
     the developer's own finds kept apart.
   - `releases.md`: one row per app release since Play testing began (0.4.0 … 0.8.5, 31 rows) plus 0.9.0 pending,
     with the feedback rows each answered.
   - `questionnaire-draft.md`: the form's 8 questions in order with draft answers from repo facts; every number the
     owner must supply marked TODO with where to get it.
2. **Kept current by every chat** (done, lane L31): `START-HERE.md` step 0 (each new report → ticket + feedback row),
   `runbooks/release-by-hand.md` Step 8 and its checklist (a `releases.md` row; "Fixed in" on the rows a release
   closes), the old owner to-do page (the clock and the apply step).
3. **Each chat until approval** (Claude): log new feedback; ask the owner whether Play Console testing feedback came in
   (no API) and log it; add the 0.9.0 row's facts when it ships; put the newest production build on Closed.
4. **On or after 2026-12-07** (owner): check ≥12 opted in for 14 days, fill the TODOs, apply (Dashboard > "Apply for
   production"). Record the date and Google's answer in the README's clock table and here.

## Reports answered without a ticket of their own

- `e034e473bf96ab93` (staging, the owner, 0.8.5 Android, 2026-11-23): "Why does Harrison lake have hatchery only lake
  trout" — answered as the book read the agreed way (RULINGS C2), no change; `pipeline/docs/GOTCHAS.md` "Harrison Lake's
  lake trout show hatchery only" (PR #85); feedback log D08. Cited here so `python -m pipeline reports` stops calling it NEW.

## Acceptance

Google approves production access; the clock table and this ticket record the dates. Until then every new report and
every release lands in the log in the same chat it is seen.

Depends: T371, T863. Size: S (setup) + upkeep. Owner: yes (the Console counts, the TODOs, the Apply click). Default: keep
the closed test running and the log current; apply on 2026-12-07 if ≥12 stayed opted in.

## Owner

- [ ] 2026-11-23 — In Play Console (Testing > Closed testing > Testers) check that at least 12 testers are opted in; write the count, the 12th tester's opt-in time and any opt-out in the clock table of `handoff/PLAN/play-production-access/README.md` (or tell Claude). Every tester opted in on 2026-11-23, so the earliest application is 2026-12-07.
- [ ] 2026-11-23 — When Play Console > Monitor and improve > Ratings and reviews > Testing feedback shows anything, tell Claude (no API): it is logged in `feedback-log.md` and ticketed.
- [ ] 2026-11-23 — Fill the TODOs at the end of `handoff/PLAN/play-production-access/questionnaire-draft.md` (recruiting pick, usage numbers, age group, installs range, vitals crash / ANR rates).
- [ ] 2026-12-07 — On or after this date: Play Console > Dashboard > "Apply for production", pasting the answers in your own words; then tell Claude the date and, within about 7 days, Google's answer.
- [ ] 2026-11-24 — After production access is granted: Play Production staged rollout of the newest Closed build (Test and release > Production > Create new release > Add from library; start at 20%, raise it from the same page once the crash numbers look clean). The App Store half is T1071.
- [x] 2026-11-24 — Play Closed testing feedback? → "no play closed testing feedback all was through the app pathway. users dont know how to use play one and I dont either." (2026-11-24).
