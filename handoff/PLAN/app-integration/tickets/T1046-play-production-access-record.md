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
   closes), `OWNER-TODO.md` (the clock and the apply step).
3. **Each chat until approval** (Claude): log new feedback; ask the owner whether Play Console testing feedback came in
   (no API) and log it; add the 0.9.0 row's facts when it ships; put the newest production build on Closed.
4. **On or after 2026-12-07** (owner): check ≥12 opted in for 14 days, fill the TODOs, apply (Dashboard > "Apply for
   production"). Record the date and Google's answer in the README's clock table and here.

## Acceptance

Google approves production access; the clock table and this ticket record the dates. Until then every new report and
every release lands in the log in the same chat it is seen.

Depends: T371, T863. Size: S (setup) + upkeep. Owner: yes (the Console counts, the TODOs, the Apply click). Default: keep
the closed test running and the log current; apply on 2026-12-07 if ≥12 stayed opted in.
