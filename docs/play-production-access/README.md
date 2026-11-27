# Google Play production access: the closed-test record (T1046)

Everything needed to apply for production access on Google Play once the closed test is done, kept current as the
test runs. Ticket: `handoff/PLAN/app-integration/tickets/T1046-play-production-access-record.md` (stays OPEN until
Google approves). Earlier history: T371 (the closed track) and T863 (production builds to Closed / TestFlight External).

Files in this folder:

| File | What it holds | Who keeps it current, and when |
|---|---|---|
| `README.md` | Google's rules, the clock, the checklist to apply | Claude, when a checklist item changes; the owner fills the owner fields |
| `feedback-log.md` | one row per piece of tester feedback, report or crash, linked to its ticket and the release that fixed it | every chat, at `START-HERE.md` step 0 (reports first): each new item is a row plus a ticket |
| `releases.md` | one row per app release since testing began, with the feedback rows it answered | every release (`runbooks/release-by-hand.md` Step 8): add the row, set "Fixed in" on the rows it closes |
| `questionnaire-draft.md` | the application form's 8 questions in form order, each with a draft answer from repo facts | Claude refreshes the numbers before the owner applies; the owner rewrites in his own words |

**This repo is public.** Nothing here may identify a tester: no names, emails, phone models tied to a person, install
ids, report ids, or quotes that point at someone. A tester is an anonymous tag (see `feedback-log.md`).

## What Google requires (read 2026-11-23)

Source: https://support.google.com/googleplay/android-developer/answer/14151465 (re-read it before applying; Google
changes the numbers).

- A **new personal developer account** must run a closed test with **at least 12 testers opted in continuously for at
  least 14 days** before it can apply for production.
- A tester who opts out and back in **restarts their own 14 days**. When applying, at least 12 must be opted in and
  must have been opted in for the 14 days before.
- **Apply** from Play Console: Dashboard > "Apply for production". The form has three parts (our drafts:
  `questionnaire-draft.md`):
  1. **About your closed test:** how easy it was to recruit testers (single choice); engagement (did testers use all
     features; did usage match what is expected in production, and how it differed); a summary of the feedback and how
     it was collected.
  2. **About your app** (not shown publicly): the target audience, as specific as possible; the value proposition; the
     estimated installs in the first year (a range).
  3. **Production readiness:** what changed from what the closed test taught; how we decided the app is ready.
- Google keeps testers' feedback under **Play Console > Monitor and improve > Ratings and reviews > Testing feedback**,
  filterable by app version. Google's tip: "Maintain a record of received feedback" (that is `feedback-log.md`).
- Review usually takes **7 days or less**. If engagement or the tester count is too low, the answer is to keep the
  closed test running and apply again.
- After approval: **keep testing** (Closed stays open), and policy compliance and a crash-free app stay on us.

## The clock

| Field | Value |
|---|---|
| Closed track opened | 2026-11-19: build 39 (0.8.3, production) rolled out to Closed (STATUS 2026-11-19 evening) |
| All testers opted in (owner, 2026-11-23) | **2026-11-23**, day 1 of 14 |
| Earliest apply date | **2026-12-07** (14 full days after 2026-11-23), only if ≥12 stayed opted in the whole time |
| Opted-in count today | OWNER: check in Play Console (Testing > Closed testing > Testers) that ≥12 are opted in: ____ |
| Exact opt-in time of the 12th tester | OWNER: from Play Console: ____ |
| Anyone opted out and back in? | OWNER: ____ (their 14 days restart; move the earliest apply date) |
| Applied on | OWNER: ____ |
| Google's answer | OWNER: ____ (approved / keep testing; date) |

If a tester drops out and the count goes under 12, the 14 days start again from the day it is back at 12: write the new
date in the table and in the owner to-do: a new item file in `handoff/PLAN/owner-todo/` (T1041; never `OWNER-TODO.md`
itself, which is a pointer page).

## Checklist to apply

Owner-only steps are marked **[you]**; everything else Claude keeps ready.

- [ ] ≥12 testers opted in continuously since 2026-11-23 (or the later restart date) **[you, Play Console]**
- [ ] 14 days passed: on or after 2026-12-07 **[you]**
- [ ] `feedback-log.md` current: every Play testing-feedback item read in the Console and logged (Play has no API for it)
      **[you read the Console; Claude writes the rows]**
- [ ] `feedback-log.md` current for the automatic sources: `.venv/bin/python -m pipeline reports` read, every NEW item a
      row plus a ticket **[Claude]**
- [ ] `releases.md` current through the last release on Closed, each feedback row with its "Fixed in" **[Claude]**
- [ ] The newest production build is on Closed testing (0.9.0 when it ships), so testers are on the build we apply with
      **[you: promote; Claude may run the promote, see `runbooks/release-by-hand.md` Step 6]**
- [ ] Android vitals: crash rate and ANR rate under Google's bad-behaviour thresholds for the last 28 days
      (Play Console > Monitor and improve > Android vitals) **[you read; Claude notes the numbers in the draft]**
- [ ] Policy: Data safety form matches `handoff/PLAN/play/data-safety.md`; privacy policy URL live; content rating and
      target audience forms done; no policy warnings in the Console inbox **[you]**
- [ ] `questionnaire-draft.md`: every TODO filled, answers read and put in the owner's own words **[you, Claude drafts]**
- [ ] Apply: Dashboard > "Apply for production"; paste the answers **[you]**
- [ ] Record the date applied and Google's answer in the clock table and T1046 **[you tell; Claude writes]**
- [ ] If refused: keep Closed running, read the reason, ticket it, re-apply when fixed

## How each file is kept up to date

- **Every chat, `START-HERE.md` step 0:** run the reports command; each NEW report, crash kind, TestFlight item or
  relayed tester message becomes a ticket AND a `feedback-log.md` row (status "open", "Fixed in" empty). Ask the owner
  whether Play testing feedback came in (no API) and log what he relays.
- **Every release, `runbooks/release-by-hand.md` Step 8:** add a `releases.md` row (version, date, tracks, what changed,
  feedback rows answered) and set "Fixed in" plus status "fixed" on each feedback row the release closes.
- **Ticket closed outside a release** (a worker or data change): set the row's "Fixed in" to what shipped it
  (`feeds worker <date>`, `data r-…`).
- **Before applying:** refresh every number in `questionnaire-draft.md` from the log and the consoles.
