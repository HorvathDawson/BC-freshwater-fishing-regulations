# T371 — 0.5.0 to Play CLOSED testing (start the clock for a production release)

Status: OPEN — batch 7 of RELEASE-PLAN-0.5.0 (owner 2026-11-03: "release 0.5.0 to closed testing from internal testing so
we can start getting requirements for google play release").

## Why
Production access on Google Play for a PERSONAL developer account (created after Nov 2023) needs a closed test first:
at least 12 testers opted in for 14 continuous days, then an application for production access (Play Console →
Dashboard). Verify the current numbers in the Console at the time (Google has changed them). The clock starts at the
closed track's first rollout, so 0.5.0 should go there, not only to internal testing.

## Work
1. Play Console: create the Closed testing track (a testers list or Google Group, the opt-in link), countries.
2. Store listing + content: the minimum Play requires before a closed rollout — app content forms (privacy policy URL:
   the privacy page (T352 reworded it), data safety form matching what the app sends (crash/2, report/2, install id,
   location stays on device), target audience, ads: none, content rating), listing text and graphics (T244).
3. Roll 0.5.0 (the batch-7 AAB, same build as internal) out to Closed testing; share the opt-in link; track 12 testers × 14 days.
4. Write down every requirement the Console raises (a checklist here) — that list is the production-release work.

Owner-only steps: the Console clicks, the tester list, any form that states policy. Claude: drafts the data-safety
answers from the code, the listing text, the checklist. Depends: T244 (store listing), T352 (privacy wording).

## Owner 2026-11-04: start with fewer testers
A closed track takes any number of testers and more can be added any time. Production access (a personal account) needs
12 testers opted in for the last 14 CONTINUOUS days — the qualifying 14 days count only once 12 are opted in. Starting
small still clears the track setup, forms and review early. Check the Console's current numbers (Dashboard).
