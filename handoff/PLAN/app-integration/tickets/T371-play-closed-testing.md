# T371 — 0.5.0 to Play CLOSED testing (start the clock for a production release)

Status: IN PROGRESS — Claude's drafts done (play/data-safety.md, play/listing.md); owner: the Play Console steps (OWNER-TODO "T371"). The build to promote is T495's gated production build; build 22 (0.7.0, staging) is the newest internal one (2026-11-09).
Earlier: IN PROGRESS — 2026-11-05: the build to promote is now **0.6.2, versionCode 11** (on internal since 11-05), not 0.5.0; promote after 2–3 clean internal days. Drafts re-checked 11-05: data-safety current; the Apple/store privacy draft caught up (T244). — the Claude drafts are done (data-safety + listing, 2026-11-04). The Console steps are the owner's. Batch 7 of RELEASE-PLAN-0.5.0 (owner 2026-11-03: "release 0.5.0 to closed testing from internal testing so we can start getting requirements for google play release").

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
3. Roll the current internal build (0.6.2, versionCode 11 as of 11-05; same AAB, no rebuild) out to Closed testing; share the opt-in link; track 12 testers × 14 days.
4. Write down every requirement the Console raises (a checklist here) — that list is the production-release work.

Owner-only steps: the Console clicks, the tester list, any form that states policy. Claude: drafts the data-safety
answers from the code, the listing text, the checklist. Depends: T244 (store listing), T352 (privacy wording).

## Owner 2026-11-04: start with fewer testers
A closed track takes any number of testers and more can be added any time. Production access (a personal account) needs
12 testers opted in for the last 14 CONTINUOUS days — the qualifying 14 days count only once 12 are opted in. Starting
small still clears the track setup, forms and review early. Check the Console's current numbers (Dashboard).

## 2026-11-04: drafts written (Claude)
- `handoff/PLAN/play/data-safety.md` has the Data safety answers, drafted from the code, each with file:line evidence.
  Answers the owner must confirm: Device or other IDs is now YES (the T352 install id; the 10-29 draft said No);
  Approximate location now includes the problem report's map centre (~100 m, and since T384 often the reader's own
  position); App interactions is new (the open water and screen on crash and report); precise location shared with
  Open-Meteo (a saved spot's unrounded coordinates, sent when a visit is logged, when a spot's panel loads and on
  refresh) and Esri, both decided 10-29. Optional: round coordinates to 2 decimals before Open-Meteo, so only
  approximate location leaves the phone (would be a new ticket).
- Build matters: the Play AAB is env **staging** (`deploy/staging/build-apk.sh:42`), so its reports and crash reports go
  to the staging worker. An env `production` build has no report or crash endpoint (`app/apps/mobile/src/env.ts:38`)
  and sends neither. The AAB also has `ENV_SWITCH=1`, so testers see the environment switch.
- `handoff/PLAN/play/listing.md` has the short and full description (the one copy; the store/listing-draft.md text
  moved there) and the graphics table, reusing T244's files in `store/graphics/`. The app name is open: "Can I Fish?"
  (app.json) vs "Can I Fish This?" (APP_NAME) vs "canifishthis".
