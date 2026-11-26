# T371 — 0.5.0 to Play CLOSED testing (start the clock for a production release)

Status: IN PROGRESS — Claude's drafts done (play/data-safety.md, play/listing.md). 2026-11-19: the Closed track holds the gated production build 39 (0.8.3, e9732147f), waiting for Google's review (STATUS 2026-11-19 evening and 22:04). Left, owner: the tester list (12 testers opted in for 14 continuous days) and then the production-access application; the Console's requirement list (Work 4) written down here.
Earlier: IN PROGRESS — 2026-11-05: the build to promote is now **0.6.2, versionCode 11** (on internal since 11-05), not 0.5.0; promote after 2–3 clean internal days. Drafts re-checked 11-05: data-safety current; the Apple/store privacy draft caught up (T244). — the Claude drafts are done (data-safety + listing, 2026-11-04). The Console steps are the owner's. Batch 7 of RELEASE-PLAN-0.5.0 (owner 2026-11-03: "release 0.5.0 to closed testing from internal testing so we can start getting requirements for google play release").

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T371 | 0.5.0 to Play Closed testing: track, listing/app-content forms, 12 testers × 14 days → production access checklist | IN PROGRESS (build 39, 0.8.3 production, on Closed awaiting Google review; owner: testers x 14 days, production access) | e9732147f | owner 2026-11-03 (batch 7) |

Source: owner 2026-11-03: "release 0.5.0 to closed testing from internal testing so we can start getting requirements
for google play release" (batch 7 of RELEASE-PLAN-0.5.0). Owner 2026-11-04: start with fewer testers.

## What
Production access for a PERSONAL developer account (created after Nov 2023) needs a closed test first: at least 12
testers opted in for the last 14 CONTINUOUS days (the qualifying 14 days count only once 12 are opted in; more testers
can be added any time), then an application for production access (Play Console → Dashboard). Verify the current
numbers in the Console at the time (Google has changed them).

## Work (left)
1. Owner: the tester list (12 opted in × 14 continuous days), then the production-access application. Owner-only: the
   Console clicks, the tester list, any form that states policy.
2. Claude: write down every requirement the Console raises (a checklist here) — that list is the production-release work.

Done (Claude, 2026-11-04): `handoff/PLAN/play/data-safety.md` (Data safety answers from the code, file:line evidence;
owner to confirm: Device or other IDs YES (the T352 install id); Approximate location includes the problem report's map
centre (~100 m, since T384 often the reader's own position); App interactions (open water and screen on crash and
report); precise location shared with Open-Meteo (a saved spot's unrounded coordinates) and Esri, both decided 10-29.
Optional: round coordinates to 2 decimals before Open-Meteo (would be a new ticket)) and `handoff/PLAN/play/listing.md`
(short + full description, graphics table reusing T244's `store/graphics/`; app name open: "Can I Fish?" (app.json) vs
"Can I Fish This?" (APP_NAME) vs "canifishthis").
Build note (2026-11-04): the Play AAB is env **staging** (`deploy/staging/build-apk.sh:42`), reports + crash reports go to the
staging worker; an env `production` build has no report or crash endpoint (`app/apps/mobile/src/env.ts:38`). The AAB
also has `ENV_SWITCH=1`, so testers see the environment switch.

Depends: T244 (store listing), T352 (privacy wording).
