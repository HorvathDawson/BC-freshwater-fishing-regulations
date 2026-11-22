# T993 — Document and clean up every release / CI / CD process, so a HUMAN can release by following it

Status: IN PROGRESS (main chat 2026-11-21: owner picked it for this chat; inventory first).

Source: owner 2026-11-21, during the 0.8.5 staging release, verbatim: "we need to have a ticket to make the review
process smoother we always have so many issues we need to fix...".

## Evidence (0.8.5 staging, 2026-11-21): nine stops, each found only by the run before it
1. Fresh copy + fast-fail: the committed mini release had been cut before the T686 switch flipped.
2. Size gate: `water` was over its gate by 85 KB (T985 #2 mouth place). Found after a 25-minute build.
3. App-version gate: `--min-app-version 0.8.4` was not passed, though T686 always needed it.
4. CI red: the feeds worker test pinned the old band words. `pnpm check` doesn't run the worker suite.
5. The staging site was app 0.8.3, under the release's minimum app.
6. Fast-fail: a step I added handed worker files to the app's vitest.
7. Binding diff: Lost Lagoon +1, expected.
8. Golden pages: 9 changed, all renumbered source indexes.
9. Release-backed tests: 7 water-map tests were pinned to the old release's missing mouth place.

## Owner 2026-11-21 (scope widened)
Verbatim: "In the same ticket we want to make it so a human could release following a well documented process with a
lot of those checks handling the items that need to be done etc. the ticket should be about documenting and cleaning up
all the processes ci/cd etc etc".

The goal: a person with no Claude session can cut and ship a release by following one written process. The checks do
the remembering: each one either does the step or says exactly what to do next.

### Scope
- **One release runbook** (for a human): every step from "batch done" to "on production". It covers:
  - the changelog and version bump;
  - format switches and the mini re-cut;
  - data to staging and its smoke;
  - the staging site;
  - Play Internal, then Closed; TestFlight internal and external;
  - production data, the production feeds worker, and the production website.

  Each step gives the one command, what success looks like, and what to do on each failure. It names who may run each
  step (owner-only steps marked).
- **Inventory and clean up every process** behind it:
  - CI workflows (app-ci, pipeline-ci and any others);
  - fast-fail, fresh-copy and `pnpm check`;
  - `pipeline release` and `pipeline build`;
  - site-all.sh and site.sh;
  - build-apk.sh and play-upload.py;
  - build-ios-testflight.sh and testflight-distribute.mjs;
  - the feeds-worker deploy and the prod R2 upload.

  For each, cover: what it is for, what overlaps, what is dead, and what only CI runs (e.g. the worker suite).
  Merge or delete what is duplicated.
- **Checks that handle items, not just stop:** derive what can be derived (the minimum app from the formats), do what
  is safe (re-cut, redeploy the staging site), and list every remaining item at once with its command.
- The release ticket template gets the checklist (see Work below).

## Work (to scope when picked up)
- **One rehearsal that lists EVERYTHING at once**, run before the real release. `--dry-run --rehearse-follows` exists
  (T680); find out why it was not enough, and make it the step before every release. It should include:
  - the candidate build and its size gates;
  - the app/site version gates, with the minimum app derived from the formats (no flag to remember);
  - CI parity: the worker suite, as fast-fail now does;
  - the binding diff;
  - the fixture re-cut;
  - the release-backed tests.
- **Lanes test against the next release's shape.** A lane that changes a format or adds a column re-cuts the mini
  release and runs the release-backed tests on a candidate, so its tests don't pin the old release (stops 1, 2, 9).
- **Auto-accept what is safe.** A golden re-cut that only renumbers ids/indexes, and a binding diff that only names
  entries the release adds on purpose, pass with a printed note (see T992).
- **Release prep checklist** for the release ticket: flip switches, re-cut the mini release, staging site at the new
  app version, the gate budget for each new column.

Depends: T992 (the explanation) folds into the runbook. Size: L. Owner: reads and tries the runbook.

## Owner 2026-11-21 (new chat after 0.8.5)
Picked for this chat with T978, verbatim: "Do t993 and t978 and look for other build/ audit related tickrts that match
this which should go into this chat too. Would be nice to make everything more streamline and fast so future tickets are
easier to work on for releases. This won't lead to a full release yet. The real release 0.8.6 will want more stuff in it
but this is for this chat". No release is cut in this chat.

## Owner 2026-11-21, second answer: what else this chat takes
All four offered groups: release gates (T791 rest, T788 rest), test isolation (T777, T792 B, T768 rest), cleanup (T987,
T661 item 3), faster suites (T866, T931, T736, T485 step 3). Plus, verbatim: "You could also get an agent to do a
basic audit for non standard practices or issues with the flow of data etc. we want to make our code production
level. Also clean up gha workflows and make sure we just waste less time with bad ci/cd of failed runs." The audit is
T994; the GitHub Actions cleanup (fewer failed and wasted runs) is in this ticket's inventory.

## Inventory done 2026-11-21 (read-only agent)
Report: `handoff/PLAN/audit-2026-11-21/t993-inventory.md` (process inventory, 30-day GitHub Actions waste, every 0.8.4 /
0.8.5 stop traced to the check that should have caught it, the gate glossary for T992, a 17-item ranked plan).
Headlines: app-ci is 71 % of Actions time; 210 of its runs (1,578 min) were docs-only pushes run for the ticket guard;
one failing app-ci step skips the worker suite and bundles (it hid 0.8.5 stop 4 for 20.9 h); the rehearsal sees stages
4-5 only on a built candidate and the minimum app is never derived (stops 2, 3, 5, 7, 8, 9 were post-build only);
heavy.sh does not run fast-fail though five places say it does.

Lanes (plan item numbers):
- **G1 GitHub workflows:** 1 (ledger guard own job), 3 (app-ci parallel jobs, no masking), 12 (flakes, TZ), 13
  (housekeeping, partial clone); with T994 findings 6, 7.
- **G2 release command:** 2 (derive minimum app pre-build), 4 (three-step rehearsal), 7 (one before-push command,
  T791 #2; the hook stays the owner's call), 8 (auto-accept renumber-only + attributable diffs), 9 (size headroom),
  15 (no duplicate runs), 16 (data tier on the candidate, T791 #4); T788 #2, #8, #10.
- **G3 deploy gates:** 14; T788 #1, #4, #5, #11; T994 findings 9, 10, 18, 19, 20, 28.
- **After T978 lands:** 5 (heavy.sh gate, T661 #3), 6 (tests never read machine state: T777, T792 B), 10 (T768), T866,
  T987, then 11 (the one human runbook + the T992 explanation), last, once the code settles; 17 with T485 / T736.

## Owner 2026-11-21, dialog (best practice)
Owner, verbatim: "generally we want to change stuff to be best practice in industry".
- **How work lands:** "PRs + required checks (Recommended)": every lane opens a GitHub PR into
  redesign/stream-sections; branch protection requires the CI checks green before it merges; agents open and merge the
  PRs once green. Built after G1's job split lands (the required check names come from it).
- **Pre-push hook:** "Yes, versioned hook (Recommended)": a hook checked into the repo (`core.hooksPath`), so every
  checkout and worktree gets it, running G2's before-push command; `git push --no-verify` is the emergency skip.
