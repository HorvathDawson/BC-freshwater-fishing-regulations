# T993 — Document and clean up every release / CI / CD process, so a HUMAN can release by following it

Status: IN PROGRESS (main chat 2026-11-21: owner picked it for this chat; inventory first). Plan item 11 (the one human runbook) written 2026-11-22, IN REVIEW: owner: see ## Owner; the other open items are their lanes' (T866, T978, T987, item 17).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T993 | Document + clean up every release / CI / CD process so a human can release from one runbook; checks do the remembering (0.8.5 stopped nine times) | IN PROGRESS (plan item 11, the one human runbook `handoff/PLAN/runbooks/release-by-hand.md`, IN REVIEW: owner: see ## Owner) | T992 | owner 2026-11-21 |

Source: owner 2026-11-21, during the 0.8.5 staging release, verbatim: "we need to have a ticket to make the review
process smoother we always have so many issues we need to fix...".

Owner 2026-11-21 (scope widened), verbatim: "In the same ticket we want to make it so a human could release following a
well documented process with a lot of those checks handling the items that need to be done etc. the ticket should be
about documenting and cleaning up all the processes ci/cd etc etc".
Owner 2026-11-21 (new chat after 0.8.5), picked with T978, verbatim: "Do t993 and t978 and look for other build/ audit
related tickrts that match this which should go into this chat too. Would be nice to make everything more streamline and
fast so future tickets are easier to work on for releases. This won't lead to a full release yet. The real release 0.8.6
will want more stuff in it but this is for this chat". Second answer: all four offered groups (release gates T791 rest,
T788 rest; test isolation T777, T792 B, T768 rest; cleanup T987, T661 item 3; faster suites T866, T931, T736, T485 step
3), plus, verbatim: "You could also get an agent to do a basic audit for non standard practices or issues with the flow
of data etc. we want to make our code production level. Also clean up gha workflows and make sure we just waste less
time with bad ci/cd of failed runs." (the audit is T994).
Owner 2026-11-21 dialog, verbatim: "generally we want to change stuff to be best practice in industry" — "PRs + required
checks (Recommended)"; pre-push hook "Yes, versioned hook (Recommended)" (`core.hooksPath`, `git push --no-verify` the
emergency skip). GitHub Actions cleanup, owner: "clean up the old workflows and old gha. We want a nice clean repo".

## What
A person with no Claude session can cut and ship a release by following one written process; the checks do the
remembering: each one either does the step or says exactly what to do next. Evidence: 0.8.5 staging (2026-11-21) stopped
nine times, each found only by the run before it (stale mini release, `water` over its gate by 85 KB after a 25-minute
build, `--min-app-version` not passed, worker suite not in `pnpm check`, staging site under the minimum app, fast-fail
handing worker files to vitest, binding diff, renumbered golden pages, release-backed tests pinned to the old release).
Inventory: `handoff/PLAN/audit-2026-11-21/t993-inventory.md` (17-item ranked plan; the lanes below are its item numbers).

Done:
- GitHub Actions cleanup (2026-11-21): 8,399 runs of removed workflows deleted; 36 stale caches (10.4 GB → 1.24 GB);
  retention 90 → 30 days; staging-security.yml + staging-smoke.yml deleted (owner "Delete both (Recommended)"); Copilot
  agent "No, remove it (Recommended)": its environment and one run deleted.
- G1 workflows (`lane/t993-g1-gha`; 8c1228707, 77ee9ca7d, 8a89a73a9): plan 1 `docs-ci.yml`; plan 3 app-ci = `gates` /
  `vitest` / `bundles` + `feeds-worker-ci.yml`; plan 12 `WaterScreen.unnamed` timeout fixed (one shared store promise),
  `TZ: America/Vancouver`; plan 13 action bumps, partial clone; T994 6 / 7 (`permissions: contents: read`, data-path
  grep; its allowlist is GONE since T1021, 2026-11-24). Shape pinned in `pipeline/tests/test_workflow_pins.py`.
- G3 deploy gates (`lane/t993-g3-deploy` a3ca419e2): T788 #1 (`StatusUnread`), #4 (`deploy/lib/site-stamp.sh`), #5
  (`--no-basemap` / `--no-synopsis` refused on the live root), #11 (`heavy.sh` disk floor, `HEAVY_MIN_DISK_GB`,
  `HEAVY_KILL_DISK_GB`); plan 14. Tests `pipeline/tests/test_deploy_gates.py`.
- G2 release command (`lane/t993-g2-release`; ea6d534f2, 60ed4ac17, d3272d735, d0f0d6ab1, 31d770acf, 5d6478a09): plan 2
  min app derived pre-build, 4 rehearsal NOT SEEN, 9 `build.headroom`, 8 renumber-only / attributable diffs
  (`pipeline/release_attribute.py`), 15, 16 `data-tier`, 7 `scripts/before_push.py`; T788 #2, #8, #10; rc 76
  `RC_WORKFLOW_UNREADABLE`.
- T768 lane (`test/T768-follow-tests`; ff946c86e, c5d66fccc, 1716bcb87, f8916d70f): plan 10 anchor scanner `absent`,
  column-without-re-cut `--check`, `app/tools/part-end-proof.mjs`; plan 12 `WaterMode` across-waters: no failure since
  T849.
- Plan 5 (heavy.sh gate, T661 #3): DONE b644b39c2.
- Plan item 11 (IN REVIEW, lane docs/T993-one-human-runbook): `handoff/PLAN/runbooks/release-by-hand.md` is THE runbook
  (Steps 0-8, fix during a release, data-only, rollback, checklist); T992's explanation
  `handoff/PLAN/runbooks/release-gates-explained.md`; `release.md` and `PRODUCTION.md` updated; reviewed 2026-11-22 by a
  second agent. Owner answers 2026-11-22 applied: (1) CONFIRM_PROD, verbatim: "only me unless i say otherwise that time"
  — the production website, feeds worker and data are owner steps; Claude prepares and hands over the exact command.
  (2) Closed testing gets the PRODUCTION Android build ("Production build (Rec.)"). (3) Version: "let it be 0.9 but we
  wont release yet until we have more stuff from next chat in it".

## Work (left)
- Owner: pick or drop the eight simplification ideas (## Owner); read and try the runbook.
- Other lanes: T866, T978, T987, item 17 (with T485 / T736); plan 6 (tests never read machine state: T777, T792 B).
- The claude.ai doc twin of the runbook (https://claude.ai/artifact/5r4yEqNwqygin1oKo5Zxjk) still has the 2026-11-18
  text; sync it when item 11 lands.
- G2 left: the hook (owner's call); the real-data rehearsal (`--dry-run --build --rehearse-follows`) not run (a lane
  worktree has no data); the data tier's time on a candidate not measured; the manifest's `regulations.versions` /
  layout versions are not app-gated. Stop 7 (Lost Lagoon) would now STOP: T981's commit changed reach code — conservative
  by the review.
- T768 left: the pipeline scanner's `absent` kind (`pipeline/tests/anchors.py`); a whole record typed or
  `toBeUndefined()` on a release field stays unscannable.
- G1 unverified on a runner at the time: the `filter: blob:none` clone with the tests' `git show <commit>:<path>`, the
  composite action's working directories, the saved minutes. `feeds-sync-inputs`: disable needs the token: `gh workflow
  disable feeds-sync-inputs.yml` (or the Actions tab). Turning the Copilot agent off in the repo's Copilot settings is
  the owner's (not reachable by API). Not caught by the data-path grep, for G3/F2: `pieces_gate.py:156`
  `Path("data/release")` default.

Depends: T992 (the explanation) folds into the runbook. Size: L. Owner: reads and tries the runbook.

## Owner

- [ ] 2026-11-21 — Pick or drop the eight simplification ideas at the end of `handoff/PLAN/runbooks/release-gates-explained.md`.
