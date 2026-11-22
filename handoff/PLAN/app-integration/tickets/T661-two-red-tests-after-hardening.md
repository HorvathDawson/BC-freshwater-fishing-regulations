# T661 — Two tests red after the hardening lanes

Status: DONE — d12bb0108, 8c6b53935, b644b39c2: both red tests are green (item 1 fixed, item 2 passes unchanged, stepcache fix); item 3 decided on the evidence (2026-11-22): the parked gate is DROPPED, not installed, and the five claims that heavy.sh runs fast-fail are deleted; the iPhone build runs fast-fail itself (T993 plan 5).

Source: scripts-hardening hand-back 2026-11-11. Both fail on the branch and stop fast-fail's changed-test step.

## Work
1. `pipeline/tests/test_release_command.py::test_heavy_runs_a_scripts_precheck_before_it_takes_the_lock`: second half
   fails since T534, because `app/tools/testbed/session.sh` now wants exactly one AAR in `MLN_LOCAL_MAVEN`. Give the
   test a fixture AAR dir (or point the precheck at one); the heavy.sh half passes.
   Done d12bb0108 (2026-11-11): the TEST was wrong — session.sh's one-AAR check (T534) is real and stays; the test's
   `MLN_LOCAL_MAVEN` now holds one stand-in `android-sdk-vulkan-*.aar`. `test_release_command.py`: 46 passed.
2. `pipeline/tests/test_review_gate.py::test_the_live_tree_prints_pipeline_build_and_after_a_split_the_whole_atlas_chain`:
   trace it (it may only be the dead Oxbows override ids, T637; re-run once T637 lands before touching code).
3. Then install the parked heavy.sh gate (T535's, kept under `handoff/PLAN/app-integration/T535-heavy-gate/` until
   b644b39c2 deleted it) once fast-fail is green. Decided 2026-11-22: dropped (below).

Depends: T637, T534, T535. Size: S.

**2026-11-11 morning (chat 11).** Item 2 and the gate install still wait: fast-fail and the review-gate test need the
build 27 release as LATEST. Also fixed here: `test_stepcache::test_the_read_set_does_not_depend_on_whether_a_pyc_exists`
failed only under the chunked runner's PYTHONPYCACHEPREFIX (8c6b539); `one-colour-pathway.test.ts` did not follow
`check` -> `check:fast` (2577f4a). 13 pipeline tests stay red until the release is promoted (list in STATUS).

**2026-11-11 evening.** Item 2: `test_the_live_tree_prints_pipeline_build_and_after_a_split_the_whole_atlas_chain`
passes on the branch as it is (run alone, unfiltered: 1 passed); no code was touched, so nothing here waits on T637 any
more. Item 3 NOT installed: `scripts/fast-fail.sh` is red (rc 1, 5 min 45 s; 21 failed + 6 errors, all in other lanes'
in-flight work: `test_release_command.py` 18 + 6 errors, `test_shore_cuts.py` 2, `test_status_index.py::
test_the_app_fixture_is_current`). Installing the gate on a red fast-fail stops every lane's next heavy job, which is
the ticket's own condition. The parked pair is still exactly the live `scripts/heavy.sh` + `scripts/tests/heavy.test.sh`
plus the gate (diffed), so the install is still `cp` both and commit. Its shell test, run against the parked copy
(stub jobs, scratch lanes): 0 failed in 12 s with HEAD still; 8 failed in a run during which another lane committed.
Likely cause, not proven: the test hands heavy.sh `FAST_FAIL_DONE=<HEAD at its start>` and a commit mid-run makes the
gate run a fast-fail the scratch tree does not have. Check that before the install: in a shared checkout the gate
itself re-runs fast-fail whenever a neighbour commits between the caller's fast-fail and the lock.

## 2026-11-12 (pipeline lane): still not installed
Another lane is editing `app/` (water Map view, test-bed tools) right now, so fast-fail's app steps run on its in-flight
files; installing a gate that runs fast-fail before every heavy job would stop that lane's emulator runs. The open point
from 2026-11-11 also stands: in a shared checkout the gate re-runs fast-fail whenever a neighbour commits between the
caller's fast-fail and the lock. Install when the tree is quiet, after deciding whether the gate keys on the tree's
content (as the release's fast-fail does since T666) rather than HEAD.

## 2026-11-22 (lane ci/T661-heavy-gate, with T993 plan 5): dropped, not installed — b644b39c2
Decision on the evidence: **delete the claims, not install the gate.**
- The parked pair was no longer "cp both and commit": it forked a two-lane heavy.sh, and today's (323 lines) has the third
  lane (bbee2220f), the 30 % start-gate cap (80e981bc6), the free-disk guard (T788 #11, a3ca419e2) and PR #5's
  per-directory duplicate guard (a5f5dede1); a port would re-open every one of them.
- Unsound for today's callers: it keyed the "already ran" word on HEAD while the release keys its fast-fail pass on the
  tree's content (T666 item 5) — the open point of 2026-11-12, never settled; and it would run the app + pipeline checks
  before every heavy job: the pre-push hook (`scripts/before_push.py`, through heavy.sh), the release's own fresh-copy,
  follow-test and in-season jobs (handed no word), atlas / reach / deliver jobs (an app typecheck failure would stop an
  atlas build), the emulator. T978 moved the other way (a build or a lane never fails on what it does not touch).
- Every long job that wants fast-fail already runs it at its entry: the release preflight, build-apk.sh, and now
  build-ios-testflight.sh (T993 plan 5, deploy/lib/build-links.sh `build_gates`). An ad-hoc heavy job runs it by AGENTS 20a.
Deleted: the parked dir; the claims in `scripts/fast-fail.sh` (header), `deploy/staging/build-apk.sh` (the T535 comment
+ `FAST_FAIL_DONE` / `HEAVY_NO_FAST_FAIL` exports), `deploy/staging/site-all.sh` (the `FAST_FAIL_DONE` export nothing
read), `pipeline/release.py` (the docstring, `Ctx.heavy_env` and its nine hand-offs), `scripts/tests/heavy.test.sh` (the
`FAST_FAIL_DONE` it passed). AGENTS 20a now says heavy.sh runs no fast-fail. Pinned:
`test_staging_scripts_fail_safe.py::test_nothing_hands_heavy_a_fast_fail_word` (no script names either variable; heavy.sh
never calls fast-fail.sh); `test_release_command.py` (the build is handed no env).
Tests (unfiltered): heavy.test.sh 0 failed in 11 s; test_staging_scripts_fail_safe, test_ci_status, test_release_command,
test_deploy_gates, test_before_push, test_workflow_pins, test_ci_paths: 249 passed; app r8-config + test-tag: 18 passed.
