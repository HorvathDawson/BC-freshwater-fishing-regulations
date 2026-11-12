# T661 — Two tests red after the hardening lanes

Status: PARTIAL — d12bb0108; left (Claude): install the parked heavy.sh gate (T535) once scripts/fast-fail.sh is green

Source: scripts-hardening hand-back 2026-11-11. Both fail on the branch and stop fast-fail's changed-test step.

## Work
1. `pipeline/tests/test_release_command.py::test_heavy_runs_a_scripts_precheck_before_it_takes_the_lock`: second half
   fails since T534, because `app/tools/testbed/session.sh` now wants exactly one AAR in `MLN_LOCAL_MAVEN`. Give the
   test a fixture AAR dir (or point the precheck at one); the heavy.sh half passes.
   Done d12bb0108 (2026-11-11): the TEST was wrong — session.sh's one-AAR check (T534) is real and stays; the test's
   `MLN_LOCAL_MAVEN` now holds one stand-in `android-sdk-vulkan-*.aar`. `test_release_command.py`: 46 passed.
2. `pipeline/tests/test_review_gate.py::test_the_live_tree_prints_pipeline_build_and_after_a_split_the_whole_atlas_chain`:
   trace it (it may only be the dead Oxbows override ids, T637; re-run once T637 lands before touching code).
3. Then install the parked heavy.sh gate (T535: `handoff/PLAN/app-integration/T535-heavy-gate/`) once fast-fail is green.

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
