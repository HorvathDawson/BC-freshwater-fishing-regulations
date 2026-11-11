# T661 — Two tests red after the hardening lanes

Status: OPEN

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
