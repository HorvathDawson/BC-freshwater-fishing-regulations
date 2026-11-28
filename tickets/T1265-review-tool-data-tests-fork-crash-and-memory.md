# T1265 — Review-tool data tests: an intermittent fork crash and memory that is never released

Status: OPEN.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1265 | Review-tool data tests: an intermittent fork crash and memory that is never released | OPEN | — | the fork crash (posix_spawn or the repo made before PROJ loads); the fixtures release the atlas caches; the CI tests fetch the blobs they read |

Source: T1258's lane (2026-11-27; its hand-back in the planning chat's night notes `T1258/REPORT.md`, candidate fixes there and in
T1258's "Not done here"), and the CI failure on PR #366.

## The three faults
1. **Fork crash.** `git init` in the `wt` fixture (`pipeline/tests/test_review_write_through.py:105`) died with SIGSEGV in 2 of 4
   closed_areas runs ("crashed on child side of fork pre-exec"): PROJ 9.5.1's fork handler closes its sqlite handle in the child and
   sqlite's logging crashes there. The origin/main run and the rebased run were clean, so main is not proven free of it.
   Candidate fixes: start git through posix_spawn (absolute `/usr/bin/git`, `-C <repo>` instead of `cwd=`, `close_fds=False`, which
   runs no fork handlers), or make the temp repo once per module before PROJ is imported.
2. **Memory never released.** Each backend fixture drops and re-imports app/reuse, so the atlas caches load again (once per file;
   once per test in the function-scoped `wt` fixture) and the old copies stay: closed_areas goes 12 -> 20 -> 27 GB over its three
   needs_atlas tests; the five files in one process come to about 50 GB (the 49-53 GB kills). Until fixed they must run one file at a
   time. Options: release the caches on teardown, or load the backend once per session.
3. **CI blob fetch.** On #366 two review tests failed once: `git show 2bd29a26:<path>` (`MARKED_AT`,
   `pipeline/tests/test_review_proposals.py:110`) exited 128 because CI's lazy blob fetch failed. Make the tests fetch what they read
   (an explicit fetch of that commit, or a frozen fixture of the files).

## Acceptance
Ten closed_areas runs with no crash; in one process the resident size no longer grows test over test (the probe plugin's
per-test reading); the review tests pass on a fresh shallow CI clone.

Depends: none. Size: M. Model: Opus (1, 2), Sonnet (3). Owner: no.
