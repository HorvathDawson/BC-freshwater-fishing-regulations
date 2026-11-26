# scripts: dev and CI helpers

Small tools for working in the repo and for CI, run from the repo root. Python ones that CI runs on a bare runner use the standard
library only.

| script | what |
|---|---|
| `bootstrap.sh` | a fresh clone to a machine that can build: tools check, `.venv`, the pinned source data (`data/source/pins.json`) |
| `heavy.sh` | run ONE memory-heavy job at a time per lane, with a free-memory guard: every build goes through it (AGENTS 18, A5) |
| `before_push.py` | what the PR's required checks ask, asked here first (`--list`, `--changed`); the pre-push hook runs it. `before_pr.sh --title "<title>"` the PR title with its suffix, then the two suites left to the PR |
| `ci_paths.py`, `ci_status.py` | the branches and CI's areas, typed once (the workflows' `changes` jobs run it); the status of a commit's checks |
| `fast-fail.sh` | the cheap checks that stop a doomed long job in a minute or two |
| `test-smoke.sh`, `test-full.sh`, `pytest-chunked.sh`, `fresh-copy-tests.sh`, `test-env.sh` | the test tiers (AGENTS 18): smoke per commit, full before a merge, chunked and fresh-copy runs |
| `related_tests.py`, `mutation_report.py` | the tests a change reaches; the PR mutation report (report only) |
| `check_*.py`, `check-*.py`, `check-changelog.mjs` | CI guards: no hard-coded data paths, release-backed goldens paired, format pairs, changelog |
| `dev-build.sh`, `update-hydat.sh`, `hydat-fetch.sh` | refresh the gauge feeds for the dev app; the owner's HYDAT update (AGENTS 64) |
| `start-workflow.sh` | start a GitHub workflow and name its run (never a schedule) |
| `trout-refresh.sh` | the trout fixture refresh chain, one command |

Tests: `pipeline/tests/test_before_push.py`, `test_ci_paths.py`, `test_workflow_pins.py` and the other `test_*` named for a script.
Naming: shell for process orchestration, Python for logic that needs tests; there is a second home for scripts in `deploy/` (uploads and
builds) and a third in `pipeline/tools/` (audits and emitters).
