# scripts: dev and CI helpers

Small tools for working in the repo and for CI, run from the repo root. Python ones that CI runs on a bare runner use the
standard library only. Every file in this folder is named below (`pipeline/tests/test_readmes_name_every_tool.py`
fails when one is not).

## Set up
| script | what |
|---|---|
| `bootstrap.sh` | a fresh clone to a machine that can build: tools check, git hooks, `.venv`, JS install, then the pinned source data (`data/source/pins.json`, about 16 GB). `--setup-only` stops before the download (the cheap path; `docs/runbooks/fresh-machine.md` section 2) |

## Before a push or a pull request
| script | what |
|---|---|
| `before_push.py` | what the PR's required checks ask, asked here first (`--list`, `--changed`); the pre-push hook runs it |
| `before_pr.sh` | `--title "<title>"`: the PR title with its merge suffix, then the two suites the hook leaves to the PR |
| `related_tests.py` | the tests a change reaches (the hook and the mutation report use it) |
| `mutation_report.py` | the PR mutation report, report only; `mutation-requirements.txt` pins its tools |
| `fast-fail.sh` | the cheap checks that stop a doomed long job in a minute or two (AGENTS 20a) |

## Tests (AGENTS 18)
| script | what |
|---|---|
| `test-env.sh` | the test environment, sourced by the runners below, never run |
| `test-smoke.sh` | Tier 1: the smoke set |
| `test-full.sh` | Tier 2: the full set, through `heavy.sh`, at milestones or before a merge |
| `pytest-chunked.sh` | the pipeline suite in chunks, one process each (one process grows to about 111 GB) |
| `fresh-copy-tests.sh` | HEAD cloned to a temp folder and tested there (the release pre-check runs it) |
| `tests/heavy.test.sh`, `tests/start-workflow.test.sh` | shell tests of `heavy.sh` and `start-workflow.sh`; `fast-fail.sh` runs them |

## Guards CI runs
| script | what |
|---|---|
| `check_data_paths.py` | no hard-coded curated or source data path (pipeline-ci) |
| `check-format-pairs.py` | every format string Python writes that the app or worker reads has one value on both sides |
| `check-goldens-paired.py` | the app's release-backed goldens were cut from the release `LATEST` names |
| `check-changelog.mjs` | the app version has a changelog entry with a bullet |
| `ci_paths.py` | the branches and CI's areas, typed once (the workflows' `changes` jobs run it) |
| `ci_status.py` | is CI green on a commit: the newest runs of the four required workflows, via `gh` |

## Machine and data
| script | what |
|---|---|
| `heavy.sh` | run ONE memory-heavy job at a time per lane, with a free-memory guard; every build goes through it (AGENTS 18, A5). `--status` shows the lanes |
| `start-workflow.sh` | start a GitHub workflow and print its run id (never a schedule) |
| `hydat-fetch.sh` | one HYDAT release, downloaded resumably and checked, then extracted |
| `update-hydat.sh` | the owner's one command when the feed says a newer HYDAT is out |
| `dev-build.sh` | the gauge roster and feed for the dev app, in dependency order |
| `trout-refresh.sh` | the trout fixture refresh chain, one command |
| `rename_handoff.py` | one-off (ran for T1081): the `handoff/PLAN/` to `docs/` and `tickets/` rename |

Tests: `pipeline/tests/test_before_push.py`, `test_ci_paths.py`, `test_workflow_pins.py` and the other `test_*` named for a script.
Naming: shell for process orchestration, Python for logic that needs tests; there is a second home for scripts in `deploy/`
(uploads and builds) and a third in `pipeline/tools/` (audits and emitters).
