# Restart: the speed build chat (T1144, T1161, T1135, T1165, T1159, T1133)

Started 2026-11-25 night by the owner: make development and releases faster. Orchestrator on Fable; at most 3
lanes (Opus for CI and pipeline test changes); one independent review per PR; every PR through the merge queue
with auto-merge; PR titles 93 characters or less; each change shows the failure it still catches. Each lane has
its own scratchpad worktree of `origin/main`; never the main checkout or a release branch.

## Rules of this chat

- Only changes that need no data build, emulator, phone or heavy job: CI workflows, test config, release tooling
  with unit tests. Never `scripts/heavy.sh`, `pipeline release`, atlas / reach / deliver / tiles builds, gradle,
  xcodebuild, the parser. So `scripts/before_pr.sh` is not run while a release builds: each PR body says so.
- The planning chat alone numbers tickets and holds the measured plans: the notes are in
  `handoff/PLAN/app-integration/tickets/archive/night-notes-2026-11-25.tar.gz` (`ci-speed-build-plan.md` first,
  then `ci-merge-queue.md`, `release-timeline.md`, `delivery-strategy.md`, `ship-workflow-design.md`).
- A PR that changes `scripts/heavy.sh` or `pipeline release` is enqueued only after the planning chat checked
  with the release chat. Tell the planning chat each PR number, and the measured minutes after it merges.
- Questions to the owner: multiple choice, in chat. Items under "With the owner's yes" need his yes one by one.
- When the folder rename lands: `python3 scripts/rename_handoff.py --commit` on every open branch, then merge main.

## Order

1. The PR title checked with its " (#NNN)" suffix before a PR opens (T1144): branch `ci/T1144-pr-title-check`.
2. Release runs and heavy jobs keep the Mac awake; an unattended run refuses to start on battery (T1161 item 1):
   branch `chore/T1161-keep-awake`.
3. The pipeline tests in parallel on CI, each test file in one process, measured before and after (T1144):
   branch `ci/T1144-pipeline-tests-parallel`. The Mac keeps `scripts/pytest-chunked.sh`.
4. No third test run on push to main; a small cache job stays (T1144). After 3: same workflow files.
5. One pins file for the release-backed tests, re-cut by one command with the fixtures (T1135). LAST, and ask the
   planning chat first: its follows note refines the design.
6. With the owner's yes on each: the web-only release path and build-time flags (T1165), the data-contract floor
   check (T1165), the pre-cut gate (T1159), the first steps of the ship workflow (T1133).

## State

Read it from GitHub, not from here: `gh pr list --search "T1144 OR T1161 OR T1135 in:title"` and each ticket's
row. Baseline to beat (measured 2026-11-25): a code PR takes a median 40.1 min from last push to merge, 22.4 in
PR CI and 19.7 in the queue; the pipeline `Tests` step is 11 to 19 min on one core.

## Report

Each merged change is reported to the owner and the planning chat with its measured minutes, and written into
its ticket's row.
