# T995 — Branch structure: `main` is the protected trunk; PRs with required checks; release branches and tags

Status: PARTIAL — the switch landed 2026-11-22: code c374b91fd (lane ci/T995-main-trunk + review/T995, reviewed), `main` = trunk. pruned 2026-11-22 (lane L13: 16 merged local branches and the clean merged worktree wt1008 deleted; 12 merged remote lane branches deleted: app-fix/t822-push-register, ios/t739-push, lane/l10-cw-through, l5-push, l6-ios, l8-shrink, l9-strictest, la-feeds-leftovers, lf-review-leftovers, r1-084, t739-privacy, t926-numbers2; kept: `main`, `redesign/stream-sections`, `cache/maplibre/fslean-m2`, locked or dirty worktrees, every session lane). left (Claude): delete `redesign/stream-sections` and `cache/maplibre/fslean-m2` after the 0.9.0 production feeds worker redeploys from main (it dispatches on that ref until then).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T995 | Branch structure: `main` is the protected trunk; PRs with required checks; release branches and tags | PARTIAL (switch landed c374b91fd 2026-11-22; pruned 11-22; left (Claude): delete `redesign/stream-sections` + `cache/maplibre/fslean-m2` after the 0.9.0 production feeds worker redeploys) | c374b91fd, d4eae5b46 | owner 2026-11-21 |

Source: owner 2026-11-21, verbatim: "Also we could rethink our git branch structure to have a more real branching
structure ?" and "generally we want to change stuff to be best practice in industry". Dialog answers (all the
recommended options): "main is the trunk", "Release branch at lock", "Merge commit". Earlier the same day (T993):
"PRs + required checks" and "Yes, versioned hook".

## Done
- Code side (lane `ci/T995-main-trunk` + `review/T995`, c374b91fd): one name per language — `scripts/ci_paths.py`
  (`TRUNK`, `REMOTE`, `release_branch()`, `app_version()`, `AREAS`, `REQUIRED`, `REPO`) and `app/packages/core/src/repo.ts`
  (`REPO_BLOB`, the worker's `DEFAULT_REF`), pinned equal by `test_ci_paths.py` (also: nothing outside `handoff/` names the
  old dev branch); every workflow runs on every `pull_request`, a `changes` job gates the real jobs and the last job `ok`
  passes only on success/skipped (`ci_paths.py verdict`); `test_workflow_pins.py` derives and pins AREAS. Hooks
  `.githooks/pre-push` (`before_push.py --changed` / `--pre-push`; `BEFORE_PUSH_SKIP=<reason>` for the release command's
  pushes), `.githooks/commit-msg`. Release command `--cut X.Y.Z`, preflight wants HEAD = the tip of
  `origin/release/<app.json version>`, tags `data/<r-id>` + `v<version>` at ship, `--prod` refuses missing / moved tags
  (`test_release_command.py`).
- GitHub side (main chat 2026-11-22): `main` fast-forwarded to c374b91fd (`-s ours` over main's stale d6a5ca528);
  `core.hooksPath .githooks`; merge commits only, auto-merge on, head branches deleted on merge. Rulesets: **main trunk
  (T995)** (id 14082789: no deletion / force push, PR required, merge method `merge`, required checks `docs ok`, `app ok`,
  `feeds worker ok`, `pipeline ok`, admins bypass **only on a PR merge**); **release branches (T995)** (27831606);
  **old dev branch read-only** (27831608) until the production feeds worker dispatches on `main`. Tag
  `archive/redesign-stream-sections` at c374b91fd. Staging feeds worker redeployed with DEFAULT_REF `main`.
- Merge / hook fixes: d4eae5b46; PR #5 (65d5d6843: GIT_DIR stripped in the hook, before_push and the pipeline conftest;
  the hook runs `--no-tier`); PR #8 (the feeds-worker pin resolves paths lexically).
- Pruned 2026-11-22 (lane L13), see Status.

## Left (Claude)
- After the 0.9.0 production feeds worker redeploys from `main`: delete `redesign/stream-sections` and
  `cache/maplibre/fslean-m2`.
- `handoff/PLAN/PARKED-TICKETS.md` and `ios/START-HERE-ios.md` still name the old branch (history pages, left as is).
  actionlint was not run (not installed). Open question for T998: a data-only release after the merge-back runs from
  which branch (app.json's version names an already-merged `release/<v>`)?

## Acceptance
`main` protected with the checks required; a lane's PR merges only green; a release cut makes its branch and tags;
no script or doc names `redesign/stream-sections` outside history.

Depends: T993 G1 (check names), G2 (before-push command). Size: M. Owner: no (decided).
