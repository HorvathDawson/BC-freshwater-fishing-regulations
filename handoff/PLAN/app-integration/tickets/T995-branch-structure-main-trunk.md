# T995 — Branch structure: `main` is the protected trunk; PRs with required checks; release branches and tags

Status: IN PROGRESS (main chat 2026-11-21; the switch waits until this chat's first lane wave has landed).

Source: owner 2026-11-21, verbatim: "Also we could rethink our git branch structure to have a more real branching
structure ?" and "generally we want to change stuff to be best practice in industry". Dialog answers (all the
recommended options): "main is the trunk", "Release branch at lock", "Merge commit". Earlier the same day (T993):
"PRs + required checks" and "Yes, versioned hook".

## Today (measured 2026-11-21)
GitHub's default branch is `main`, frozen at d6a5ca528 (2026-08-04, 1 commit not in dev); all work is on
`redesign/stream-sections` (7,639 commits ahead). No branch protection, no release tags (5 `archive/*` tags), 364 local
branches (229 merged into dev), 17 remote. ~27 references to the dev branch outside `handoff/` (workflows, README,
`deploy/staging/upload.sh`, the feeds worker's dispatch ref and its tests, `app/packages/core/src/menu/licences.ts`).

## Work
1. **Trunk:** merge `redesign/stream-sections` into `main` (keeping main's one commit), push; `main` is the trunk.
   Leave `redesign/stream-sections` behind an `archive/redesign-stream-sections` tag, then delete the branch once
   nothing names it. Update every code reference and AGENTS.md / memory ("main untouched" and the dev-branch rule end).
2. **Protection on `main`:** PRs only; required checks = the G1 jobs (docs-ci, app-ci gates/vitest/bundles,
   feeds-worker-ci, pipeline-ci) with path-filtered jobs reporting a pass when skipped; no force-push, no deletion;
   merge commits only (the ledger cites lane hashes); auto-delete head branches.
3. **Branches:** short-lived `feat/`, `fix/`, `ci/`, `docs/`, `chore/` + ticket id (e.g. `fix/T994-gauge-history`);
   lanes open a PR (`gh pr create`) and merge when green. Prune merged local and remote branches and merged worktrees.
4. **Releases:** at scope lock the release command cuts `release/<version>`; it builds and ships from there. A bug fix
   lands on `main` first, then is cherry-picked onto the release branch. Every release is tagged by the release command
   (app `v<version>`, data `data/<r-id>`); production steps check the tag.
5. **Pre-push hook:** versioned (`core.hooksPath`), running the before-push command from T993 G2; `--no-verify` is the
   emergency skip.
6. AGENTS.md gets the rule; the restart page and runbooks name `main`.

## Acceptance
`main` protected with the checks required; a lane's PR merges only green; a release cut makes its branch and tags;
no script or doc names `redesign/stream-sections` outside history.

Depends: T993 G1 (check names), G2 (before-push command). Size: M. Owner: no (decided).

## Notes (lane `ci/T995-main-trunk`, 2026-11-22: the code side; the GitHub side is the main chat's)
- **One name per language:** `scripts/ci_paths.py` (`TRUNK`, `REMOTE`, `release_branch()`, `app_version()`, CI's `AREAS`,
  `REQUIRED`; stdlib only) is read by `ci_status.py`, `before_push.py`, `pipeline/release.py` and, through
  `python3 scripts/ci_paths.py trunk | release-branch`, by `fast-fail.sh`, `deploy/lib/build-links.sh`,
  `deploy/staging/upload.sh` (its GH_REF is now the release branch: the run-timing workflow runs that copy).
  TypeScript: `app/packages/core/src/repo.ts` (`REPO_BLOB`, the worker's `DEFAULT_REF`); `test_ci_paths.py` pins the
  two equal and that nothing outside `handoff/` names the old dev branch. `ci_status.py` reads the runs of the branch
  HEAD is on (release branch, PR branch, or main).
- **REQUIRED CHECKS (the four job names for branch protection):** `docs ok`, `app ok`, `feeds worker ok`,
  `pipeline ok`. Each workflow runs on every `pull_request` (no path filter); a `changes` job
  (`.github/actions/changes`, plain `git diff --name-only base...head` via `ci_paths.py github <area>`) gates the real
  jobs with `if:`; the last job `ok` (`if: always()`, needs every job) passes only on success/skipped
  (`ci_paths.py verdict`). `push:` keeps the path filters (= `AREAS`, pinned) and runs only on `main` + `release/**`.
- **Hooks:** `.githooks/pre-push` (= `before_push.py --changed`: only the changed areas vs the merge base with
  origin/main + the working tree; a docs-only push = the ticket guard alone), `.githooks/commit-msg` (T998 stub).
  `before_push.py --hook` prints `git config core.hooksPath .githooks`; `BEFORE_PUSH_SKIP=<reason>` is what the release
  command's own pushes set. Setup line also in README "Getting started".
- **Release command:** `--cut X.Y.Z` (release/X.Y.Z from a fresh origin/main, pushed; refuses an existing branch or
  a tagged version); preflight wants HEAD on `origin/release/<app.json version>` (no branch: says `--cut`); `--push` /
  the push gate push `HEAD:release/<v>`; at ship (stage 7 after the smoke) tags `data/<r-id>` + `v<version>` (first
  ship of the version; a data-only release keeps the old `v` tag), pushes them, records `tags` / `ship_commit`, prints
  `gh pr create --base main --head release/<v> …`; `--prod` refuses missing / moved tags (and a release staged before
  this, which has no `ship_commit`: rerun `--staging` on it to tag). Mutation-pinned in `test_release_command.py`.
  `build-apk.sh` / `build-ios-testflight.sh` push nothing (the versionCode commit lands on HEAD = the release branch,
  pushed with the release's push): unchanged.
- **Left (main chat):** merge the dev branch into `main` (`-s ours` over d6a5ca528), push; branch protection with the
  four checks, merge commits only, no force-push / deletion, auto-delete head branches; `git config core.hooksPath
  .githooks`; archive tag + delete the old branch; prune merged branches/worktrees; memory ("main untouched" ends).
  `handoff/PLAN/PARKED-TICKETS.md` and `ios/START-HERE-ios.md` still name the old branch (history pages, left as is).
  actionlint was not run (not installed). Open question for T998: a data-only release after the merge-back runs from
  which branch (app.json's version names an already-merged `release/<v>`)?
- **Review fixes (branch `review/T995`, 2026-11-22):** AREAS now cover what the tests read, derived and pinned
  (`test_workflow_pins.py`: pipeline area gained every workflow/action, the hooks, deploy/, app/, README, the pytest
  setup and two handoff folders — so app PRs now run pipeline-ci too); HEAD must BE the release branch's tip
  (preflight + upload.sh); tags name the uploaded commit and are judged on origin; `ci_status.py` finds runs by commit,
  and `--strict` (production APK) stops on no green run unless the area is unchanged since one; the pre-push hook
  checks the pushed refs (`--pre-push`); `ci_paths.REPO`.
