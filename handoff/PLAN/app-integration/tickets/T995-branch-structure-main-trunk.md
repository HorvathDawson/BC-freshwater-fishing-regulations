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
