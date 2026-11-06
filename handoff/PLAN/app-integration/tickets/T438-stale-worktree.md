# T438 — A locked agent worktree (1.4 GB) under .claude/worktrees

Status: DONE 2026-11-06 (chat 3, no commit: a git-only removal) — from the audit (T333–T335, 2026-11-05). Severity: **low**. Verified by a second reader.

Source: `handoff/PLAN/audit-2026-11-05/` (findings: tests 17; `*.verified.md` has the evidence per finding).

## What happens
Locked today 16:49 by pid 75240; its branch is clean and merged into HEAD.

## Fix
Confirm no session holds it, then `git worktree unlock` + `remove`. Never while a session may use it.

## Acceptance
Each listed defect fixed with a test that fails without the fix (or a recorded "no" with the reason). Owner picks
when; no fix in the audit run.

Size: S. Owner: no (picks the order).

## Result (2026-11-06)
The lock was gone by now (pid 75240 dead); the worktree was clean, its branch fully merged, no process in it, untouched
since 11-05 17:41. `git worktree remove` + `git branch -d` (safe delete). No test: nothing in the repo changed.
