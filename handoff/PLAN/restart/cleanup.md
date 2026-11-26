# Restart: the clean-up chat (T1081, chat D)

Orchestrator on Fable; workers by task (Opus for data and tooling, Sonnet for mechanical edits). Branches stay local
until the planning chat lifts the PR hold. Integration branch: `docs/T1081-cut`; each lane works in its own worktree off
it and merges back by cherry-pick or merge, explicit paths in every commit (`type(T1081): subject`).

## State (2026-11-25 evening)

The cut is ONE local branch, `docs/T1081-lean-out`, ready for one PR (merge commit, not a rebase). It has merged
`origin/main` (T1141-T1148 and the 17:00 owner asks are converted: their lines are `## Owner` lines in T1140, T1146,
T1145, T1041, T1011 and the other tickets named in the conversion list). An independent review found 1 blocker and 9
should-fix items; all are fixed (the archive tool now keeps folders surviving files cite, and a guard keeps that list
true; the research open tickets plan from is back under `handoff/PLAN/research/`). Guards: `node app/tools/check-tickets.mjs`
(includes the size guard), `node app/tools/ledger.mjs --check`, `--archive --dry` (0 folders).

## Next

1. The planning chat lifts the PR hold: push, open the one PR, enqueue it.
2. The RENAME: `scripts/rename_handoff.py` (`handoff/PLAN/` to `docs/`, tickets to a top-level `tickets/`, the four
   legacy restart pages into `restart/`, `LEGACY_RESTART` goes) is rehearsed and waits for the planning chat's word:
   0.10.0 on production and merged back, no other PR open.
3. The curation review tool's path move (with the rename, last; a saved-mark write-through test proven before and
   after) is still to prepare.
4. Unfinished by design: `play-production-access/` still names `owner-todo/` (README.md:56) and `STATUS-archive.md`
   (releases.md:9); the planning chat owns that folder. Live chats' pages (`RESTART-091-chat.md:25`, T1041's Status)
   still name the removed `ledger.mjs --merge`.

## Counts (baseline before: 11,153 files, 1,483 markdown files, 143,408 markdown lines, 1,143 MB)

Now: 6,062 tracked files, 438 markdown files, 38,648 markdown lines, 349 MB. A new chat reads about 819 lines
(README 64, START-HERE 27, AGENTS 438, STATUS 45, this page 37, `views/OWNER.md` about 208).

## Rules for this chat

Never touch `data/**`, `handoff/PLAN/sloughs/marks/`, `handoff/PLAN/curation-review/tidal/owner-decisions.json`,
`handoff/PLAN/play-production-access/`, `handoff/PLAN/repo-move/` or `ROADMAP-after-0.9.0.md`, nor another live chat's
restart page. Delete a file only when nothing surviving (an open ticket, running code, a guard) needs it.
