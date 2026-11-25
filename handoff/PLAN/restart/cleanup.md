# Restart: the clean-up chat (T1081, chat D)

Orchestrator on Fable; workers by task (Opus for data and tooling, Sonnet for mechanical edits). Branches stay local
until the planning chat lifts the PR hold. Integration branch: `docs/T1081-cut`; each lane works in its own worktree off
it and merges back by cherry-pick or merge, explicit paths in every commit (`type(T1081): subject`).

## Done

- Wave 1: closed tickets archived (`tickets/closed.tsv` + `archive/closed-2026.tar.gz`), plan folders cut,
  `AGENTS.md` 1,022 to 438 lines.
- Wave 2, owner blocks: the owner to-do folder and page are gone; every open ask is an `## Owner` block in its ticket
  (`tickets/OWNER-UNFILED.md` holds the ones with no ticket); `views/OWNER.md` is built from them.
- Wave 2, restart pages: one page per live chat in `handoff/PLAN/restart/`, START-HERE at 30 lines, a guard for both.

## In progress

- Wave 2: the decisions page and `facts.md`, and the README files (`app/`, `pipeline/`, the repo root).

## Next

1. Bring the open tickets to one template and run one review round.
2. Add the size guard (a cap per document kind, in CI).
3. ONE scripted rename commit, `handoff/PLAN/` to `docs/`, tickets at a top-level `tickets/`, and the curation review
   tool's path move (the four legacy restart pages move into `restart/` in it, and `LEGACY_RESTART` goes). Only on the
   planning chat's word: 0.10.0 on production and merged back, and no other PR open.

## Baseline (before the clean-up)

11,153 files, 1,483 markdown files, 143,408 markdown lines. Report the new counts against these at the end.

## Rules for this chat

Never touch `data/**`, `handoff/PLAN/sloughs/marks/`, `handoff/PLAN/curation-review/tidal/owner-decisions.json`,
`handoff/PLAN/play-production-access/`, `handoff/PLAN/repo-move/` or `ROADMAP-after-0.9.0.md`, nor another live chat's
restart page. Delete a file only when nothing surviving (an open ticket, running code, a guard) needs it.
