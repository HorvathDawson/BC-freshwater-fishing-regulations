# T1112 — A fresh worktree cannot pass the pre-push hook or read the workers' reports: one setup command

Status: OPEN.

Source: store content chat 2026-11-24 23:50 (three of its lanes hit it), and the planning chat and the iOS chat the
same day. Every chat now works in a scratchpad worktree of origin/main, so every chat hits both.

## What was found
- `scripts/before_push.py` (the build-coho step) fails in a fresh worktree: there is no `.venv` there. Lanes
  symlink the main checkout's `.venv` by hand.
- `pipeline reports` prints UNREAD for both feeds workers from a worktree: the git-ignored run tokens
  `app/workers/feeds/.run-token` and `.run-token.prod` exist only in the main checkout. START-HERE step 0 is half
  blind unless they are symlinked by hand. (The commit hook also needs `pnpm -C app install` once per worktree.)

## Work
1. One command, `scripts/worktree-setup.sh` (or a `--setup` on an existing tool): from inside a worktree it finds
   the main checkout (`git worktree list`), links its `.venv` and the two run tokens (read-only use), runs the pnpm
   install, sets `core.hooksPath`, and prints what it did. It never copies a secret and never writes into the main
   checkout.
2. `before_push.py` and `pipeline reports` say exactly that command when they find the venv or a token missing,
   instead of a bare failure or UNREAD.
3. START-HERE step 0 and the chat prompts in the roadmap name the command (one line each).

## Acceptance
In a new worktree: the command, then the pre-push hook passes and `pipeline reports` reads every source. A test
for the missing-venv and missing-token messages.

Severity: medium (every chat loses time; step 0 silently half-read). Release: 0.9.2, tooling. Model: Sonnet.
Size: S. Owner: no.
