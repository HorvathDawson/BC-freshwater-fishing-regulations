# Start here

One page, one pointer (T437): a new chat reads, in order,

1. `AGENTS.md` (every rule), then `CLAUDE.md`.
2. The newest restart page: **`handoff/PLAN/NEXT-SESSION-2026-11-21-ci-chat.md`** (written 2026-11-21 23:25: build/CI
   streamlining chat (T993, T978, T994, T995); wave 1 merged; next: T995 branch switch, wave 2).
3. The top entry of `handoff/PLAN/STATUS.md` (what changed last), and `handoff/PLAN/OWNER-TODO.md` (what only the
   owner can do).
4. Chat length (owner 2026-11-21): recommend a new chat at natural breaks (a release shipped, a batch done) and /compact
   before a heavy step when the context is long. Update the restart page first.
5. The work list: `handoff/PLAN/app-integration/tickets/README.md` (the ledger; tickets are the sole source of work).
6. The branch (T995, `AGENTS.md` W47): `main` is the protected trunk. Work on a short-lived branch from a fresh
   `origin/main` (`feat/`, `fix/`, `ci/`, `docs/`, `chore/` + ticket id), open a PR with auto-merge, never push to
   `main`; a release ships from its `release/<version>` branch. Hooks: `git config core.hooksPath .githooks`.

A second chat working the parked tickets (T667 rest, T686, T602, loose nits) reads `handoff/PLAN/PARKED-TICKETS.md` instead of step 2; iOS: `handoff/PLAN/ios/START-HERE-ios.md`.

iOS hardening (owner reopened it 2026-11-14, own chat, simulator work first): `handoff/PLAN/ios/START-HERE-ios.md`.

When a newer restart page is written, point line 2 here at it and delete the one it replaces (git keeps it). Pages
that are not restart pages: `INBOX.md` (the owner's notes), `OWNER-PHONE.md` (what to watch for in testers' reports),
`ticket-audit-2026-11-05.md` (proposals waiting on the owner), `implement_progress.md` and `IMPLEMENT-HANDOFF.md`
(history still cited from code and tickets).
