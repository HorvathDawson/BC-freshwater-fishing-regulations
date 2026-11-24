# Start here

One page, one pointer (T437): a new chat reads, in order,

0. **Reports first, every chat** (owner 2026-11-22: "always look for more reports on Apple external test, feed
   worker, etc."). One read-only command (T1006) reads every source: both feeds workers' reports + crashes (GET only),
   TestFlight tester feedback + the external review state, and Play reviews:
   `.venv/bin/python -m pipeline reports` (`--since YYYY-MM-DD`, `--only worker-prod,worker-staging,testflight,play`,
   `--no-save`, `--json`). Each item says the ticket that cites its id, or NEW. Anything NEW becomes a ticket that
   writes the report id / crash hash in it; then add a new "Store checks" line to `OWNER-TODO.md`. The last-check time
   is kept in `cache/reports-last-check.json` (git-ignored; moved only when every source was read). Play closed-testing
   feedback has no API: ask the owner whether any came in.
   **Every new piece of feedback (a report, a crash kind, a TestFlight item, Play testing feedback, anything a tester told
   the owner) becomes a ticket AND a row in `handoff/PLAN/play-production-access/feedback-log.md`** (anonymous tag, no
   names; T1046, the production-access record, until Google approves). Keep `questionnaire-draft.md` in step with it (Q3, Q7, Q8); each release updates it (runbook Step 8).
1. `AGENTS.md` (every rule), then `CLAUDE.md`.
2. The newest restart page: **`handoff/PLAN/NEXT-SESSION-2026-11-22-evening.md`** (written 2026-11-22 20:15: the 0.9.0
   batch chat, stopped early for credits; open PRs, lanes' State lines in tickets, then the question round and the cut).
3. The top entry of `handoff/PLAN/STATUS.md` (what changed last), and `handoff/PLAN/OWNER-TODO.md` (what only the
   owner can do).
4. Chat length (owner 2026-11-21): recommend a new chat at natural breaks (a release shipped, a batch done) and /compact
   before a heavy step when the context is long. Update the restart page first.
5. The work list: `handoff/PLAN/app-integration/tickets/README.md` (the ledger; tickets are the sole source of work).
6. The branch (T995, `AGENTS.md` W47): `main` is the protected trunk. Work on a short-lived branch from a fresh
   `origin/main` (`feat/`, `fix/`, `perf/`, `test/`, `ci/`, `docs/`, `chore/` + ticket id; commits and PR titles `type(Tnnn): subject`), open a PR with auto-merge, never push to
   `main`; a release ships from its `release/<version>` branch. Hooks: `git config core.hooksPath .githooks`.

**After 0.9.0:** the chats, their order and the prompts to paste are in `handoff/PLAN/ROADMAP-after-0.9.0.md` (owner 2026-11-24).

A second chat working the parked tickets (T667 rest, T686, T602, loose nits) reads `handoff/PLAN/PARKED-TICKETS.md` instead of step 2; iOS: `handoff/PLAN/ios/START-HERE-ios.md`.

iOS hardening (owner reopened it 2026-11-14, own chat, simulator work first): `handoff/PLAN/ios/START-HERE-ios.md`.

When a newer restart page is written, point line 2 here at it and delete the one it replaces (git keeps it). Pages
that are not restart pages: `INBOX.md` (the owner's notes), `OWNER-PHONE.md` (what to watch for in testers' reports),
`ticket-audit-2026-11-05.md` (proposals waiting on the owner), `implement_progress.md` and `IMPLEMENT-HANDOFF.md`
(history still cited from code and tickets).
