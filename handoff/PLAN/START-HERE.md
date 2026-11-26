# Start here

A new chat reads, in order:

0. **Reports first, every chat.** `.venv/bin/python -m pipeline reports` (`--since YYYY-MM-DD`, `--only
   worker-prod,worker-staging,testflight,play`, `--no-save`, `--json`; read-only) reads both feeds workers' reports and
   crashes, TestFlight feedback and review state, and Play reviews; each item names the ticket that cites it, or NEW.
   Play closed-testing feedback has no API: ask the owner. **Every new piece of feedback (a report, a crash kind, a
   tester's word, Play feedback) becomes a ticket AND a row in `handoff/PLAN/play-production-access/feedback-log.md`**
   (anonymous tag, no names; T1046). Keep `questionnaire-draft.md` in step with it.
1. `AGENTS.md` (every rule), then `CLAUDE.md`.
2. Your chat's restart page: one short page per live chat (60 lines at most, deleted when its chat ends):
   - `handoff/PLAN/restart/cleanup.md`: the clean-up chat (T1081).
   - `handoff/PLAN/restart/speed.md`: the speed build chat (T1144, T1161, T1135; then T1165, T1159, T1133).
   - `handoff/PLAN/restart/map-engine.md`: the map engine build chat (T1154, the engine seam).
   - Not yet moved into `restart/` (their chats edit them where they are): `handoff/PLAN/RESTART-091-chat.md`,
     `handoff/PLAN/RESTART-repo-move-chat.md`, `handoff/PLAN/RESTART-store-content-chat.md`;
     the iOS release chat is retired (2026-11-25), its hand-over is `handoff/PLAN/ios/RESTART-ios-release-chat.md`.
3. What changed last: the top entry of `handoff/PLAN/STATUS.md`.
4. The work: `handoff/PLAN/views/LEDGER.md` (rows in work; closed rows `views/DONE.md`) and `handoff/PLAN/views/OWNER.md`
   (what only the owner can do), both built by `node app/tools/ledger.mjs` (`--check` says if stale; `--next` / `--claim`
   take a new ticket's number). Tickets are the sole source of work; how they work and the `## Owner` block format:
   `handoff/PLAN/app-integration/tickets/README.md`.
5. Branches (`AGENTS.md` W47): `main` is the protected trunk; work on a short-lived `type/Tnnn-slug` branch from a fresh
   `origin/main`, open a PR with auto-merge, never push to `main`. A release ships from `release/<version>`.
6. The chats, their order and prompts: `handoff/PLAN/ROADMAP-after-0.9.0.md`; iOS: `handoff/PLAN/ios/START-HERE-ios.md`.

Recommend a new chat at natural breaks (a release shipped, a batch done) and /compact before a heavy step; update the
restart page first. Not restart pages: `handoff/PLAN/INBOX.md` (the owner's notes), `handoff/PLAN/OWNER-PHONE.md`.
