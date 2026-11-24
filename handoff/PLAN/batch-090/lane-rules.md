# Lane rules (0.9.0 batch, 2026-11-22) — all binding

Repo: /Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations. You never message the owner.

- Read AGENTS.md and CLAUDE.md before changing anything. Prefix shell commands with `rtk` (plain git plumbing is fine
  if rtk mangles it). `graphify query "<question>"` before grepping code. NEVER run the LLM parser (run_parse.sh,
  `python -m pipeline.regs.parsing.dispatch`, anything spawning `claude -p`).
- OWN WORKTREE ONLY: `git -C <repo> fetch origin && git -C <repo> worktree add <scratchpad>/wt-<lane> -b <branch> origin/main`
  where <scratchpad> = /private/tmp/claude-502/-Users-dawson-horvath-dev-personal-BC-freshwater-fishing-regulations/0dd7666c-62a4-47d3-be71-968820062437/scratchpad.
  `pnpm install` in its app/ (commitlint needs it). Missing gitignored links (.venv, node_modules, MapLibre repo, run
  tokens, keystore, data dirs): `deploy/lib/build-links.sh` lists them — symlink from the main checkout, never commit.
  NEVER edit, stash, reset or switch the main checkout (the owner's uncommitted review files live there). Never write
  into the main checkout's data/generated live paths or data/release/LATEST.
- Scope (owner 2026-11-22): small tickets only; if a ticket turns out big or research-heavy, stop it and say so.
  Everything you ADD for review (data rows, names, cuts) is a proposal, never pre-accepted. Never drop/rename/filter data
  content without the owner. A2: back up data/curated/regulations/synopsis/region-*.json before writing, then diff.
  Curated writes via pipeline.common.curated_write.
- Heavy jobs (atlas/side builds, reach, tiles, `-m slow`, full pytest, Gradle, emulator, iOS simulator) ONLY via
  `scripts/heavy.sh <label> 30 -- <cmd>` (lanes: default, `HEAVY_LOCK=~/.claude/heavy-job-2.lock`,
  `HEAVY_LOCK=~/.claude/heavy-job-3.lock`; check `scripts/heavy.sh --status`). Never rebuild/promote the live atlas.
  Kill only PIDs/ports you started; never pkill/killall by pattern; check a port is free before you bind it.
- Tests: only what changed (rtk pytest can false-pass: run gating tests unfiltered, read the exit code);
  `scripts/fast-fail.sh` before pushing. No silent fallbacks (W46). A test must be able to fail (rule 33): prove new
  tests by mutation.
- Commits: Conventional Commits `type(Tnnn): subject`, header ≤100 chars; `git commit -- <paths>` only;
  `node app/tools/check-tickets.mjs` before each commit; never name a Tnnn without its file + ledger row. Update each
  ticket's Status + ledger row in the same PR (DONE names the code commit hashes: commit code first, then the ledger).
  The row is the one-row table under the ticket file's Status line, and an owner item is its own file in
  `handoff/PLAN/owner-todo/` (T1041): never edit `tickets/README.md`'s ledger, a `DONE.md` or `OWNER-TODO.md` for them.
  New tickets: next free number — re-check `origin/main` ticket files AND open PR branches right before committing
  (other lanes add tickets). Commit messages end with "Co-Authored-By: Claude <your model> <noreply@anthropic.com>".
- Push; open PRs (`gh pr create --base main`, title ≤93 chars, body ends
  "🤖 Generated with [Claude Code](https://claude.com/claude-code)"), one per coherent group. Do NOT enable auto-merge or
  merge: each PR gets an independent review first. No deploys (staging or production) unless your brief says so.
  Production is never yours (CONFIRM_PROD is the owner's).
- Pictures for the owner: save under handoff/PLAN/app-integration/<Tnnn>-shots/, never decide a look yourself.
- REPORT (≤15 lines): per ticket outcome (DONE / PARTIAL / stopped-too-big + why), PR URLs, tests + exact results,
  what needs the 0.9.0 data release, owner questions with picture paths, what you did NOT verify.
