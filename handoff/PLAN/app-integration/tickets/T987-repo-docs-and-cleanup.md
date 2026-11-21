# T987 — Repo README and docs brought up to date; clean-up pass

Status: POSTPONED (owner 2026-11-21: next release, not 0.8.5).

Source: owner 2026-11-21: "add deferred next release ticket to update readme and docs for repo and clean up stuff. (not
for this release".

## Work
- **README and docs:** bring the repo's README and the docs up to date with what exists now: the top-level README,
  `app/README.md`, `app/MAP.md`, `pipeline/docs/MAP.md`, `pipeline/README.md`, the deploy/release runbooks, and
  `handoff/PLAN` pages that are history rather than live. Fix or remove stale paths, commands and numbers (AGENTS 42:
  every number reproducible or gone).
- **Clean-up:** dead scripts, stale generated files and folders, leftover worktrees and branches already merged, old
  handoff pages (git keeps them), unused tools. Quick importance check per item, no approval list (owner 10-24); git is
  the safety net. Never touch curated data, marks, or the owner's review files.
- Keep the guards green (`pnpm check`, `node app/tools/check-tickets.mjs`, the docs guard).

## Acceptance
A new reader can follow the README to build, test and release; no doc cites a path that does not exist; the clean-up is
listed in the commit messages.
