# T1120 — At the repo move, a fresh clone proves everything can be set up and rebuilt from nothing

Status: IN PROGRESS: light half done 2026-11-25 (clone, installs, hooks, fast tests, web app: PASS; one setup page written); the heavy half is open: fetch, data build first stage, Android and iPhone builds, deploy dry runs.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1120 | At the repo move, a fresh clone proves everything can be set up and rebuilt from nothing | IN PROGRESS — chat 3 (repo move), last step (owner 2026-11-25) | | the heavy half (bootstrap fetch, data build stage 1, both phone builds, deploy dry runs) from fresh-machine.md sections 5-10; the owner confirms the secrets list and the two `.p8` keys |

Source: owner 2026-11-25 (planning chat), verbatim: "and when we do repo to free org swap we want to do a fresh
install maybe just to make sure everythign can be redone properly from fresh install still. if that makes sense."
Related: T1041 (the repo move checklist, roadmap chat 3), T1112 (one worktree setup command: venv, run tokens),
T993 (the release runbook), T1081 (docs lean-out: the setup page is one of its inputs).

Planning chat's reading (not the owner's words): after the move, clone the new repo into an empty folder (on this
Mac, beside the main checkout, which is not touched) and follow only what the repo says. Everything that works today
only because of a file that lives outside git on this machine is a find.

## Work
1. Before the clone, list what is git-ignored or outside the repo and still needed: the Python venv, `app/node_modules`,
   the worker run tokens, signing keys and keystores (`android-dir-holds-debug-keystore`), store API keys, wrangler
   login, `~/.gradle/gradle.properties`, the fetched sources (FWA, the basemap raw file, HYDAT, the synopsis PDF),
   `data/release`, caches, the owner's uncommitted review files. For each: rebuilt by a command, fetched by a command,
   or a secret restored from where.
2. Fresh clone, then in order, each step from the docs only: hooks, Python and JS installs, the fast test tiers, the
   web app running locally, an Android debug build, an iPhone simulator build, a data build from fetched sources as
   far as the first full stage (heavy steps through `scripts/heavy.sh`, one at a time), a `--dry-run` of each deploy
   script. No production step, no parser run (human-only).
3. Every step that fails or needs something unwritten: fix the doc or the script, or report it to the planning chat
   for a ticket. The result is one setup page a new machine can follow, and T1112's command covers the worktree case.
4. Secrets: a short list of which secrets exist and where the owner keeps them (names only, never values, the repo is
   public). The owner confirms he can restore each.

## Acceptance
From an empty folder, the written steps alone reach: tests green, web app up, both phone builds made, the data build's
first stage done, every deploy dry run clean. The list from step 1 has no row marked "unknown".

Depends: T1041 (after the slug flip). Severity: medium (a lost machine today may not be recoverable from the repo;
unknown until tried). Release: chat 3 (repo move), last step. Model: Opus orchestrates, Sonnet lanes for the doc fixes.
Size: M (machine time more than credits). Owner: yes (confirms the secrets list; about 10 minutes).
