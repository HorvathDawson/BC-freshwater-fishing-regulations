# T334 — Audit all tests and documentation

Status: IN PROGRESS 2026-11-05 — scope approved by the owner (below); POSTPONED — (next credit cycle, with T333) (owner 2026-10-30: "add another ticket to audit all tests and
documentation").

## Tests (pipeline/tests, app packages, workers, testbed)
- Tests that cannot fail (seeded from their own input — memory checks-seeded-from-input-launder), skipped/xfail left
  behind, pins nobody can explain, duplicates of one check, slow tests that should be fast, fixtures from dead atlases.
- Coverage gaps on the paths that matter: rules → bindings → bundle → app answer; review mark → curated file; feeds.
- Mutation-check the key gates (a deliberate break must fail a test).
## Documentation (AGENTS.md, CLAUDE.md, READMEs, handoff/PLAN, pipeline/docs, data-flow.md, tickets)
- Rules the code no longer follows; docs describing removed paths; duplicate/conflicting docs; stale ticket states;
  one index of what is current.
## Output
Ranked findings (verified by a second reader) → fix tickets; dead docs deleted (git is the safety net).
Depends: T333 (run together). Size: M–L (credits).

## Scope with the owner FIRST (owner 2026-11-03)
"Make sure the agent that eventually works on that will collaborate with me to really nail down what we want out of
the audit." Before ANY audit work: the session that starts T333/T334/T335 sits down with the owner (one batched
question round, then a written scope in this ticket that the owner approves): what counts as a finding, which areas
are in/out, depth vs credits, what the output looks like (report only, or fixes too), and who decides each fix. No
workflow or agent fan-out until that scope is approved here.

## Add (owner asked 2026-11-05, data-and-build chat)
Include what that chat changed, so no doc points at the old shape:
- every doc and runbook against T324's renames (old curated paths: `regulations/entries/catalogue`, `decided/`,
  `splits.json`, `mined_names.json`, `gauges/review.json` …) — grep for each old path, not only docs/;
- the new runbooks (`handoff/PLAN/runbooks/{release,annual-synopsis,fresh-machine}.md`) read cold: can a human follow
  each step with no other context?;
- one place a newcomer starts (root README → AGENTS.md → data-flow.md → runbooks), and which handoff/PLAN files are
  history vs current (NEXT-SESSION-*, IMPLEMENT-HANDOFF, implement_progress, proposed-tickets-*);
- docstrings that describe removed steps (`review.export` files, `match apply` after a rebuild, the lock tool).

## Scope APPROVED (owner 2026-11-05, batched question round)
- **Areas:** everything — pipeline (Python), app + workers (packages, apps/mobile, feeds/data workers, app/tools),
  review app + deploy (curation-review, deploy, scripts, CI), tests + docs (T334).
- **Finding** = a duplicate data type or pathway, a silent fallback, a check seeded from its own input, a test that
  cannot fail, non-determinism, dead code/files, a missing write-time guard, a doc/AGENTS rule the code no longer follows.
- **Output:** a ranked report (file:line, why it matters, proposed fix) → one fix ticket per finding (or per cluster).
  No code changed in the audit run; the owner picks what gets fixed.
- **Depth:** medium — one finder per area × lens, one verifier per finding (~8–10 agents).
- **T335:** delete `archive/` (and `pipeline/docs/archive/`) after moving whatever live code still reads; proof =
  targeted suites + app typecheck + `pipeline build --graph --dry-run`.
