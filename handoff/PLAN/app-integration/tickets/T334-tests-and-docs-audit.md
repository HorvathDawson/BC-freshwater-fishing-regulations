# T334 — Audit all tests and documentation

Status: POSTPONED — (next credit cycle, with T333) (owner 2026-10-30: "add another ticket to audit all tests and
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
