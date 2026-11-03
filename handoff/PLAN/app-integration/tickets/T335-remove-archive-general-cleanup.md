# T335 — Remove archive/ and general cleanup

Status: POSTPONED — next credit cycle, together with the T333/T334 audits (owner 2026-10-30: "the clean up was for with the audit for next credit cycle not now"; original: "we can remove archive and do any more general cleanup").

1. Delete `archive/` (211 tracked files, 22 MB: old cron-runner, mobile, pipeline, r2-worker, scripts, webapp) and
   `pipeline/docs/archive/`. First move anything still READ by live code (several app/tools/*.mjs, the feeds worker,
   pipeline/__init__.py and pipeline-ci.yml mention "archive/") to its proper home, or drop the reference.
2. General cleanup: stray root `__pycache__/`, untracked leftovers (`app/tools/build-play-graphics.mjs`,
   `play-shots.mjs`, `store/graphics/` — commit or remove after asking what they are), old side builds and scratch
   under data/generated, releases pruned to live + previous (T331 item 8), dead scripts.
3. Proof: the targeted suites + app typecheck + a dry `pipeline build --graph` still pass.
Run in the next credit cycle with T333/T334. Size: M.

## Scope with the owner FIRST (owner 2026-11-03)
"Make sure the agent that eventually works on that will collaborate with me to really nail down what we want out of
the audit." Before ANY audit work: the session that starts T333/T334/T335 sits down with the owner (one batched
question round, then a written scope in this ticket that the owner approves): what counts as a finding, which areas
are in/out, depth vs credits, what the output looks like (report only, or fixes too), and who decides each fix. No
workflow or agent fan-out until that scope is approved here.
