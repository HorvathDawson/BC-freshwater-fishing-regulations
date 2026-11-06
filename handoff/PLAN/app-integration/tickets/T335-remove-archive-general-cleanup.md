# T335 — Remove archive/ and general cleanup

Status: IN PROGRESS 2026-11-05 — scope approved by the owner (below); POSTPONED — next credit cycle, together with the T333/T334 audits (owner 2026-10-30: "the clean up was for with the audit for next credit cycle not now"; original: "we can remove archive and do any more general cleanup").

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

## Verified by the audit (2026-11-05, `handoff/PLAN/audit-2026-11-05/review-deploy.verified.md`)
- **archive/:** NOTHING live opens, imports or globs into it (CI and tools included); every hit is a comment, a test
  path exclusion or `--exclude-dir=archive` (`pipeline-ci.yml:108`). Delete it; reword the comments that name it.
- **pipeline/docs/archive/:** move up to `pipeline/docs/` first the 3 docs live code cites (`REACH-BUILDER.md`,
  `HANDOFF-curated-layout.md` §8, `06-ui-data-contract.md`) and `classify.py:17`'s policy source; and
  `waterbody-splits.json`, which the CURATED file `data/curated/waters/cuts.json:10023,10045` names as its research
  source — move the file and keep the path working, never reword curated notes without asking the owner.
- AGENTS header + rule 37 cite archive/: rewrite with T437.
