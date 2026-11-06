# T335 — Remove archive/ and general cleanup

Status: DONE 2026-11-06 (chat 3, HASH) — scope approved by the owner (below).

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

## Result (2026-11-06)
- `archive/` deleted (211 tracked files + 17 MB of untracked v1 cache/pycache). `pipeline/docs/archive/`: the 4 docs
  live code cites moved up to `pipeline/docs/` (`REACH-BUILDER`, `HANDOFF-curated-layout`, `HANDOFF-dfo-curation`,
  `06-ui-data-contract`), references rewritten; the rest deleted. **Kept in place:** `waterbody-splits.json`, which
  curated `cuts.json` names (no curated note reworded). Every remaining "archive/…" mention (provenance comments in
  app/pipeline) reads with `git show d1b36ca44:<path>` — said once in README, AGENTS, `pipeline/docs/00-README.md`.
- `audit_split_binding.py` lost its only citation with the archive docs; listed in `pipeline/docs/MAP.md`.
- Cleanup: side builds and scratch of DONE tickets removed (~25 GB; side 34 → 9.9 GB, scratch 1.3 GB → 98 MB). Kept:
  side `t322b`, `t326` (T322/T326 still PARTIAL), `scratch/answers_reference` (a test reads it),
  `scratch/slough_fold_preview.json` (T322). Releases already live + previous. `play-shots.mjs` /
  `build-play-graphics.mjs` are tracked now; `store/graphics/` stays gitignored. The root `__pycache__/` regenerates
  (`project_config` is a root module) and is ignored.
- Proof: the 26 test files that name archive paths (575 pass; the 1 failure is `test_export_codec`'s
  files-on-disk-pair, a data-state test while chat 2's Mac deliver rewrites the bundle), the orphan gate, app typecheck,
  `pipeline build --graph --dry-run` (rc 0).
