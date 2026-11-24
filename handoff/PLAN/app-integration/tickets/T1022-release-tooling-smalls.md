# T1022 — Release-tooling smalls (from the 11-22 restart page's "Next" item 5)

Status: DONE — 5befc9c32 (items 1-3), 4e0ac69e0 (item 4); the pre-push range is T1019 (Earlier status: IN PROGRESS — lane L13 (docs/T539-ledger-cleanup).)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1022 | Release-tooling smalls: is_app_path counts dev tooling, a memory-note reference in release.py, prod_instructions names only PRODUCTION.md, T866 inventory line numbers | DONE | 5befc9c32, 4e0ac69e0 | — |

Source: `handoff/PLAN/NEXT-SESSION-2026-11-22-after-ci-chat.md` Next item 5. The pre-push range
(`merge-base(origin/main,HEAD)..HEAD`) is T1019 (lane L6), not here.
## Work
1. `release_version.is_app_path` counted dev tooling at the top of `app/` (`knip.jsonc`, `layers.json`,
   `vitest.config.ts`) and the data package's fixture release (`app/packages/data/dev/`) as app paths: a `fix` there bumped
   the app version. Now in `NOT_APP`; test pins each, and that `app/package.json` and real data code still count.
2. `pipeline/release.py` hint after a release cited a memory note ("memory feeds-worker-deploy-allowed") and a comment cited
   "memory heavy-jobs-one-at-a-time": a note is not in the repo. Now the facts themselves (staging any time, production
   the owner's; AGENTS 18).
3. `prod_instructions` (and the `--prod` docstring) named only `release/PRODUCTION.md`; the one human runbook is
   `handoff/PLAN/runbooks/release-by-hand.md` (T993), PRODUCTION.md is the host/rollback reference. Both named now; a
   test holds that both files exist.
4. T866 inventory (`handoff/PLAN/audit-2026-11-21/t866-fold-inventory.tsv`): its `old_line` column numbers the files as they
   were on origin/main e1eb198a9 (before the fold), which is not said; the header now says it and how to read them.
## Acceptance
- `pipeline/tests/test_release_version.py` (the `NOT_APP` pins and the runbook-names test) fails with each change reverted.
Depends: —. Size: S. Owner: no.
