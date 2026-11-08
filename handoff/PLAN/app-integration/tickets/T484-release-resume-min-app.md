# T484 — A resumed release forgets the min app version after its promote

Status: DONE — proved 2026-11-08: r-25a89ce9f8e6 resumed after its promote and its checks still read min app 0.5.0. Was: IN REVIEW — 50c2260d9 (found 2026-11-07 releasing r-1b4f54fb6ed9). Proof pending: the next real resumed release.

`python -m pipeline release --staging` stopped at the staging upload (branch not pushed). The rerun re-ran the checks
with the default min app = `default_min_app(ctx.live)` (pipeline/release.py:408) — but LATEST had already moved to the
new, unstamped release, so the default became "any" and the app gate failed (paint/5, salmon/4). Workaround:
`--min-app-version 0.5.0`.

## Do
- On resume, take the min app version from the ledger's `checks.min_app_version` for this release id when present;
  else the release LATEST pointed at before the promote.
- Test: promote, fail staging, rerun → the gate uses the first run's min app.
- Fast-fail (AGENTS 20a): the "branch must be pushed" check belongs in preflight, not stage 6.

## Done (50c2260d9)
- `resume_min_app`: ledger `checks.min_app_version` for this release id, else the stamp of `live_before` (recorded at
  the build stage: LATEST before the promote), else the live stamp. Used by the run and the dry-run plan.
- Test `test_a_rerun_after_the_promote_keeps_the_first_runs_min_app` (unstamped new release, salmon/4, upload fails,
  rerun): fails on the old code with the 11-07 message ("oldest app allowed is any"), passes now; in smoke.txt.
- Push check in preflight (`git_pushed`: fetch + is-ancestor; a failed fetch stops, never passes). upload.sh keeps its
  own check for direct runs.
