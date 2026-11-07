# T484 — A resumed release forgets the min app version after its promote

Status: OPEN — found 2026-11-07 releasing r-1b4f54fb6ed9.

`python -m pipeline release --staging` stopped at the staging upload (branch not pushed). The rerun re-ran the checks
with the default min app = `default_min_app(ctx.live)` (pipeline/release.py:408) — but LATEST had already moved to the
new, unstamped release, so the default became "any" and the app gate failed (paint/5, salmon/4). Workaround:
`--min-app-version 0.5.0`.

## Do
- On resume, take the min app version from the ledger's `checks.min_app_version` for this release id when present;
  else the release LATEST pointed at before the promote.
- Test: promote, fail staging, rerun → the gate uses the first run's min app.
- Fast-fail (AGENTS 20a): the "branch must be pushed" check belongs in preflight, not stage 6.
