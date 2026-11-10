# T543 — CI: pinned Python installs, timeouts, cancel superseded runs, path filters match what the jobs read

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the workflows). Rank 11. No GitHub `schedule:` trigger was found
(owner rule holds).

## Today
- **Unpinned Python on CI.** `requirements.txt:2` says install with `-c constraints.txt` (T416: unpinned, "a fresh venv
  got Pillow 13 / shapely 2.2 and its release differed"). Only `scripts/bootstrap.sh:32` does. `app-ci.yml:35`,
  `pipeline-ci.yml:43`, `feeds-hydat.yml:101` install without it; `feeds-runtiming.yml:101` installs a hand-kept unpinned
  list, and that job publishes a feed.
- **No timeout / cancel.** `app-ci.yml:11-17` has neither `concurrency` nor `timeout-minutes`; `pipeline-ci.yml:33-34`
  has no timeout (GitHub default 6 h).
- **Path filters.** `app-ci.yml:5-9` triggers on `app/**` only, but its worker parity tests import
  `pipeline.regs.dfo_salmon.wording` and `pipeline/gauges` through Python; a pipeline-only push to those runs no parity
  test on CI. (`emit --check` and the ledger check are covered elsewhere.)
- `feeds-hydat.yml` checks its key late; the R2 env + staging-only guard is copied across the manual workflows (prod
  switch half belongs to T404).

## Work
1. `-c constraints.txt` on all four installs; `constraints.txt` in both cache keys and path filters; pnpm version read
   from `app/package.json`.
2. `app-ci`: concurrency group with cancel-in-progress, `timeout-minutes: 25`; `pipeline-ci`: `timeout-minutes: 20`.
3. `app-ci` paths gain `pipeline/regs/dfo_salmon/**`, `pipeline/gauges/**`, `requirements.txt`, `constraints.txt`.
4. hydat key check is the first step.

## Acceptance
- Both workflows green on one push; `pip freeze` on CI matches constraints for the pinned set (printed in the run).

Depends: —. Size: S. Owner: no.
