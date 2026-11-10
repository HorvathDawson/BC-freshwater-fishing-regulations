# T535 — fast-fail runs every cheap gate, from every long-job entry point, cheapest first

Status: OPEN.

Source: workflow review 2026-11-09 (verified against the code). Rank 3.

## Today
- `app/package.json:7` `check` chains 18 gates; `scripts/fast-fail.sh` runs four by hand (tickets `:54`, colours `:131`,
  answers `:134`, typecheck `:137`) plus changed vitest. Cheap `pipeline-ci.yml` steps it does not run: curated paths
  resolve (`:61-62`), `check_curated --ci` / `--waters` (`:70-71,76-77`), `emit --check` for all generated files
  (`:83-84`). The uncopied ones fail only after the push.
- AGENTS 20a says fast-fail runs before any long job. It is executed in two places only: `pipeline/release.py:133-135`
  and `deploy/staging/build-apk.sh:98-102`. Not by `python -m pipeline build`, `app/tools/testbed/session.sh --build`,
  `scripts/test-full.sh`, `scripts/update-hydat.sh`.
- The review app is outside every gate: fast-fail has no `curation-review` step; the frontend has no typecheck script
  (`curation-review/frontend/package.json:8-9`); `pipeline-ci.yml:19-25` does not trigger on `curation-review/**` or
  `scripts/**` although its own path gate scans them (`:110`). A frontend break is found when the owner restarts the app.
- CI's no-data marker expression is typed twice (`pipeline-ci.yml:92`, `fast-fail.sh:73`).
- `fast-fail.sh` writes CI's path gate to one fixed temp file; two lanes can run each other's half-written copy.
- `release.py:317-352` runs the 1–2 min fast-fail before its instant checks (dirty tree, pushed, LATEST, site gate) and
  again on every resume; `--dry-run` (`:548-590`) omits the site gate that the real preflight stops on.

## Work
1. Measure each `pnpm check` gate and each cheap CI step once; fast-fail runs all but the full vitest suite through one
   shared script (`check:fast`) so the list has one home. Record any gate left out and why.
2. `scripts/heavy.sh` runs fast-fail before waiting for the lock unless nested or the caller already ran it for this
   HEAD (`FAST_FAIL_DONE=<sha>`); printed-reason escape. Every heavy job is then gated, during the wait.
3. Review app: frontend `typecheck` script + a fast-fail step when its files change; backend tests run when
   `curation-review/backend` changes; add `curation-review/**` and `scripts/**` to `pipeline-ci.yml` paths.
4. One home for the marker expression (fast-fail reads it from the workflow, as it does the path gate).
5. Per-run temp file (`mktemp`) for the path gate.
6. Release preflight: instant checks first, fast-fail last, skipped on a resume when it passed for this HEAD with a clean
   tree; `--dry-run` runs the site gate and the app-version gate.

## Acceptance
- A table in this ticket: gate, seconds, in fast-fail yes/no (+ reason). No-change fast-fail ≤ 2 min.
- A seeded break in each newly covered gate fails fast-fail (mutation check, listed).
- `heavy.sh` with a failing fast-fail never takes the lock.

Depends: T533 (shares the preflight edit). Size: M. Owner: no.
