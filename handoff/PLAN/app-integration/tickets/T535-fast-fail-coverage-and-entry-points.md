# T535 — fast-fail runs every cheap gate, from every long-job entry point, cheapest first

Status: IN PROGRESS — aac40d46e, parked gate 3993744e9; left (Claude): install the heavy.sh fast-fail gate (copy handoff/PLAN/app-integration/T535-heavy-gate/heavy.sh + heavy.test.sh into scripts/, commit) once fast-fail is green repo-wide — installing it while T637 keeps fast-fail red would stop every lane's next heavy job.

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

## Built (2026-11-10, aac40d46e; heavy.sh gate parked in 3993744e9)
1. `app/package.json` `check:fast` = every `pnpm check` gate but vitest (`check` = `check:fast && test`); fast-fail runs
   it in one step (it replaced fast-fail's hand-run ledger / colours / answers / typecheck steps). CI's cheap steps run
   from `.github/workflows/pipeline-ci.yml` by their step names (`ci_step`), so the list has one home.
2. heavy.sh fast-fail gate: BUILT AND TESTED, NOT INSTALLED (see "Left"). `handoff/PLAN/app-integration/T535-heavy-gate/`
   holds the gated `heavy.sh` and its shell test (section 12: failing fast-fail = its exit code, job not run, lock never
   taken; `HEAVY_NO_FAST_FAIL=<reason>` printed; `FAST_FAIL_DONE` not HEAD runs it; a passing fast-fail runs while
   another job holds the lock). Release preflight and build-apk.sh / site-all.sh already pass `FAST_FAIL_DONE` /
   `HEAVY_NO_FAST_FAIL`.
3. Review app: frontend `tsc --noEmit -p curation-review/frontend` when a frontend file changes (no package.json
   script: curation-review/ is not this lane's); backend: the `test_curation_review_*` files as CI runs them, one
   process each (~30 s), when `curation-review/backend` changes; `pipeline-ci.yml` triggers on `curation-review/**`
   and `scripts/**`.
4. The no-data marker expression is read from CI's "Tests" step. 5. The path gate (and every CI step) runs from a
   per-run `mktemp` file. 6. Release preflight: instant checks first, fast-fail last, skipped on a rerun for the HEAD it
   passed for (ledger `fast_fail`); `--dry-run` runs the site gate and the app-version gate.
Also: `FAST_FAIL_ONLY=<regex>` reruns chosen steps and says PARTIAL; steps ordered cheapest first; testbed-static test
added to `vitest_changed` when app/apps or ui-native change (T542's ask).

| gate (measured 2026-11-10) | s | in fast-fail |
|---|---|---|
| conflict markers, heavy.sh test (when changed), changelog (T538) | 0 / 10 / 0.1 | yes |
| CI: curated paths resolve, check_curated --ci, --waters, emit --check | 0.1 / 0.1 / 0.5 / 0.3 | yes (ci_step) |
| CI: no hardcoded paths | 2 | yes (ci_step) |
| smoke coverage, silent skips, py compiles, format pairs, goldens | ≤ 1 each | yes |
| marks landed / override ids name a water | 3 / 2 | yes |
| pnpm: tickets, boundaries, platform, style, spinner, splash, gauge-policy, calendar, fish, answers, pack, deps | 0.3–0.5 each | yes (check:fast) |
| pnpm: colours 6.4, promises 4.0, typecheck:seo 3.2, knip 2.1, typecheck 0.4 | | yes (check:fast) |
| pnpm: test (whole vitest) | ~5–17 | NO — Tier 1; fast-fail runs the changed files' tests |
| changed pipeline tests as CI / whole no-data tier with a ship base (T533) | varies / minutes | yes |
| /changes budget | 7 | yes |
| review frontend tsc / review backend tests | 2.5 / 30 | yes, when changed |
| CI "Tests" (whole no-data suite), tile tools | minutes | no (CI; with a ship base: yes) |
No-change total: ~35 s by the steps above (`check:fast` 18 s); one full green "all passed" time was not printable today
(the override-id step is red on purpose until T637).

Proof: mutation in a scratch worktree (`FAST_FAIL_BASE=HEAD FAST_FAIL_ONLY=<step>`), each seeded break caught at its
own step: a ticket file with no ledger row (check:fast), a hand-edited `answers.generated.ts` (emit --check), broken
`area_closures.json` (--waters), a removed curated file (paths resolve), a type error in the review frontend, a
backend that raises on import (backend tests), the CI "Tests" step renamed (marker read). `test_release_command.py`:
fast-fail after the instant checks, once per HEAD with FAST_FAIL_DONE handed to the build, the reason handed to heavy,
dry run runs the site + app gates. Real `release --staging --dry-run` prints both gates.

Not verified: the heavy.sh gate on a live lane (parked); fast-fail's single green total time.
