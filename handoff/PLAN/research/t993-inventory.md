# T993 inventory: release, CI and CD processes (read-only audit, 2026-11-21)

Branch `redesign/stream-sections` at 0d15cc5a8 (local; origin at 94c903726). Nothing in the repo was edited, and nothing
was built, released, deployed or uploaded. The raw data is in this scratchpad under `gha/`: the run lists `api-*.jsonl`,
224 failed-step logs in `logs/`, and the signatures in `sigs.json`. The scripts that made the numbers are `summ.py`,
`pushdiff.py`, `sig2.py`, `classify.py` and `streaks.py`.

How the numbers were measured:
- **Minutes** are wall-clock run time (`run_started_at` to `updated_at`) from the Actions API. The repo is PUBLIC, so
  these minutes are not billed. What they cost is waiting, plus red runs that block `ci_status.py`.
- **The 30-day window** was 2026-10-22 to 2026-11-21. There were no runs from 10-22 to 10-28 (the API counts 0), so the
  data covers 10-29 to 11-22 UTC. That is 1,040 runs in total, which matches the API's own `total_count` (1040).

---

## 0. Top findings

1. **The app workflow is 71 % of all Actions time, and 21 % of its time is pushes that changed only `handoff/`.**
   - app-ci measured 701 runs and 7,486 min.
   - Its path filter includes `handoff/PLAN/app-integration/tickets/**` and `handoff/PLAN/*.md`. That way the ticket
     and docs guard runs on a ledger edit, so a red ledger run never stays the newest one and blocks `ci_status.py`.
   - The cost: every STATUS, restart-page or ticket commit runs the whole 17-minute app job (vitest, the web bundle and
     the native bundles).
   - Docs-only pushes were 210 runs and 1,578 min (153 of those runs were cancelled).
   - The guard itself takes seconds. Moving it to its own small job is the largest single saving (section 2).
2. **One failing step hides every later step.**
   - app-ci is one job with sequential steps. When `pnpm check` fails (132 of 134 app failures), the feeds worker
     tests and both bundle builds are SKIPPED.
   - Proven on run 44851899590: "Feeds worker tests skipped".
   - The 0.8.5 worker band-words failure (stop 4) was hidden this way. From 11-21T01:44Z, app-ci was red on dev with the
     T980 fixture error, which 0.8.4 had shipped past with `--no-follow-tests`. The worker failure only surfaced at
     21:35Z, after the mini re-cut cured the first error.
   - In total, app-ci was red on dev for 20.9 h before the release preflight's CI check stopped on it.
3. **`--dry-run --rehearse-follows` cannot rehearse a release before it has been built.**
   - Stages 4 and 5 are rehearsed only once a CANDIDATE exists. With no candidate, the rehearsal checks the app gate
     against LATEST's manifest, which passes.
   - The minimum app defaults to the previous release's stamp (`release.default_min_app`), not to what the formats need.
   - The preflight site gate is asked with no manifest (`site_problems(min_app)` calls `check(None, min_app)`).
   - So the size gate, app-version gate, site gate, binding diff, golden pages and release-backed tests (0.8.5 stops 2,
     3, 5, 7, 8 and 9) can only show up after a 25-minute build.
   - The derivation the gate needs already exists: `app_gate.oldest_reader`, together with `contract.FILES` and
     `app_reads.json`. Section 3 covers this in detail.
4. **heavy.sh does NOT run fast-fail, though five places say it does.**
   - The gate is still parked under `handoff/PLAN/app-integration/T535-heavy-gate/` (T661 item 3).
   - The live `scripts/heavy.sh` never mentions fast-fail beyond one comment (`grep -i fast` finds line 138 only).
   - The claims: `scripts/fast-fail.sh` lines 12-14, `deploy/staging/build-apk.sh:289`, `deploy/staging/site-all.sh:51`
     (it exports `FAST_FAIL_DONE`, which nothing reads), and `pipeline/release.py` lines 23 and 729.
   - A consequence: `deploy/staging/build-ios-testflight.sh` runs no fast-fail, no CI check (`ci_status.py`) and no
     changelog precheck. `build-apk.sh` runs all three.
5. **CI failures cluster in a few avoidable classes.** Counts are of failed runs. The classification is by regex (method
   in section 2), so read it as ±10 %.
   - **Tree-scanning inventory pins:** app 53, pipeline 26. Examples: `tools/failures.test.ts` (17 runs),
     `back-layers` (8), `one-water-screen` (8), `t117_walks` (14), `t121` (7).
   - **Ticket ledger and docs guard:** app 48.
   - **Tests that read machine state CI lacks:** pipeline 34. Examples: `data/generated/inseason/resolved.json`,
     `atlas/full/registry.json`, the export's `grp_runs`, `tippecanoe`.
   - **Other assertions** (stale pins or real defects): app 23, pipeline 14.
   - **A stale committed mini release:** pipeline 4.
   - **Confirmed flakes:** 5 app runs went green on a rerun with no change. 4 were `WaterMode.test.tsx` "across
     waters", 1 was a `WaterScreen.unnamed.test.tsx` 30 s timeout.
   - **Timezone (UTC vs PDT):** 2 to 4.
6. **The release ledger lost 0.8.5's accepted binding diff.** The record for r-741815c6090d says
   `binding_diff: same reach run`, but stop 7 was a real diff (Lost Lagoon +1) that was accepted. This is T788 item 8,
   still live.
7. **The human runbook already exists but is stale.** `handoff/PLAN/runbooks/release-by-hand.md` (2026-11-18, with a
   claude.ai doc twin) is the right base for T993. Section 1.4 lists what is out of date.

---

## 1. Process inventory

Who may run it, according to AGENTS, the script headers and memory:
- **A** = an agent may run it.
- **O** = owner only (a `CONFIRM_PROD` guard, or a credit / identity reason).
- **A\*** = an agent may run it only by a recorded owner go.

### 1.1 GitHub workflows (`.github/workflows/`)

| workflow | purpose / trigger | what it runs | gates / stops on | overlaps | dead or duplicated; CI-only |
|---|---|---|---|---|---|
| `app-ci.yml` ("app") | push + PR on app/**, the bundle schema/indexes, `pipeline/regs/dfo_salmon/**`, `pipeline/gauges/**`, requirements/constraints, `handoff/PLAN/app-integration/tickets/**`, `handoff/PLAN/*.md`; concurrency cancel-in-progress per ref | one job (25 min timeout), with measured step times: checkout with full history (30-37 s), Python venv (34-36 s), **`pnpm check`** (985-1,161 s), **feeds worker vitest** (23-27 s), **web export** (28-32 s), **native export, iOS + Android JS** (54-59 s) | any step; later steps are SKIPPED after a failure | `pnpm check` = fast-fail's `check:fast` step + the release follows' "whole app check"; the worker suite = fast-fail's worker step + `deploy/staging/feeds.sh` + release follows | **CI-only:** the native JS bundle export (otherwise met only inside the APK/iOS builds); the web export at CI time (site-all.sh does it at deploy); the Linux/UTC run (TZ pins). The PR trigger had 0 runs in 30 days |
| `pipeline-ci.yml` ("pipeline") | push + PR on pipeline/**, data/**, config.yaml, project_config.py, requirements, the workflow itself, `app/packages/core/src/*.generated.*`, the mini release, curation-review/**, scripts/**; workflow_dispatch; cancel-in-progress | pip install + freeze (26-34 s), tippecanoe + pmtiles (10-15 s), the curated-paths / match-artifacts / waters checks (≤1 s each), `emit --check` (45-53 s), **pytest no-data tier in ONE process** (621-779 s), `mini_release --check` (10-12 s), the hardcoded-path grep | any step | every cheap step is read by name into fast-fail (`ci_step`); the Tests marker expression is read by `test-env.sh no_data_markers` (fresh copy, fast-fail); the fresh copy runs the same tier | **CI-only:** the Tests tier on a clean Linux runner after the push (the fresh copy asks the same question earlier, once per release); tippecanoe on PATH (a Mac needs it installed) |
| `feeds-runtiming.yml` | run timing publish; workflow_dispatch (started by `deploy/staging/upload.sh` via `scripts/start-workflow.sh`, the button, or the worker); push of its own file (job `if: workflow_dispatch`, so it is skipped) | fetch PSF + NuSEDS only on `refresh`, else the private R2 copy; parse; build; the 10 % shrink floor; publish | the floor, `allow_shrink`, source age > MAX_SRC_DAYS, bucket guard (staging only) | — | the push trigger yields 7 skipped runs (noise only). Failures: 3 network fetch timeouts (11-07, the open.canada.ca outage, T485), 1 `upload-artifact` ECONNRESET failing the run (11-09), 3 startup_failure (11-11) |
| `feeds-hydat.yml` | HYDAT climatology to R2; workflow_dispatch; push of its own file (skipped) | download HYDAT, climatology, upload | — | `scripts/update-hydat.sh` / `hydat-fetch.sh` locally | **no dispatch run in 30 days** (3 skipped push runs); its real use is not verified |
| `staging-security.yml` | button only: `check-public.mjs` (+ optional flood test) against staging R2 | node script | private keys must 404 | — | 0 runs in 30 days. Keep (cheap, owner-launched) |
| `staging-smoke.yml` | button only: `app/tools/smoke-staging.mjs` on staging | headless Chrome | the smoke | **duplicate** of the smoke `deploy/staging/upload.sh` and `site.sh` run locally after every upload and deploy | 0 runs in 30 days. Delete or keep as the button for a human |
| `feeds-sync-inputs.yml` (GitHub registration) | — | — | — | — | **DEAD**: the file was deleted in fd09e65af (T520), but GitHub still lists the workflow "active"; 4 failed runs 11-05..11-07 (`data/release/LATEST` absent on the runner). `gh workflow disable` it |

### 1.2 Local check runners

| process | purpose | entry | what it runs | who | gates | overlaps / dead |
|---|---|---|---|---|---|---|
| `scripts/fast-fail.sh` | the cheap checks before any long job (AGENTS 20a) | `./scripts/fast-fail.sh`; `FAST_FAIL_ONLY=`, `FAST_FAIL_ALL=1`, `FAST_FAIL_BASE=`, `FAST_FAIL_FRESH_COPY_NEXT=1` | 40 steps (list below) | A | exits on the first failure, or lists every failure (`FAST_FAIL_ALL`) | runs `pnpm check:fast` (= `pnpm check` minus vitest), four pipeline-ci steps verbatim, the worker suite, changed-test vitest and pytest. Its header claims heavy.sh runs it: **false** (finding 4). Data-state steps that pass only in the main checkout (marks landed, wrong saves, classified waters, goldens cut from LATEST, override ids name a water, which reads the live registry) also run inside build-apk's worktree (T978 #5) |
| `scripts/test-smoke.sh` (Tier 1) | per-commit smoke | `./scripts/test-smoke.sh` | `pnpm -C app check` + `pytest -m smoke` | A | first failure | `pnpm check` duplicates fast-fail's check:fast + the follows' app check; no worker suite (unlike app-ci) |
| `scripts/test-full.sh` (Tier 2) | per-phase full set | `scripts/heavy.sh test-full 40 -- ./scripts/test-full.sh` | strict `pnpm check`; pytest fast + slow, chunked; card gate; release-perf / placeable / tributary-lines on LATEST; `emit --check` | A (heavy) | lists every failure | the **only runner of `-m slow` and the needs_* data tier** outside a release (T931: ~55 stale pins went unseen because nothing ran it); `emit --check` duplicates pipeline-ci; no worker suite |
| `scripts/fresh-copy-tests.sh` | HEAD cloned, the no-data pipeline tier as CI runs it | release preflight (via heavy.sh), or by hand | `pytest-chunked.sh -m <CI markers>` in a `--shared` clone | A (heavy) | rc | **pipeline only**: no app check, no worker suite, no `mini_release --check`, no emit checks. Overlaps pipeline-ci "Tests" exactly |
| `pnpm check` (`app/package.json`) | every app gate + vitest | `pnpm -C app check` | `check:fast` (25 scripts: tickets, anchors, changelog, boundaries, platform, style, colours, spinner, coho, adipose, splash, gauge-policy, calendar, fish, answers, pack, app-reads, feed-formats, deps, knip, typecheck ×3, promises, inventories) then `vitest run` | A | first failure (`&&`) | **does not run the worker suite** (the worker is not in the workspace); CI 16.4-19.4 min vs "3 to 4 min" on the Mac (AGENTS 18); CI skips 996 of 9,087 tests (release-backed, no release on the runner; run 44851899590) |
| `scripts/ci_status.py` | is CI green on HEAD | called by release preflight, upload.sh, build-apk.sh | `gh run list` for app-ci and pipeline-ci | A | red on the sha, or the newest finished run red, = STOP; running / cancelled / none = WARN | **the reason docs edits trigger app-ci** (a red ledger run must be superseded). A new workflow would have to be added to `WORKFLOWS` |
| `scripts/heavy.sh` | memory-gated lanes for heavy jobs | `scripts/heavy.sh <label> <min %> -- cmd` | waits for free memory, kills at < 6 % or over RAM, `heavy-precheck: yes` before the lock | A | — | **no fast-fail gate** (parked, T661 #3) |

The fast-fail steps, in order:
- conflict markers; tracked node_modules; STATUS size;
- heavy.sh / start-workflow.sh shell tests (when changed); changelog; app-reads `--check`;
- the four pipeline-ci steps; the smoke set derived by rule; no-silent-skips; one curated writer; the wsc scan;
- the mini chain; the source scanners; the review tests; the test-env contract;
- pipeline anchors; the hardcoded-path grep; changed Python compiles; format pairs; curated cut labels;
- the feeds worker; feed formats; DFO vectors; trout vectors; goldens cut from LATEST;
- marks landed; wrong saves; classified waters; override ids;
- `pnpm check:fast`; changed pipeline tests as CI runs them; the /changes budget;
- the review frontend typecheck and backend tests (when changed); vitest on changed app files.

### 1.3 Release and build commands

| process | purpose | entry | what it runs | who | gates | overlaps / dead / notes |
|---|---|---|---|---|---|---|
| `python -m pipeline release --staging` | build once, promote, staging | `.venv/bin/python -m pipeline release --staging [--push]` | 1 preflight → 2 baseline → 3 build → 4 checks → 5 follows → 6 promote → 7 staging upload + smoke | A (memory `staging-r2-deploy-allowed`) | section 4 | the fast-fail check:fast gates run in preflight AND again one by one in follows (T680); the worker suite runs in fast-fail AND in follows; the site gate is asked in preflight (with no manifest) AND in upload.sh (with the manifest) |
| `... --dry-run [--rehearse-follows]` | rehearsal | as above | preflight table + build plan; stages 4-5 only with a CANDIDATE | A | lists every WOULD STOP | **cannot rehearse stages 4-5 before a build** (section 3). Its fast-fail / fresh-copy pass is recorded, so the real run skips them |
| `... --prod <r-id>` | production data | `CONFIRM_PROD=<r-id> ... --prod <r-id>` (TTY, id typed again, no agent env) | `CONFIRM_PROD=canifishthis.ca deploy/prod/upload.sh <r-id>` | **O** | ledger staged + fingerprint unchanged + staging serves the same manifest | the 0.8.5 ledger shows `prod_at` 2026-11-21T17:37 |
| `python -m pipeline build` | the stage graph (atlas → clim/reach/tiles → deliver) | `--dry-run`, `--atlas [--promote]`, `--side DIR`, `--hold-latest`, `--status`, `--force STAGE`, `--jobs N` | each stage as its own process, keyed by inputs | A (heavy) | stage failure | the release calls it with `--hold-latest` through heavy.sh; building alone with `--hold-latest` leaves a CANDIDATE that the rehearsal can then check (this is the missing documented step, section 3) |
| `python -m pipeline.release_follows` | the follows table | — | lists the rows and the test files found by rule | A | — | — |

### 1.4 Deploy scripts

| script | purpose | who | gates | overlaps / dead / notes |
|---|---|---|---|---|
| `deploy/staging/upload.sh` | release to the staging bucket, pointer last, test/ channel, smoke | A | app gate, pieces gate (feeds `/status` dfo.pieces), site gate (`SITE_CHECK_SKIP`), CI (`CI_CHECK_SKIP`), commits on GitHub, run-timing workflow start (or skip, T485) | release stage 7 asks the same refusals first (`staging_prechecks`); the pieces gate is asked only here, after the promote (T788 #2); `FEEDS_URL` comes from the caller's shell (T788 #1) |
| `deploy/staging/site-all.sh` | the one staging site command | A | LATEST manifest present, the /changes budget, scripts parse, clean `app/` (`SITE_DIRTY_OK`) | its comment that heavy.sh ran fast-fail is false; `--relink` when only the app changed |
| `deploy/staging/site.sh` | merge the web export + SEO, noindex, stamp, deploy, smoke | A | the SEO pages built on LATEST (`SITE_RELEASE_ANY`) | **does not refuse a dirty app/ on its own** (`SITE_DIRTY_OK` count 0 in it; T788 #4) |
| `deploy/prod/site.sh` | production site from web-prod + seo-prod | A\* (memory `prod-web-deploy-allowed`, 11-18: Claude deploys after tier + staging smoke); the script header says "THE OWNER RUNS THIS (agents never deploy prod)" | `CONFIRM_PROD=canifishthis.ca`, clean app/ | **no `deploy/prod/site-all.sh`**: production is 3 hand commands (export, SEO via heavy.sh, site.sh) in PRODUCTION.md step 6 and release-by-hand step 7.3. The header and memory disagree on who runs it |
| `deploy/staging/feeds.sh` / `deploy/prod/feeds.sh` | feeds worker: tests, deploy, run every job | staging A; prod A\* (memory 11-21: a release step after prod data + matching site) | worker vitest, `REPORT_SALT`, `NOTIFY_EMAIL_TO`, the pieces gate | the prod header still says "THE OWNER RUNS THIS (agents never deploy prod)" — stale against memory |
| `deploy/staging/build-apk.sh` (`--play`, `--precheck`, `--publish-only`) | sideload APK, or AAB to Play Internal | A (memory `play-upload-anytime`) | the precheck lists everything: changelog, versionCode vs Play, keystores, Play account, wrangler, run token, exactly one patched MapLibre AAR + llvm-readelf, clean app.json, uncommitted app/; then CI, then fast-fail, then the lock | escapes `--no-fast-fail=`, `--no-ci-check=`, `--no-notes=`, `--same-version=`, `--uncommitted=`, `--stock-maplibre=`. In a worktree, the data-state fast-fail steps fail (T978) |
| `deploy/prod/build-apk.sh` | `build-apk.sh --play --production` | **O** (`CONFIRM_PROD=canifishthis.ca`) | as above | memory `maplibre-patched-always`: production uses the patched MapLibre too |
| `deploy/staging/play-upload.py` | Play API: upload, `--max-version-code`, `--promote VERSION_CODE --track alpha` | upload A; promote to Closed A (memory 11-19 `play-closed-track-allowed`); production track O | Play API errors | `--promote` (2026-11-21) is in **no runbook** |
| `deploy/staging/build-ios-testflight.sh` | signed iOS build to TestFlight + version files + `testflight-distribute.mjs` | the header says "THE OWNER RUNS THIS"; in practice the app chat ran it for 0.8.4/0.8.5 (STATUS) | `need` on each secret, x.y.z version; heavy.sh at 20 % | **no CI check, no changelog precheck, no fast-fail** (heavy.sh runs none), unlike build-apk. A shared precheck is the T978 item |
| `deploy/staging/testflight-distribute.mjs` | the external TestFlight group + Beta Review submission | A (dry-run GET-only); real run as the build | Apple API states | tested by `testflight-distribute.test.mjs` via `test_deploy_node_scripts.py` |
| `deploy/prod/upload.sh` (the prod R2 upload) | promote what staging serves; previous pointer kept; prune (T865) | **O** | byte-identical manifest vs staging; `CONFIRM_PROD`; `--bucket <non-prod> --dry-run` rehearsal | run timing is copied from staging; no smoke run here: `node app/tools/smoke-staging.mjs --production` is a separate step |
| `deploy/prod/rollback.sh`, `drop-v1.sh` | rollback; one-time v1 removal | O | fail-closed | — |

Runbooks today: `handoff/PLAN/runbooks/release.md` (data release, every flag), `handoff/PLAN/runbooks/release-by-hand.md`
(the human's whole release, 2026-11-18) and `handoff/PLAN/release/PRODUCTION.md` (first transition + every later
release + rollback). Overlap is heavy, and release-by-hand is stale:
- It says `data/release/PUBLISHED` does not exist (production has been live since 11-19).
- It has no Play Closed step (`play-upload.py --promote`).
- It has no TestFlight external step (`testflight-distribute.mjs`) and does not say that it runs by itself after the
  build.
- It has no "derive min app" (it says to add `--min-app-version` by hand, which was stop 3).
- It shows the prod site, prod feeds and prod data as owner-only. Memory now lets Claude run the prod site and prod feeds
  as release steps.
- It does not say that the rehearsal before a build checks nothing past preflight.

---

## 2. GitHub Actions waste (2026-10-22..11-21; no runs before 10-29)

| workflow | runs | success | failure | cancelled | other | wall min (succ / cancel / fail) | success median / p90 |
|---|---|---|---|---|---|---|---|
| app | 701 | 266 | 134 | 301 | — | 7,486 (4,557 / 1,782 / 1,146) | 16.7 / 21.4 min |
| pipeline | 301 | 154 | 82 | 65 | — | 3,003 (1,854 / 341 / 809) | 11.5 / 16.9 min |
| feeds-runtiming | 31 | 17 | 4 | 0 | 7 skipped, 3 startup_failure | 71 | 2.4 / 2.8 min |
| feeds-hydat | 3 | 0 | 0 | 0 | 3 skipped | 0 | — |
| feeds-sync-inputs (deleted file) | 4 | 0 | 4 | 0 | — | 2 | — |
| staging-security / staging-smoke | 0 | | | | | 0 | — |
| **total** | **1,040** | | | | | **10,562 min ≈ 176 h** | |

Other measurements:
- **Cancelled runs:** 366, about 2,123 min (app cancelled median 4.0 min, pipeline 3.9). Concurrency
  cancel-in-progress is already on in both workflows, so cancels are bursts of pushes, not missing config.
- **Branches:** app ran 97 times on non-dev branches (1,307 min, 21 failed), pipeline 32 (389 min, 10 failed). The lane
  runs earned their keep: on 11-20 lane CI (l8-shrink 16:52Z, l10, l6, lf, r1-084) failed on the T978 machine-state
  tests 5.5 h before dev went red (22:19Z).
- **Push + PR duplicates:** none (0 pull_request runs). The PR triggers are unused.
- **Reruns:** 9 app runs had attempt > 1, and 5 of them went green on the rerun with no change. Those are confirmed
  flakes: `WaterMode.test.tsx` "across waters" ×4 and `WaterScreen.unnamed.test.tsx` timeout ×1. Earlier attempts'
  minutes are not in the totals (the API lists the latest attempt).
- **Red streaks on dev:**
  - app: 40 streaks, median 1.2 h; 6 lasted ≥ 5 h; 107.7 h in total, excluding the first streak.
  - pipeline: 17 streaks, median 1.5 h; 6 lasted ≥ 5 h; 166.7 h in total, excluding the first streak.
  - The first streak (10-29..11-06, 189 h) is the "CI red and nobody saw" period that `ci_status.py` was written for
    (T533).

### What the pushes changed (dev only; `git diff prev_sha..sha` between consecutive runs)

| app-ci runs | cancelled | failure | success | minutes |
|---|---|---|---|---|
| docs-only (every path under `handoff/`) | 153 | 17 | 40 | 1,578 |
| touched `app/` | 77 | 86 | 143 | 3,661 |
| other (no app/, not only handoff: dfo_salmon, gauges, requirements …) | 48 | 9 | 30 | 938 |

pipeline-ci (no docs-only class: `handoff/` is not in its filter): runs that touched app/ too used 1,619 min; the rest
used 978 min.

### Failure causes

Method: the failing step of every failed run comes from the jobs API (224 jobs). The failure lines of every log were
classified by regex, with the first match winning, in this order:
1. ledger;
2. machine state;
3. timezone;
4. inventory;
5. flake;
6. other assertion.

The examples were spot-checked by hand.

| cause | app runs (min) | pipeline runs (min) | examples (run id) | kind |
|---|---|---|---|---|
| tree-scanning inventory / scanner pin (a new file or catch must be listed) | 53 (462) | 26 (268) | `tools/failures.test.ts` ×17, `back-layers` ×8, `one-water-screen` ×8, `regs-wide-layout` ×6; `t117_walks` ×14, `t121` ×7, `fwa_edge_types`, `curated_writer`, `smoke_coverage`, `review_frontend_catches` (42732286766: "3 silent .catch … allowlist says 0") | stale pin; fast-fail runs only some of these, only when it guesses the file is related |
| ticket ledger / docs guard (`pnpm tickets`, `check-tickets.test`, `start-here.test`) | 48 (181) | — | "✗ the ticket ledger is not whole" ×34 runs; "the handoff docs are wrong (T539)" ×4 | process: the guard also reads commit SUBJECTS, so a commit that names a T-id before its row exists passes every pre-commit check and fails on CI |
| machine state / data CI cannot have | — | 34 (323) | `resolved.json` absent (44821162377 …, T978 #3), `grp_runs` in a stale export (T978 #6), `atlas/full/registry.json` (42190092357 ×17), `no build at …/atlas/full`, `tippecanoe` not installed (fixed) | machine state (T978, T777) |
| other assertion: stale pin or real defect | 23 (344) | 14 (155) | `test/cards.test.ts` band words (44952343649), `background.test.ts` file list, `test_t207` Cranberry Canyon `('high','low')` | mixed: mostly pins on data shape; not separable by regex |
| release-shape pin (a fixture lacks what a test needs) | 7 (120) | — | "the regs fixture holds no unnamed stream that a tributary rule closes all year" ×7 (11-21, T980 #2) | stale pin left by the 0.8.4 `--no-follow-tests` escape |
| stale committed mini release | — | 4 (with the next row: 8 runs, 63 min) | "re-cut it: python -m pipeline.tools.mini_release --write" (43821025943, 44438353759) | merge order (T978 #4) |
| other pipeline steps | — | 4 | no-hardcoded-paths ×2, tile tools ×1, emit `--check` ×1 | real defect |
| timezone | 2-4 | — | `tools/clock.test.ts`, `core/src/day.test.ts` ("02:05" vs "09:05") | machine state (the runner is UTC) |
| flake (confirmed by a green rerun) | 5 first attempts | — | `WaterMode.test.tsx` ×4, `WaterScreen.unnamed.test.tsx` timeout ×1 | flake |
| run timing workflow | — | — | 3 network fetch (11-07 outage), 1 artifact ECONNRESET, 3 startup_failure | outside dependency |
| cancelled duplicates | 301 (1,782) | 65 (341) | — | burst pushes |

### Needless triggers and structure

- **app-ci on `handoff/**`:** 210 runs, 1,578 min. The only thing it needs to run there is `pnpm tickets` (seconds).
- **app-ci on `pipeline/regs/dfo_salmon/**`, `pipeline/gauges/**` and requirements:** these need only the worker parity
  suite and the venv; the "other" class was 87 runs, 938 min.
- **One job, sequential steps:** the first failure hides the worker suite and the bundles (finding 2). Each push learns
  one failure per round.
- **Full-history checkout** (`fetch-depth: 0`) costs 30-37 s per run, about 1,002 runs × ~32 s ≈ 530 min. It is needed
  only by `check-tickets.test.ts` (commit subjects) and `test_review_proposals.py` (`git show MARKED_AT`). A partial
  clone (`filter: blob:none`) keeps history without the blobs. The saving is not measured.
- **pytest in one process** on CI (621-779 s): locally it is chunked for memory; on CI the time is the cost. Whether
  pytest-xdist is safe here is not verified (T792 B fork crashes).
- **The deleted `feeds-sync-inputs.yml`** is still registered "active". The push triggers on feeds-*.yml only make
  skipped runs (10). Actions on Node 20 are deprecated (warning in every log: checkout@v4, setup-python@v5).

### Fixes ranked by minutes saved (30-day basis, estimates; "saved" = wall-run minutes)

| # | fix | basis | est. saved / 30 d | size | tickets |
|---|---|---|---|---|---|
| 1 | Move the ledger/docs guard to its own job or workflow on `handoff/**` (tickets + start-here tests + `check-tickets.mjs`, ~1-2 min); drop `handoff/**` from app-ci; add it to `ci_status.WORKFLOWS` | 210 docs-only runs / 1,578 min | ~1,250 min (and ~150 fewer cancels) | S | T993, T791 #2 |
| 2 | Split app-ci into parallel jobs (gates `check:fast`; vitest; worker; bundles) with per-job path filters (worker-only for dfo_salmon / gauges / requirements) and no step masking another | "other" 87 runs / 938 min → worker job ~2 min; masking cost the 20.9 h streak | ~750 min, plus every failure listed in one round | M | T993, T791, T866 |
| 3 | Tests stand on fixtures, never on `data/generated` (autouse sandbox for curated + generated roots; `needs_*` markers by rule) | 34 pipeline runs / 323 min | ~320 min + lane time | M | T978, T777 |
| 4 | Run every tree-scanning inventory/scanner test in fast-fail always (seconds each) and in one before-push command | app 53 (462) + pipeline 26 (268) | up to ~700 min (only the pushes that skipped fast-fail; how many did is not measured) | S | T791 #2, T866 |
| 5 | A commit-msg or pre-push hook running `node app/tools/check-tickets.mjs` (the subject check can only run after the message exists) | 48 runs / 181 min | ~180 min | S (the hook is the owner's call, T791) | T791 #2 |
| 6 | Fix the WaterMode "across waters" flake and the WaterScreen.unnamed timeout | ≥ 7 occurrences (4 + 3 final failures) | ~100 min + reruns | S | T768, T866 |
| 7 | Partial clone instead of full history | ~32 s × 1,002 runs | ≤ 530 min (not measured) | S | T993 |
| 8 | pytest-xdist / sharded vitest | wall time only (pipeline 10-13 min, pnpm check 16-19 min) | wall time, not minutes; unverified | M | T866, T792 B |
| 9 | Pin TZ or make the tests explicit | 2-4 runs | ~40 min | S | T866 |
| 10 | Housekeeping: disable `feeds-sync-inputs`, drop the register-only push triggers, bump actions off Node 20, decide on `staging-smoke.yml`; do not fail run timing on the report artifact upload (say it, keep the publish) | 4 + 10 noise runs; 1 runtiming failure | small | S | T993, T485 |

---

## 3. The stops: which check should have caught each, and why it did not

How the sequence was reconstructed:
- **The 0.8.5 sequence comes from:**
  - the T993 text;
  - `git log` 11-21 (13:36 T985 mouth column; 14:22 version bump + T686 switch; 14:35 mini re-cut; 15:18 size gate
    raised; 15:39 worker card test; 16:16 fast-fail fix; 16:31 fixtures re-cut; 16:39 water-map tests);
  - the ledger record for r-741815c6090d (built 15:29; follows tests 16:56; promoted 17:07; staged 17:33);
  - the app-ci runs.
- **The release's own console output was not read.**

### Why `--dry-run --rehearse-follows` was not enough (`pipeline/release.py` `dry_run`, `rehearse_follows`)

1. **Stages 4-5 are rehearsed only on a CANDIDATE.**
   - With none, `dry_run` prints "no CANDIDATE yet: these are rehearsed on the release once the build has made it (run
     --dry-run again then)".
   - It runs the app gate against LATEST's manifest, which always passes against LATEST's own stamp.
   - The size gate, binding diff, in-season resolve, re-cut and release-backed tests cannot run at all.
   - So a single rehearsal before the build sees only the preflight. A useful rehearsal needs three steps: build (`python
     -m pipeline build --hold-latest`, the same build the release makes), then `--dry-run --rehearse-follows`, then the
     real run. No runbook says so.
   - `release-by-hand.md` describes the rehearsal as "about 15 min, builds nothing" and promises "every stop the real
     run would hit", which it cannot give.
2. **The minimum app is not derived.**
   - `default_min_app(live)` returns the live release's stamp ("does not move unless the human says so").
   - `app_gate.problems` already knows the answer and prints "rerun with MIN_APP_VERSION=<overall>".
   - `oldest_reader(manifest)` exists. `contract.FILES` (the code's formats) and `app_reads.json` would give it BEFORE
     the build (not verified that every switch-dependent format is visible in `contract.FILES`).
3. **The preflight site gate cannot see format needs.**
   - `site_problems(min_app)` calls `site_gate.check(None, min_app)`, so `required()` takes only the stamp.
   - With the derived minimum (item 2), stop 5 would be a preflight stop.
4. **The rehearsal does not run what CI runs.** The fresh copy runs the pipeline tier only. Fast-fail runs `check:fast`
   and changed-file vitest, not the whole app suite, so a red app-ci is known only through `ci_status.py`.

### 0.8.5: nine stops

| # | stop | the check that should have caught it earlier | why it did not |
|---|---|---|---|
| 1 | fresh copy + fast-fail: the committed mini release was cut before the T686 switch flipped | the lane commit that flipped the switch (5dcfffd4c, 14:22) should have run fast-fail's mini chain or `mini_release --check`, both in pipeline-ci and in fast-fail | the switch commit was a release-prep commit (version bump + switch together), so no lane fast-fail ran on it; its pipeline-ci run was cancelled by the next push (21:23Z cancelled). Separately, app-ci had been red since 01:44Z on the T980 fixture (the 0.8.4 `--no-follow-tests` escape), and the re-cut fixed both |
| 2 | size gate: `water` over by 85 KB after a 25-minute build | the T985 #2 lane (6d34b7580, 13:36) adding `water_exit.lon/lat` | no check prices a new column before a build; `gate_problems` reads only a built manifest. The lane measured nothing. A headroom report on LATEST (water 2,594,027 vs a 2,650,000 gate = 2.1 %) would have flagged any column |
| 3 | app-version gate: `--min-app-version 0.8.4` not passed | the preflight / rehearsal | not derived (above, item 2); the default is the old stamp |
| 4 | CI red: the feeds worker test pinned the old band words | fast-fail (worker step) and app-ci | fast-fail had no worker step then (added 2026-11-21); `pnpm check` never runs the worker; app-ci did, but its worker step was SKIPPED while `pnpm check` failed on the T980 fixture: masked from 01:44Z to 21:35Z |
| 5 | the staging site was 0.8.3, under the release's minimum | preflight `check_site` | the site gate had the old stamp (0.8.0 via LATEST) and no manifest (above, item 3) |
| 6 | fast-fail: a step added that day handed worker files to the app's vitest | the fast-fail edit itself (fix in 6fd77d662) | fast-fail's own selftest covers the runner (`FAST_FAIL_SELFTEST`), not each step's file routing; no test fed `vitest_changed` an `app/workers/` path |
| 7 | binding diff: Lost Lagoon +1, expected | nothing earlier: an intended change | T981 (ebbcab1c9) added it on purpose; no way to declare the expected diff with the batch. The ledger then lost the accept (the record says "same reach run"): T788 #8 |
| 8 | golden pages: 9 changed, all renumbered source indexes | the re-cut's renumber/answer split (`RF.answer_paths`) | every change under an `answers` row counts as "what an angler is told"; source-index numbering is not normalised away. Token `e5bfe9a9a1ab` accepted, 39 paths in the re-cut commit 90ee8b657 |
| 9 | 7 water-map tests pinned to the old release's missing mouth place | the T985 lane's tests run on a candidate with the new column | lanes test against LATEST (or the mini/fixture release), never the next release's shape; the tests asserted the column's absence. Fixed in 2ccf1ea45 by making them hold either way (T768's pattern) |

### 0.8.4 stops (T978: four fast-fail stops, six items in the ticket)

| # | stop | should have caught it | why not |
|---|---|---|---|
| 1 | worktree without `cache/`: the AAR lookup came back empty and the build exited with no message | build-apk's precheck ("exactly one patched MapLibre AAR") | the AAR lookup failed silently in that path (a silent exit, against W46) instead of joining the precheck list. Not re-verified in the code |
| 2 | no feeds run token in the worktree; built `--no-email` | the precheck (it does list the run token) | the token file is gitignored (`app/workers/feeds/.run-token`), so a worktree never has it; the precheck could look in the main checkout or say so |
| 3 | `data/generated/inseason/resolved.json` only in the main checkout, so `test_mini_release` raised | pipeline-ci: it did catch it, on lanes from 11-20 16:52Z and on dev at 22:19Z (44828049145) | the build started anyway; fixed by lane W4 (`bundle_fixture.no_notices`) |
| 4 | the committed mini release stale after merges | pipeline-ci "mini release matches a rebuild" (4 failures in the window) | it fails only after a push; merges don't re-cut it; fast-fail's mini chain runs only for some paths (T788 #3 widened them) |
| 5 | "review marks have landed" passes only where the owner's marks are | it should not be in the app build at all | a data check inside `fast-fail.sh`, which build-apk runs in its worktree |
| 6 | a stale live export (no `grp_runs`) failed `test_answers_27` / `test_t801_mainstem_tag` | pipeline-ci (5 lane runs on 11-20) | tests stood on the live export, not on their own fixture; fixed by W4 (`export.read.group_runs`) |

The pattern across both releases:
- Every 0.8.4 stop and 0.8.5 stops 1 and 4 were already on GitHub, or would have been with a clean run, hours earlier.
- What is missing is (a) one place that says "dev is red, and why", and (b) not starting a release or build on a red
  dev.
- Stops 2, 3, 5, 7, 8 and 9 need either the derived minimum or a candidate-shaped check before the build.

---

## 4. Gate glossary (for T992)

| gate | where | what it checks | escape | when the accept/escape is safe | auto-derive / auto-accept? |
|---|---|---|---|---|---|
| committed | preflight | code + `data/curated/` committed (marks.jsonl, closed.json may be dirty) | — | — | — |
| atlas current | preflight | the atlas not behind its curated inputs (T547) | `--ship-stale-atlas REASON` | an input edit that cannot change geometry (wording-only) and the owner agrees | could list per input whether the atlas reads geometry from it |
| marks landed | preflight + fast-fail | every review mark in force is in its curated file (T545) | `NOT_LANDED_OK` dated row | a mark that must wait (owner) | move out of the app build's fast-fail (T978 #5) |
| wrong saves | preflight + fast-fail | no place ships on a "wrong"/"unsure" save without a fix (T786) | `WRONG_SAVE_OK` dated row | owner says ship as is | — |
| classified waters | preflight (corpus) + checks (`--bundle`) | every CW the book prints is in the corpus / bundle as printed (T841) | none (edit BOOK) | — | — |
| pushed | preflight | HEAD on origin/dev | — | — | — |
| CI | preflight, upload.sh, build-apk | app-ci + pipeline-ci not red on HEAD / the newest finished run | `--no-ci-check REASON` / `CI_CHECK_SKIP` | the red is a known flake rerun green, or a docs-only red | after fix #1, docs-only reds cannot block the data release |
| live | preflight | LATEST exists | — | — | — |
| site | preflight + upload.sh | the staging site ≥ the release's min app | `SITE_CHECK_SKIP` | never for a release the site serves | **derive the minimum from the code's formats in preflight**; could redeploy the staging site itself (Claude may) |
| fast-fail | preflight, build-apk | section 1.2 list | `--no-fast-fail REASON` | a failure in a step unrelated to what ships, owner-told | — |
| fresh copy | preflight | the no-data tier on a clean clone (once per tree) | `--no-fresh-copy REASON` | a commit since the pass touched only docs (already auto) | already auto-skips for follows-only commits |
| baseline | stage 2 | LATEST's reach run kept for the diff | `--baseline DIR` | the dir's digest matches (checked) | — |
| manifest / size gates | stage 4 | files present at size; gz ≤ `SIZE_GATES[file]`, offline total, web budgets | none (edit `SIZE_GATES` with a measurement comment) | a measured, intended column (as T985 did) | **print headroom pre-build**; a lane adding a column records its gz delta |
| app gate | stage 4 + upload.sh | every format is read by the oldest app allowed | `--min-app-version X` / `MIN_APP_VERSION` | — | **auto-derive**: default = max(old stamp, `oldest_reader`), printed loudly (raising it strands older apps: say who loses data) |
| binding diff | stage 4 | rule bindings vs LATEST's reach run | `--accept-diff FILE` (content-matched) | every changed rule is one the batch meant | **auto-attribute**: a change whose entry/curated row changed in commits since LATEST's commit passes with a note; keep the record after reruns (T788 #8) |
| in-season resolve | stage 5a | every owner match resolves on the candidate | none | — | — |
| fixture re-cut | stage 5b | renumber-only is committed; a changed golden page stops | `--accept-fixtures TOKEN` | the diff only renumbers ids/indexes, or the changed words are the batch's intent | **auto-accept renumber-only** after normalising source indexes (T992 idea) |
| follow tests | stage 5c | release-backed tests + the whole app check + worker on the candidate | `--no-follow-tests REASON` | almost never: 0.8.4's use (T980) became 0.8.5's stop 1 and a 20.9 h red dev | if used, the release should open the ticket and mark CI as known-red |
| push gate | stage 5d | the release's own commits on GitHub | `--push` | always (it pushes only its own) | make `--push` the default |
| pieces gate | upload.sh, feeds.sh | the worker serves dfo.pieces when the release binds a closed-area piece | — | — | ask in preflight too (T788 #2) |
| staging smoke | upload.sh, site.sh | the live staging serves the release | `SMOKE=0` | — | — |
| site dirty / site release | site-all.sh, prod site.sh | clean app/; SEO on LATEST | `SITE_DIRTY_OK`, `SITE_RELEASE_ANY` | — | add the dirty refusal to staging `site.sh` (T788 #4) |
| review shrink | review apply | a write-through dropping > 10 rows | `--accept-shrink` | only the owner's shrink (W46) | — |
| build-apk prechecks | build-apk.sh | changelog, versionCode, keys, MapLibre AAR, clean app.json, uncommitted | `--no-notes=`, `--same-version=`, `--uncommitted=`, `--stock-maplibre=` | per the script header | share it with the iOS build |
| prod guards | `--prod`, deploy/prod/* | ledger + fingerprint + staging byte-identical + `CONFIRM_PROD` + TTY + no agent env | none | — | — |

---

## 5. Ranked cleanup plan

| # | what | files | size | overlaps |
|---|---|---|---|---|
| 1 | **The ledger/docs guard gets its own tiny CI job**; `handoff/**` leaves app-ci; `ci_status.py` reads it | `.github/workflows/app-ci.yml` (+ a new job or workflow), `scripts/ci_status.py` | S | T993, T791 |
| 2 | **Derive the minimum app before the build** from `contract.FILES` + `app_reads.json`; preflight's site gate uses it; the default stops being the old stamp | `pipeline/release.py` (`default_min_app`, `resume_min_app`, `check_site`, `dry_run`), `pipeline/deliver/release/app_gate.py`, `site_gate.py` | S-M | T993, T788 #2 |
| 3 | **app-ci as parallel jobs**: gates, vitest, worker (its own path filter: workers + dfo_salmon + gauges + requirements), bundles; no masking | `.github/workflows/app-ci.yml` | M | T993, T791, T866 |
| 4 | **The documented three-step rehearsal**: build with `--hold-latest`, then `--dry-run --rehearse-follows`, then run; or `--dry-run --build` doing the first step; the pre-build rehearsal says plainly what it cannot see | `pipeline/release.py`, `handoff/PLAN/runbooks/release.md`, `release-by-hand.md` | M | T791 #3/#4, T993 |
| 5 | **Install the parked heavy.sh fast-fail gate, or delete the five claims**; the iOS build then gets fast-fail; also give it build-apk's CI + changelog precheck (one shared precheck listing every missing link). **DONE b644b39c2 (2026-11-22): claims deleted, gate dropped (T661 says why); both store builds run `deploy/lib/build-links.sh` `build_gates` (precheck with the changelog, CI, fast-fail) before the lock** | `scripts/heavy.sh`, the parked T535 gate (deleted), `deploy/staging/build-ios-testflight.sh`, `build-apk.sh` | S-M | T661 #3, T978 |
| 6 | **Tests never read machine state**: autouse sandbox for the curated + generated roots; data tests by marker; data checks (marks landed, wrong saves, goldens from LATEST, override ids) leave the app build's fast-fail for the release preflight | `pipeline/tests/conftest.py`, `pipeline/tests/anchors.py`, `scripts/fast-fail.sh` | M | T978, T777, T792 B |
| 7 | **One before-push command** = every pipeline-ci step (read from the yml, as `ci_step` does) + `check:fast` + all inventories/scanners + the worker + `mini_release --check`; the hook is the owner's call | `scripts/` (new or fast-fail mode), `scripts/fast-fail.sh` | S | T791 #2 |
| 8 | **Auto-accept what is safe**: renumber-only golden changes after normalising source indexes; binding diffs attributable to commits since LATEST; keep the accept record across reruns | `app/tools/regs-recut/sync.mjs`, `pipeline/release_follows.py`, `pipeline/release.py`, `pipeline/atlas/reach/binding_diff.py` | M | T992, T788 #8 |
| 9 | **Size headroom before the build**: print per-file gz vs gate on LATEST in fast-fail / the rehearsal (warn < 5 %); the release ticket template carries "gate budget for each new column" | `pipeline/deliver/release/build.py`, `pipeline/release.py` | S | T993 |
| 10 | **A lane that changes a format or adds a column re-cuts the mini release and runs the release-backed tests on a candidate-shaped fixture**; tests that assert a column's absence are refused by the scanner | `pipeline/tools/mini_release`, `app/tools/check-test-anchors.mjs` | M | T768, T978 #4 |
| 11 | **One human runbook**: `release-by-hand.md` updated: rehearsal steps; derived min app; Play Closed `play-upload.py --promote`; TestFlight external; prod site/feeds as Claude steps per memory; `PUBLISHED` exists. `release.md` keeps flag detail; `PRODUCTION.md`'s "first transition" archived (git) | `handoff/PLAN/runbooks/*.md`, `handoff/PLAN/release/PRODUCTION.md`, the claude.ai doc twin | M | T993, T987 |
| 12 | **Fix flakes and TZ**: `WaterMode.test.tsx` across-waters (≥7), `WaterScreen.unnamed` 30 s timeout, the clock/day TZ pins | those tests; `app-ci.yml` (TZ) | S | T768, T866 |
| 13 | **GHA housekeeping**: disable `feeds-sync-inputs`; drop the register-only push triggers; actions off Node 20; decide `staging-smoke.yml`; partial clone; run timing not failed by the report artifact upload (said, not hidden) | `.github/workflows/*` | S | T993, T485 |
| 14 | **Stale headers**: prod `feeds.sh` / `site.sh` "agents never deploy prod" vs memory; the staging `site.sh` dirty refusal (T788 #4); `FEEDS_URL` from the shell (T788 #1); `pieces_gate.fetch_status` swallows causes (T788 #1) | `deploy/prod/*.sh`, `deploy/staging/site.sh`, `upload.sh`, `pipeline/deliver/release/pieces_gate.py` | S | T788 |
| 15 | **Trim duplicate runs per release**: the `check:fast` gates run in preflight fast-fail AND one by one in follows (T680); the whole app suite in follows and app-ci; keep one (the follows run is on the candidate, so the preflight one could be gates only) | `pipeline/release.py`, `scripts/fast-fail.sh` | M | T791, T866 |
| 16 | **The live-data tier gets a runner on the candidate** (in chunks), so the ~55 stale pins of T931 cannot accumulate | `pipeline/release_follows.py`, `scripts/pytest-chunked.sh` | M | T931, T791 #4 |
| 17 | **Run timing off the release path** (monthly worker-started refresh) and the smoke's slow-connection prompt | `.github/workflows/feeds-runtiming.yml`, `app/tools/smoke-staging.mjs` | M | T485 step 3, T736 |

## Not verified

- Billed minutes: the repo is public, so the minutes above are wall-clock time, not billing.
- The local durations of fast-fail, the fresh copy and `pnpm check`: not run (no heavy jobs).
- Whether `contract.FILES` reflects every switch-dependent format before a build.
- Whether pytest-xdist or vitest sharding is safe.
- The size of the partial-clone saving.
- 0.8.4 stop 1's silent exit in the code (taken from T978's text).
- The release console logs for either release: not read.
- How many failed pushes skipped fast-fail.
- The failure classification is heuristic (regex, first match wins).
- "Docs-only" was judged per push range (`prev_sha..sha` on dev), so a range that mixed docs with code counts as code.
