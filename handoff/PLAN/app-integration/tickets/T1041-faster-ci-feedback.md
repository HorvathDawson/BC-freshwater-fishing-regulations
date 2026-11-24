# T1041 — A red CI run or a review finding stops costing an hour or two

Status: PARTIAL — 0a44b1c75, 4ed75f4a1 (items 1-6, for 0.9.0); owner: OWNER-TODO "move the repo to a free organisation" (after 0.9.0), then Claude: the slug flip and the merge-queue rule

Source: owner 2026-11-23 (the 0.9.0 batch chat): a red CI run or a bug found in review cost an hour or two each.

## Why we lost time (checked)
- Both red CI runs of 2026-11-23 were checks the versioned pre-push hook skips (`before_push.py --changed --pre-push
  --no-tier`):
  - **#63** (run 45263371732, head 756e75240, a merge of main): `app/packages/core/src/tabSources.ts` throws on a rule
    index with no `byKey`; ui-native's `WaterScreen.tidal.test.tsx` failed ("regsSourcesOf: the rule index has no
    byKey"). The hook ran only the app tree scanners, never the tests of other packages that import core.
  - **#67** (runs 45233073084, 45239324023): `pipeline/deliver/answers/display/decided.py` and `passage_boxes.py`
    changed; `test_local_closures.py`, which imports `decided` directly, failed in the no-data tier the hook leaves to
    the PR (`--no-tier`: the ~10 min tier outlived git's push connection, SIGPIPE, 2026-11-22).
- Reviews kept finding W46 silent fallbacks and new branches with no test that can fail (rule 33).
- Stacked PRs (#67 -> #63 -> #69 -> #71) each waited, then needed main merged in and another full CI run (~14 min
  pipeline, ~7 min app).

## Owner answers 2026-11-23 (verbatim)
- Local checks: "Both (Recommended)": related tests before each push + full app suite and pipeline tier once before
  opening the PR.
- Mutation in CI: "Report only (Recommended)": on changed lines, Stryker for app TS, mutmut for Python; non-blocking.
- PR chains: "Merge queue (Recommended)".
- Timing: "All now" (built before the 0.9.0 cut).
- Merge queue needs an org (the repo is personal, HorvathDawson, public): "Move to a free org (Recommended)". The
  transfer is the owner's click.
- Later that day: "i think we should do repo move after m0.9.0". So for 0.9.0: items 1-3, and the `merge_group`
  triggers (they change nothing until a queue exists); the move, the slug flip and the merge-queue ruleset are the
  post-0.9.0 follow-up below. No code change that depends on the move.

## Built (0.9.0)
1. **Pre-push: the tests the change reaches** (`scripts/related_tests.py`, `scripts/before_push.py --changed`).
   - App: the tree scanners step becomes `vitest related --run <changed app code> <tree scanners>`. vitest's own
     import graph crosses packages, so a core change runs every package's tests that import it. For any core file that
     is ~550 of ~570 files (core's index re-exports it), ~2 min here: the honest answer, not capped. A changed
     non-code app file (fixture, .gz) and a deleted one are named in the step's note (no graph to follow).
   - Pipeline: the test files that import a changed module **directly** (absolute, `from`, relative imports; AST),
     the changed tests, and for a changed non-Python file under pipeline/ the modules whose source names it. Chosen
     over the transitive closure, which is ~400 of ~550 files for one leaf module (through the package `__init__`s:
     the whole tier). Capped at 25 files (salmon.py's 27 direct importers take 71 s here, common/curated.py's 50 take
     111 s); past the cap the rest are named as left to `before_pr.sh` / the PR. A conftest or a deletion is said, not
     followed. Run as CI runs them: its marker expression, in chunks (`pytest-chunked.sh --only`).
   - The hook's steps now run in **two lanes at once** (Python: the pipeline's; node: the rest), each step's output
     held and printed whole when it ends; `--serial` runs them one after another.
   - Proof it would have caught both: `test_a_core_change_runs_the_tests_of_every_package_that_imports_it`,
     `test_a_pipeline_change_runs_the_tests_that_import_it_as_ci_does`,
     `test_on_this_repo_decided_reaches_the_test_that_failed_on_67`, each proved by mutation; and live, on #63's red
     head (below).
2. **Pre-PR: `scripts/before_pr.sh`**: the whole app vitest suite and the pipeline no-data tier, both through heavy.sh,
   at once (the tier on HEAVY_LOCK, the app on lane 2), one verdict line with both exit codes:
   `before-pr: PASS|FAIL (app suite rc=<n>, pipeline tier rc=<n>, <s> s)`. Documented in AGENTS.md W47 beside the
   push rules. Test: `test_before_pr.py` (stand-in suites; the verdict proved by mutation).
3. **Mutation report**: `.github/workflows/mutation.yml` + `scripts/mutation_report.py`, on every pull request, not
   a required check, `continue-on-error`, the script exits 0 once started (a broken tool is said in the summary and as
   a warning). Only the PR's added lines (`git diff -U0 base...head`):
   - Python: mutmut 2.5.1 `--use-patch-file` (exactly the patch's lines; 3.x mutates whole functions in a copied
     tree), one file at a time against the tests that import it directly (`-x`, CI's markers). Pinned:
     `scripts/mutation-requirements.txt` (every version exact, with constraints.txt).
   - TypeScript: Stryker 10.1.1 (+ vitest-runner 10.1.1, installed on the runner only, the lockfile untouched)
     `--mutate <file>:<from>-<to>` per changed range, in place, against the file's sibling tests and the PR's changed
     app tests through a vitest config that includes only those (each in the project whose globs take it).
   - A changed file with no such test is listed as having none (every mutant would live), not run.
   - Bounds: each part `--budget 900` s (process group stopped at it) under `timeout-minutes: 25`; at most 400 changed
     lines a part, the rest named.
4. **Merge queue ready**: `merge_group:` on the four required workflows (app, docs, feeds worker, pipeline). Their
   `changes` jobs filter a merge group as its pull requests were (`scripts/ci_paths.py` `FILTERED`, the changes action
   reads `merge_group.base_sha` / `head_sha`), so `docs ok` / `app ok` / `feeds worker ok` / `pipeline ok` report on the
   group, never pending; docs-ci's `commits` job stays pull-request only (skipped = a pass). No event is sent until a
   ruleset turns a queue on: nothing changes now. The ruleset and the repo settings are untouched.

5. **The whole app suite no longer exits 1 with every test passed under load** (coordinator 2026-11-23: L21, L22 and
   L23 saw "[vitest-worker]: Timeout calling onTaskUpdate", 8386 passed / 0 failed / exit 1, twice; it would make
   `before_pr.sh` cry wolf). Cause: vitest's worker -> main RPC has a fixed 60 s timeout (birpc's default; vitest 3.2.7
   exposes no setting for it). With the local default of cores - 1 = 17 workers beside other lanes' jobs, the load
   average reached 60-130 on 18 cores; a worker's call went unanswered (or its own blocked loop saw the timer first) for
   60 s, an unhandled error, exit 1. CI had the same on 4 vCPUs and was fixed with `maxWorkers: 2` (2026-11-18).
   Fix: `app/vitest.config.ts` `maxWorkers` = half the cores on a laptop (9 here), CI's 2 unchanged. Nothing is
   hidden: no `dangerouslyIgnoreUnhandledErrors`, no timeout raised. Proof (16 CPU hogs beside the full suite, through
   heavy.sh): old config (17 workers, load 62) -> 1 failed (palette.render.test.tsx timed out at 5 s) + the
   onTaskUpdate error, rc 1, 211 s; new config (9 workers, load 131) -> 795 files passed, no error, 281 s. One run
   each. A real failing test still exits 1, and so does a passing test that leaves an unhandled rejection
   (both checked with throwaway test files). Pinned by `test_the_app_suite_takes_half_the_cores_on_a_laptop`.

6. **The push connection: what actually kills a long hook** (found on this PR's second push, a merge of main: the hook
   passed in 370 s under load, then git died silently, "command terminated abnormally", nothing pushed). Measured with
   a pre-push hook that only sleeps 420 s, on `git push --dry-run` (no ref written), twice at once: plain ssh ->
   "Connection to github.com closed by remote host", rc 141 (SIGPIPE: the 2026-11-22 death); with
   `ssh -o ServerAliveInterval=15 -o ServerAliveCountMax=8` -> the push went through. GitHub drops the idle ssh
   session; a keepalive holds it. This clone's `core.sshCommand` is `ssh -i ~/.ssh/id_ed25519_personal` and ssh's
   settings for `github.com-personal` have `serveraliveinterval 0`. Built: the hook passes its push URL
   (`--remote-url "$2"`) and WARNS (never fails) when neither git's ssh command nor ssh's settings for that host keep
   the connection alive, printing the one-line fix; `before_push.py --hook` prints it too. Setting it is the owner's
   (his clone's config): OWNER-TODO. Until then a lane pushes with
   `GIT_SSH_COMMAND="ssh -i ~/.ssh/id_ed25519_personal -o ServerAliveInterval=15 -o ServerAliveCountMax=8" git push`
   (no config changed; this PR's third push did so: 193 s hook, pushed).
   Also: related tests and scripts. A changed scripts/*.py or *.sh (loaded by path or run, never imported) reaches the
   test files that name it, in the hook and in the mutation report (which now mutates scripts/*.py too).

## Measured (this Mac, 18 cores; GitHub's runners are slower)
- **Hook time**, the same change (one comment in `core/src/tabSources.ts` + one in `display/decided.py`, `--changed
  --no-tier`). Quiet machine, before: **200 s** (11 steps one after another, tree scanners only). Busy machine (other
  lanes' heavy jobs), back to back: before **311 s**, after **279 s** — with the related tests in: `vitest related`
  ran ~550 app test files (173 s) and the pipeline lane added decided.py's 5 importers; the two lanes are what pays for
  it. Still over the ~2 min asked whenever a core file changes (its import graph is the whole suite); a change outside
  core is far smaller. The hook's own limit is git's push connection (~10 min died, 2026-11-22).
- **#63's red head (756e75240), live**: `vitest related` over that push's paths (65 app code files) failed exactly CI's
  two tests, `WaterScreen.tidal.test.tsx` and `WaterScreen.outside.test.tsx` ("regsSourcesOf: the rule index has no
  byKey"), 175 s. The old hook ran only the tree scanners there (passed).
- Mutation report locally: Python, 259f3550e (decided.py + passage_boxes.py, 20 changed lines): 16 mutants, 15
  killed, 1 survived, **67 s**. TypeScript, d27c09e24 (classBadge.ts, words.ts, RegsHeader.tsx, 31 lines): 33
  mutants, 23 killed, 10 survived, **58 s**. CI's time: the PR's own run (below).

## Transfer checklist (the move: after 0.9.0, owner's click)
What names the owner or the repo, or depends on the personal account:
- **Code** (the slug flip, one follow-up commit after the move): `app/packages/core/src/repo.ts` `REPO` (licence links
  `menu/licences.ts` -> `REPO_BLOB`; the worker's workflow starts `workers/feeds/src/dispatch.ts`),
  `scripts/ci_paths.py` `REPO` (`ci_paths.py repo`: `deploy/staging/upload.sh`, `scripts/start-workflow.sh`),
  their tests `app/packages/core/src/menu/legal.test.ts`, `app/workers/feeds/test/dispatch.test.ts`. Stay as they are
  (history): `app/tools/check-commits.test.ts` and `app/tools/commits-contract.json` quote old merge subjects
  ("Merge pull request #12 from HorvathDawson/…"), `app/tools/maplibre-android/archive-t420/upstream/README.md`
  (a fork branch on the personal account). Docs: `handoff/PLAN/runbooks/fresh-machine.md` (the clone URL),
  `T162` (a mention).
- **The feeds worker's GitHub token**: `GITHUB_DISPATCH_TOKEN`, a Cloudflare secret on BOTH feeds workers,
  `bc-fishing-feeds-staging` (`app/workers/feeds/wrangler.toml`) and `bc-fishing-feeds` (production,
  `wrangler.prod.toml`). It is a fine-grained PAT whose resource owner is the personal account: a token reaches only
  its resource owner's repos, so after the move it no longer reaches this repo (HYDAT and run-timing starts stop:
  the worker reports the dispatch failure). Re-issue: GitHub -> Settings -> Developer settings -> Fine-grained tokens
  -> new, resource owner = the organisation (if the org requires approval, approve it: an owner's own request is
  approved automatically), only this repository, Actions: read and write; then
  `npx --prefix app wrangler secret put GITHUB_DISPATCH_TOKEN -c app/workers/feeds/wrangler.toml` and the same with
  `-c app/workers/feeds/wrangler.prod.toml` (production: the owner's); revoke the old token. `runbooks/secrets.md`.
- **Actions secrets** (4, repository level): CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY. GitHub's transfer page: secrets "remain associated after the transfer". Check after. No Actions
  variables, no environments, no deploy keys, no webhooks, no GitHub Pages site (checked with `gh api` 2026-11-23).
- **Rulesets** (3, all `source_type: Repository`: "main trunk (T995)" 14082789, "release branches (T995)", "old dev
  branch read-only until the prod feeds worker dispatches on main (T995)"): repository rulesets live on the repo;
  GitHub's transfer page names protected branches only for PRIVATE repos moving to a free plan (this one is public).
  Expected to move; check with `gh api repos/<org>/BC-freshwater-fishing-regulations/rulesets` after.
- **Redirects**: GitHub redirects the old web and git URLs ("All links to the previous repository location are
  automatically redirected"), so shipped apps' licence links and the owner's remotes keep working, until someone makes
  a new repo at the old name. Update the remote anyway (`git remote set-url origin git@github.com-personal:<org>/…`).
- **Cloudflare / Pages / Play**: the sites are Cloudflare workers deployed from this machine (no GitHub link); R2 is
  reached with the secrets above; Google Play takes uploads from `deploy/staging/play-upload.py` (a service account,
  not GitHub). Nothing there names the repo.
- **gh CLI**: reads the repo from the git remote; nothing else to change.

## Post-0.9.0 follow-up (after the owner moves it)
1. The slug flip: `REPO` in `core/src/repo.ts` and `scripts/ci_paths.py`, `legal.test.ts`, `dispatch.test.ts`,
   `runbooks/fresh-machine.md`; one commit.
2. The merge queue: add a `merge_queue` rule to the "main trunk (T995)" ruleset (merge method: merge commit, the
   four required checks unchanged); lanes then `gh pr merge --merge --auto` into the queue instead of merging main
   into each stacked PR. A ruleset change is the owner's (or his OK).
3. The worker token re-issued (owner, above) and a HYDAT dispatch checked on staging.

- **Mutation jobs on GitHub's runners** (this PR, run 45292340840): python 79 s, typescript 126 s end to end, almost
  all of it setup (checkout, pnpm install, venv, Stryker install): the PR's first diff had no pipeline/ or app source
  line to mutate, so each said so. The mutating runs are measured locally (above); the push after scripts/*.py joined
  the Python part mutates this PR's own scripts: run 45297808690, python **16 min** (the 900 s budget + setup;
  typescript 102 s, nothing to mutate). That run showed three faults, fixed in de8268ac1 and the commit after it:
  one 190-line file took the whole budget (now each file an equal share, smaller changes first); after the stop
  mutmut's results could not be read (now SIGINT first, as Ctrl-C: it keeps what it finished); and mutants a stopped
  run never reached were counted as killed (now `--untested-policy skipped`: "untested", and timeouts are counted).
  Locally after the fixes, the same diff with `--budget 300`: 328 s, killed 20, survived 22, timeout 1, untested 675.
  The report also restores a mutated file from the bytes read before the run, never `git checkout` (which threw
  away an uncommitted local edit once).

## Not done / not verified
- A mutating run of the TypeScript part on GitHub's runners (Stryker's sandbox-free `inPlace` run there).
- The keepalive setting on the owner's clone (his).
