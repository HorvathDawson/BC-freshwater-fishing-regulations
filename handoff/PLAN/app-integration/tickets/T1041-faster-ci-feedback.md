# T1041 — A red CI run or a review finding stops costing an hour or two

Status: PARTIAL — 0a44b1c75, 4ed75f4a1, de8268ac1, 49081552b, 0dfb92695 (items 1-7, for 0.9.0), 3d06be883 (review lows, 0.9.1), 43cec978d, 1583fe40c, e4004679f (one file per ledger row / owner item, 0.9.1: PR open, merges after release/0.9.0 is back on main); owner: see ## Owner (after 0.9.0), then Claude: the slug flip and the merge-queue rule; left (Claude): `node app/tools/ledger.mjs --merge origin/main` on chore/T1041-one-file-per-row once release/0.9.0 has merged back, then merge it

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1041 | A red CI run or a review finding stops costing an hour or two: related tests on push, `scripts/before_pr.sh`, a report-only mutation job, merge-queue-ready workflows; the org move after 0.9.0 | PARTIAL — owner: see ## Owner (after 0.9.0); then the slug flip + merge-queue rule | 0a44b1c75, 4ed75f4a1, de8268ac1, 49081552b, 0dfb92695, 3d06be883, 43cec978d, 1583fe40c, e4004679f | owner 2026-11-23; one-file-per-row PR: `ledger.mjs --merge origin/main` after release/0.9.0 merges back |

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
   (his clone's config): old owner to-do. Until then a lane pushes with
   `GIT_SSH_COMMAND="ssh -i ~/.ssh/id_ed25519_personal -o ServerAliveInterval=15 -o ServerAliveCountMax=8" git push`
   (no config changed; this PR's third push did so: 193 s hook, pushed).
   Also: related tests and scripts. A changed scripts/*.py or *.sh (loaded by path or run, never imported) reaches the
   test files that name it, in the hook and in the mutation report (which now mutates scripts/*.py too).

7. **The render-lock mutation test hung on a runner** (coordinator 2026-11-23: #73's CI run 45300843329 failed only
   on `test_review_screen.py::test_the_render_lock_is_pinned_by_mutation`, its child at the 600 s timeout). Unlocked
   pdfium from 8 threads can deadlock as well as raise or crash, and `_render_in_threads` joined with no limit.
   Now its threads are daemons joined against a deadline; one still rendering is reported ("HUNG: n of 8 …") and the
   call returns. The mutation child uses 60 s, prints what hung and `os._exit`s; the outer timeout is 240 s, only a
   backstop. A hang counts as the mutation caught, as a crash already did. New test
   `test_a_render_that_never_returns_is_a_hang_reported_in_time` (proved by mutation). The child returns in under a
   second here (8 errors, no hang). Also `test_a_stopped_tool_gets_a_ctrl_c_first` now stands the child in (its real
   child raced its own start-up under load, a before-PR run 2026-11-23).

## Review lows (0.9.1)
The two low findings of the review of PR #79, fixed in 3d06be883 (lane L4, 2026-11-24):
- **(a) The mutation job was not path-gated.** Each job's first step after the checkout is now
  `python3 scripts/mutation_report.py <py|ts> --base … --head … --gate`: git and the runner's python3 only, nothing
  installed. It works out the same files and lines the part would mutate (`py_changed` / `ts_changed`). When there are
  none, the step sets `run=false`, the job summary says "skipped before the setup (the gate) … (what this part mutates)",
  and the app setup, the tool install and the run are skipped. A gate that breaks gives no answer: its step shows the
  failure (`continue-on-error`) and the part runs as it did before the gate (`if: … != 'false'`), so a broken gate never
  skips a part.
  Pinned: `test_the_gate_skips_a_part_with_no_line_to_mutate_and_says_so` (test_mutation_report.py) and
  `test_the_mutation_jobs_skip_their_setup_when_the_pr_changes_nothing_they_mutate` (test_workflow_pins.py).
- **(b) `scripts/related_tests.py` found only direct importers.** It now also goes ONE LEVEL UP: the test files that
  import a module which imports a changed one.
  - The cap: at most `PY_UP_CAP` = 10 level-up files, inside `PY_CAP` = 25 in all, direct importers first. So the hook
    is never slower than the old 25-file cap allowed.
  - Skipped: a package `__init__` is never followed as the middle (it re-exports into most of the tree). Each one is
    named in the step's note, and so is every level-up file past the cap ("one level up: N more …; 10 taken (at most
    10), M left to scripts/before_pr.sh and the PR's tier: …").
  - Measured on #67's change (decided.py + passage_boxes.py + passage_unlocated.json): 5 -> 15 files, 8 s -> 11 s
    (46 -> 103 tests, CI's markers). The `display` package `__init__` was not followed (named).
  - The mutation report keeps the direct importers (`depth=1`), because each mutant runs its whole list.
  - Tests: `test_one_level_up_reaches_the_tests_of_a_module_that_imports_the_change`,
    `test_one_level_up_has_its_own_cap_and_never_goes_through_an_init` (test_related_tests.py). The cap message is
    updated.
- Mutation-proved: 8 mutants, all killed (no level-up; follow an `__init__`; ignore the level-up cap; the report not at
  depth 1; the gate always "run"; the skip left out of the summary; a gate error escaping; one `if:` dropped).

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

## One file per ledger row and per owner item (0.9.1, roadmap row 2; branch `chore/T1041-one-file-per-row`)

Owner 2026-11-24: "One file per ledger row and per old owner to-do item, with README/old owner to-do built from them." Why: on
2026-11-23/24 a dozen lanes each edited the same three files (the ledger table in `tickets/README.md`, `tickets/DONE.md`,
the 3,500-line the old owner to-do page); nearly every PR hit a merge conflict there and had to merge main and re-run its checks.

### Design (decided before the migration)
1. **A ledger row lives in its ticket file**, as a one-row table right under the Status paragraph: the old table's
   header line, its `|---|` line, and the row, byte for byte as it stood in README.md / DONE.md. No second file per
   ticket: the row and the Status line are closed together (T437), so one file keeps that in one diff; the table form
   reads as the ledger always did and needs no new parser (the same cell split). Every one of the 983 rows had a ticket
   file (the guard already required it): no file had to be made.
2. **An owner item is one file** in `the old owner to-do folder (deleted 2026-11-25, T1081)`: line 1 `order: <number>` (higher = nearer the top; a
   tie goes by file name, so two PRs that both add a top item still never conflict), a blank line, then the item. An
   item is one `## ` section of the old page, word for word (its pictures and references are in its text);
   `_head.md` holds the page's head. The done state stays where it is kept today, on the Owner To-Do page (collection
   `todos`); the file keeps the history text, as the page did. Bullet-level items were measured and not used: of the 74
   sections, 50 are bullets only (with their continuation lines), 1 is bullets with blank lines between, 23 hold prose,
   numbered lists, tables or `###` sub-sections (one of them 1,411 lines) that do not split into items without
   rewording. A section is what one lane
   or one answer round writes, so per-section files remove the conflict seen (every lane adding a section at the top).
3. **The views are built, never committed** (option a): `node app/tools/ledger.mjs` writes
   `handoff/PLAN/views/LEDGER.md` (rows not DONE / DROPPED), `views/DONE.md` (DONE / DROPPED) and `views/OWNER.md`,
   git-ignored; `.githooks/post-checkout` and `post-merge` rebuild them. A committed view conflicts on every merge, and
   a merge driver needs per-clone config that GitHub's merge button does not have. The view a row lands in follows its
   status keyword: T540's "DONE.md holds only DONE / DROPPED" holds by construction, and a row never moves by hand.
   `tickets/README.md` keeps how the system works and the owner questions; its `## Ledger` section is a pointer.
   the old owner to-do page is a pointer page (how to read and add items).
4. **Every reader works from the new source, no guard loosened.** `check-tickets.mjs` reads each file's row (Status
   keyword agreement, DONE hashes, IN REVIEW / PARTIAL steps, the 240-character cap, the commit-subject rule) and the
   owner items (a step's `owner: see ## Owner` must be in one; the dead-path guard covers the items and the
   pointer page), plus new refusals: a row in its file twice or another ticket's row, a row typed into README.md, a
   `tickets/DONE.md`, a `## ` section in the old owner to-do page, a malformed item file. `ledgerIds` (the commit-scope check,
   commitlint) reads the files. `pipeline reports` already read the ticket files (the row is in them now). docs-ci,
   `scripts/ci_paths.py` (docs area), `before_push.py` (docs tests) and `test_workflow_pins.py` name `ledger.mjs` and
   its test. Each check's test was re-pointed and mutation-checked (below).
5. **The migration is a tool, re-runnable**: `--import` splits the old files of a work tree and refuses to write unless
   the round trip holds; `--resolve` carries an old-style side's edits during a merge; `--merge <ref>` is the one
   command for a new-style branch taking a newer main.

### The round trip (`roundTrip`, checked without the import's own parser)
- Rows: the raw row lines of README.md + DONE.md against the rows of the two built views, as sorted lists (normalised:
  the order, by id, and which table a row sits in: 82 closed rows that sat in README.md are in the DONE view). Every
  ticket file minus its row block is its old text byte for byte.
- Owner to-do: the non-blank lines of the old page against those of the built page, in order (normalised: blank lines,
  one between items, none at an item's end; a whitespace-only line counts as blank).
- Counts on main be4df3d6a (the import commit): 244 rows in README.md + 739 in DONE.md = 983 rows in, 983 ticket files
  with their row out (162 in the LEDGER view, 821 in DONE); 74 owner sections in, 74 item files out (+ `_head.md`);
  3,340 owner non-blank lines in and out; nothing dropped, nothing doubled. `ledger.test.ts` re-runs this on the files
  as they stood at be4df3d6a (from git history), so it keeps proving it after the old files are gone.

### Recipes
- **A branch that still edits README.md / DONE.md / the old owner to-do page the old way** (`release/0.9.0`, any open PR):
  `git merge origin/main`, then `node app/tools/ledger.mjs --resolve` (then `node app/tools/check-tickets.mjs` and
  `git commit`; a new ticket on such a branch gets its number's claim from the carry). The carry is 3-way against the merge base: the branch's changed rows, ticket text, added / edited /
  removed sections and its README text (outside the ledger) land in the files; a clash (both sides changed one row or
  one item differently) is left with markers AND unmerged in git's index, so `git commit` refuses it until settled.
- **This branch, right before it merges** (main moved, and `release/0.9.0` has merged back): one command,
  `node app/tools/ledger.mjs --merge origin/main` (merge, carry, the guard, the merge commit; it stops at the first
  thing left to settle by hand).

### Mutation (each killed by the tests named)
- `ledger.test.ts`, 14 mutants of `ledger.mjs`, all killed: closed-row placement, the strip's blank line, the carry's
  row precedence and text side, the round trip's dropped-row and owner-line reports, duplicate headings, a new top
  item's order, id order, the index marking of a clash, README text, DONE.md removal, a removed item, two row tables.
- `check-tickets.test.ts`, 10 mutants of `check-tickets.mjs`, all killed: the README / DONE.md / the old owner to-do page
  refusals, owner-file problems, a file's row problems, the keyword agreement, the owner-text step, the pointer page's
  dead paths, the cap, the commit-subject rule.

### Review of #114, fixed (e4004679f)
1. **No carried conflict can be committed unseen.** Every conflict `--resolve` finds now names a FILE, written with
   markers where there is text to show, and marked unmerged in git's index (stage 2 / 3 from the two sides, or the file
   as written when only the carry makes it), so `git commit` refuses until it is settled: an item edited on the old side
   and removed on the new (the reviewer's T1047 case), the reverse, a row or text changed while the file is gone on the
   new side (a rename). A ticket the old side removes goes with its claim, no conflict.
2. **No old-side section vanishes.** Items are keyed by heading and by place among same-named ones: a second item
   under a used heading is carried as its own file and is a conflict (the guard also refuses the repeated heading); a
   heading both sides renamed (paired by its neighbours or its text) is a conflict, not two silent copies.
3. **Ticket numbers collide in git** (chosen: a claim file per number). `tickets/ids/Tnnn` holds the ticket file's name;
   `node app/tools/ledger.mjs --claim` writes it for a new ticket (983 written now; `--import` and the carry write them
   too). Two PRs that take one number add the same path with different text: an add/add conflict at merge time, proved
   by a two-branch test (a different number merges cleanly). The guard: every ticket has its claim, every claim names a
   ticket of its number; a carried old-side ticket whose number the new side took is a conflict on the claim.
4. **The pointer pages are exact.** the old owner to-do page must equal `OWNER_PAGE_TEXT` and README's `## Ledger` section
   `LEDGER_POINTER` (a bullet appended the old way fails). The pointer page now says how to read `_head.md`'s pre-T1041
   wording (the head itself is kept word for word). RESTART-091 (the 0.9.1 questions item) and
   `play-production-access/README.md` point at item files.
5. **A stale view says so.** `staleViews` / `--check`; `check-tickets.mjs` prints a warning naming
   `node app/tools/ledger.mjs` (never a failure: CI has no views); post-commit and post-rewrite hooks join
   post-checkout / post-merge; the views' banner, the pointer pages and START-HERE say a view may be stale.
6. **Markers refused, orders unique.** A `<<<<<<<` / `>>>>>>>` line in a ticket, an item or the head fails the guard; an
   old-side top addition takes the next number above the current top, a middle one a midpoint no item holds.
Mutation after the fixes: 25 of `ledger.mjs` (the 14 above, plus the reviewer's two uncovered paths and nine more), all
killed; 14 of `check-tickets.mjs`, all killed. Round trip re-run on today's main (75 sections, 3,372 lines in = out) and
`--merge origin/main` re-checked against a fresh `--import` of main: file for file the same, claims included.

### Not done
- Merging: not before `release/0.9.0` has merged back into main (that chat cherry-picks ledger fixes between the two;
  restructuring main under it mid-release would hurt it). Then the `--merge origin/main` command above, on this branch.
- `the old owner to-do's head file` keeps the old page's head word for word, which still says "this file keeps the
  long text" and "README.md is the ledger": its wording is the owner's page, left for the planning chat.
- The RESTART-091 page's row for this item and the memory notes that name the old owner to-do page are the orchestrator's.

## Owner

- [x] 2026-11-25 — Repo move to `canifishthis/canifishthis` (org made 2026-11-25 13:11) → done by you, the transfer and the rename at about 15:15, before the staging run ended (repo-move chat's 15:25 report); the new `GITHUB_DISPATCH_TOKEN` is set on both workers and staging answers ok.
- [ ] 2026-11-25 — Tell the repo-move chat the production worker's check result with the new token (still owed since 15:25), and revoke the old token.
- [ ] 2026-11-25 — The worker token `GITHUB_DISPATCH_TOKEN` expires 2027-11-25 (you: "365 days away is expiry"; your token page says 1 year): re-issue it on the org and put it on both workers before then; the planning chat reminds you two weeks before.
- [x] 2026-11-25 — After the move: your OK on the merge-queue ruleset change → "Queue + restore bypass" (your pick in the dialog; the merge queue is ON, the main ruleset's admin bypass put back, 2026-11-25 about 15:24).
- [ ] 2026-11-23 — Keep the push connection alive: in the main checkout `git config core.sshCommand "ssh -i ~/.ssh/id_ed25519_personal -o ServerAliveInterval=15 -o ServerAliveCountMax=8"` (or `ServerAliveInterval 15` under `Host github.com-personal` in ~/.ssh/config).
- [x] 2026-11-25 — Rename the repo too → "Rename now (Recommended)" (2026-11-25); second collaborator account → "Drop it"; org settings → "I set them as listed".
- [x] 2026-11-25 — When to move → "Yes, move now, re-cut 0.10.0 after" (2026-11-25 15:14). Earlier (2026-11-23): merge queue "Merge queue (Recommended)", free org "Move to a free org (Recommended)", "i think we should do repo move after m0.9.0".
