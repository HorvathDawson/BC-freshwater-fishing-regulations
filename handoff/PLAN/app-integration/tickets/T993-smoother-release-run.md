# T993 — Document and clean up every release / CI / CD process, so a HUMAN can release by following it

Status: IN PROGRESS (main chat 2026-11-21: owner picked it for this chat; inventory first). Plan item 11 (the one human runbook) written 2026-11-22, IN REVIEW: owner: OWNER-TODO "To read (you): the one release runbook and the gates page"; the other open items are their lanes' (T866, T978, T987, item 17).

Source: owner 2026-11-21, during the 0.8.5 staging release, verbatim: "we need to have a ticket to make the review
process smoother we always have so many issues we need to fix...".

## Evidence (0.8.5 staging, 2026-11-21): nine stops, each found only by the run before it
1. Fresh copy + fast-fail: the committed mini release had been cut before the T686 switch flipped.
2. Size gate: `water` was over its gate by 85 KB (T985 #2 mouth place). Found after a 25-minute build.
3. App-version gate: `--min-app-version 0.8.4` was not passed, though T686 always needed it.
4. CI red: the feeds worker test pinned the old band words. `pnpm check` doesn't run the worker suite.
5. The staging site was app 0.8.3, under the release's minimum app.
6. Fast-fail: a step I added handed worker files to the app's vitest.
7. Binding diff: Lost Lagoon +1, expected.
8. Golden pages: 9 changed, all renumbered source indexes.
9. Release-backed tests: 7 water-map tests were pinned to the old release's missing mouth place.

## Owner 2026-11-21 (scope widened)
Verbatim: "In the same ticket we want to make it so a human could release following a well documented process with a
lot of those checks handling the items that need to be done etc. the ticket should be about documenting and cleaning up
all the processes ci/cd etc etc".

The goal: a person with no Claude session can cut and ship a release by following one written process. The checks do
the remembering: each one either does the step or says exactly what to do next.

### Scope
- **One release runbook** (for a human): every step from "batch done" to "on production". It covers:
  - the changelog and version bump;
  - format switches and the mini re-cut;
  - data to staging and its smoke;
  - the staging site;
  - Play Internal, then Closed; TestFlight internal and external;
  - production data, the production feeds worker, and the production website.

  Each step gives the one command, what success looks like, and what to do on each failure. It names who may run each
  step (owner-only steps marked).
- **Inventory and clean up every process** behind it:
  - CI workflows (app-ci, pipeline-ci and any others);
  - fast-fail, fresh-copy and `pnpm check`;
  - `pipeline release` and `pipeline build`;
  - site-all.sh and site.sh;
  - build-apk.sh and play-upload.py;
  - build-ios-testflight.sh and testflight-distribute.mjs;
  - the feeds-worker deploy and the prod R2 upload.

  For each, cover: what it is for, what overlaps, what is dead, and what only CI runs (e.g. the worker suite).
  Merge or delete what is duplicated.
- **Checks that handle items, not just stop:** derive what can be derived (the minimum app from the formats), do what
  is safe (re-cut, redeploy the staging site), and list every remaining item at once with its command.
- The release ticket template gets the checklist (see Work below).

## Work (to scope when picked up)
- **One rehearsal that lists EVERYTHING at once**, run before the real release. `--dry-run --rehearse-follows` exists
  (T680); find out why it was not enough, and make it the step before every release. It should include:
  - the candidate build and its size gates;
  - the app/site version gates, with the minimum app derived from the formats (no flag to remember);
  - CI parity: the worker suite, as fast-fail now does;
  - the binding diff;
  - the fixture re-cut;
  - the release-backed tests.
- **Lanes test against the next release's shape.** A lane that changes a format or adds a column re-cuts the mini
  release and runs the release-backed tests on a candidate, so its tests don't pin the old release (stops 1, 2, 9).
- **Auto-accept what is safe.** A golden re-cut that only renumbers ids/indexes, and a binding diff that only names
  entries the release adds on purpose, pass with a printed note (see T992).
- **Release prep checklist** for the release ticket: flip switches, re-cut the mini release, staging site at the new
  app version, the gate budget for each new column.

Depends: T992 (the explanation) folds into the runbook. Size: L. Owner: reads and tries the runbook.

## Owner 2026-11-21 (new chat after 0.8.5)
Picked for this chat with T978, verbatim: "Do t993 and t978 and look for other build/ audit related tickrts that match
this which should go into this chat too. Would be nice to make everything more streamline and fast so future tickets are
easier to work on for releases. This won't lead to a full release yet. The real release 0.8.6 will want more stuff in it
but this is for this chat". No release is cut in this chat.

## Owner 2026-11-21, second answer: what else this chat takes
All four offered groups: release gates (T791 rest, T788 rest), test isolation (T777, T792 B, T768 rest), cleanup (T987,
T661 item 3), faster suites (T866, T931, T736, T485 step 3). Plus, verbatim: "You could also get an agent to do a
basic audit for non standard practices or issues with the flow of data etc. we want to make our code production
level. Also clean up gha workflows and make sure we just waste less time with bad ci/cd of failed runs." The audit is
T994; the GitHub Actions cleanup (fewer failed and wasted runs) is in this ticket's inventory.

## Inventory done 2026-11-21 (read-only agent)
Report: `handoff/PLAN/audit-2026-11-21/t993-inventory.md` (process inventory, 30-day GitHub Actions waste, every 0.8.4 /
0.8.5 stop traced to the check that should have caught it, the gate glossary for T992, a 17-item ranked plan).
Headlines: app-ci is 71 % of Actions time; 210 of its runs (1,578 min) were docs-only pushes run for the ticket guard;
one failing app-ci step skips the worker suite and bundles (it hid 0.8.5 stop 4 for 20.9 h); the rehearsal sees stages
4-5 only on a built candidate and the minimum app is never derived (stops 2, 3, 5, 7, 8, 9 were post-build only);
heavy.sh does not run fast-fail though five places say it does.

Lanes (plan item numbers):
- **G1 GitHub workflows:** 1 (ledger guard own job), 3 (app-ci parallel jobs, no masking), 12 (flakes, TZ), 13
  (housekeeping, partial clone); with T994 findings 6, 7.
- **G2 release command:** 2 (derive minimum app pre-build), 4 (three-step rehearsal), 7 (one before-push command,
  T791 #2; the hook stays the owner's call), 8 (auto-accept renumber-only + attributable diffs), 9 (size headroom),
  15 (no duplicate runs), 16 (data tier on the candidate, T791 #4); T788 #2, #8, #10.
- **G3 deploy gates:** 14; T788 #1, #4, #5, #11; T994 findings 9, 10, 18, 19, 20, 28.
- **After T978 lands:** 5 (heavy.sh gate, T661 #3: DONE b644b39c2, the gate dropped and its claims deleted; the iPhone
  build runs build-apk's gates through `deploy/lib/build-links.sh`), 6 (tests never read machine state: T777, T792 B), 10 (T768), T866,
  T987, then 11 (the one human runbook + the T992 explanation), last, once the code settles; 17 with T485 / T736.

## Owner 2026-11-21, dialog (best practice)
Owner, verbatim: "generally we want to change stuff to be best practice in industry".
- **How work lands:** "PRs + required checks (Recommended)": every lane opens a GitHub PR into
  redesign/stream-sections; branch protection requires the CI checks green before it merges; agents open and merge the
  PRs once green. Built after G1's job split lands (the required check names come from it).
- **Pre-push hook:** "Yes, versioned hook (Recommended)": a hook checked into the repo (`core.hooksPath`), so every
  checkout and worktree gets it, running G2's before-push command; `git push --no-verify` is the emergency skip.

## GitHub Actions cleanup (main chat, 2026-11-21; owner: "clean up the old workflows and old gha. We want a nice clean repo")
- 8,399 runs of removed workflows (update-hydro 7,651, update-in-season 731, update-stocking 13, feeds-sync-inputs 4)
  being deleted (throttled background script); runs of live workflows kept. `feeds-sync-inputs` disabled.
- Caches: 36 stale deleted (10.4 GB → 1.24 GB, 4 left). Artifact and log retention 90 → 30 days.
- Owner dialog: staging-security.yml + staging-smoke.yml "Delete both (Recommended)" (they never ran; the deploy
  scripts run the same checks) — deleted after G1 merges. Copilot agent "No, remove it (Recommended)": its environment
  and one run deleted; turning the agent off in the repo's Copilot settings is the owner's (not reachable by API).

## Lane G1 (GitHub workflows), branch lane/t993-g1-gha
Commits: A = 8c1228707 (flake), B = 77ee9ca7d (workflows), C = 8a89a73a9 (data-path fixes).

| item | what | commit |
|---|---|---|
| plan 1 | new `docs-ci.yml` (job `ledger`: `pnpm run tickets` + the guard's two test files) on `handoff/**`, AGENTS.md, the guard's own files; `handoff/**` left app-ci; `scripts/ci_status.py` WORKFLOWS now app, pipeline, docs, feeds-worker (+ tests) | B |
| plan 3 | app-ci = three parallel jobs `gates` / `vitest` / `bundles` (shared `.github/actions/app-setup`); worker suite = new `feeds-worker-ci.yml` with its own path filter (worker, core package it imports, dfo_salmon, gauges, requirements, fixtures, lockfile) | B |
| plan 12 | `WaterScreen.unnamed` timeout: cause found and fixed. `WaterMode` across-waters: already fixed by T849; every logged CI failure predates it; I could not reproduce it again. TZ: `TZ: America/Vancouver` set on every app job (vitest.config.ts already pins it for tests; worker config had no pin and passes under UTC and Vancouver) | A, B |
| plan 13 | `feeds-sync-inputs`: see "owner" below; register-only push triggers dropped (hydat, runtiming); checkout@v5, setup-node@v5, setup-python@v6, upload-artifact@v6, pnpm/action-setup@v5 (all Node 24); partial clone (`filter: blob:none`, full history) in app gates, vitest, docs, pipeline-ci; runtiming report upload is `continue-on-error` and a `::warning::` step + summary line says so | B |
| T994 6 | `permissions: contents: read` on all 8 workflows; `staging-smoke` slug through `env: SLUG`; a test pins both | B |
| T994 7 | CI grep catches `"data/(curated|generated|source)/…"` and `"cache/dfo_salmon…"` literals; fixed `hydat_daily.py` (`--hydat`, `--out`: it prunes `--out`), `ingest_catalogue.py` (`--out`), `data/build_tidal_boundary.py`; mutation-checked the gate | B, C |

### Cause of the `WaterScreen.unnamed` timeout
`baseOnlyUnnamed()` scanned every unnamed handle INSIDE its test, and `storePage` re-opened the fixture's databases for
each one. Cheap on a laptop (4.6 s), over the 30 s on a loaded runner (CI 11-16: 46 s file). Under 20 parallel runs on
this Mac: before 15/20 FAILED, after 0/20. Fix: one shared store promise; the scan runs once at load (like `TRIB`).
`WaterMode` "across waters": 20 parallel runs, before-T849 file and current file both 20/20 here (T849's own proof was
9/10 before, 10/10 after with two suites alongside). Its failures in the 30-day log are 11-16 and 11-18 19:27Z, before
T849 (11-18 21:04Z). Watch for a failure after that; none known.

### Jobs before / after (every check kept)
Before: one job `check` (app-ci, 25 min): checkout (full) -> pnpm -> node -> install -> python venv -> `pnpm check`
(= `check:fast`: tickets, anchors, changelog, boundaries, platform, style, colours, spinner, coho, adipose, splash,
gauge-policy, calendar, fish, answers, pack, app-reads, feed-formats, deps, knip, typecheck, typecheck:worker,
typecheck:seo, promises, inventories; then `vitest run`) -> feeds worker tests -> web bundle -> native bundles. Each
step skipped when an earlier one failed. Triggers: app/**, workflow, bundle sql, dfo_salmon, gauges, requirements,
handoff tickets + handoff/PLAN/*.md.
After:
- `app-ci.yml` (app/**, workflow, action, bundle sql): `gates` = `pnpm run check:fast` (20 min); `vitest` = `pnpm run test` (25); `bundles` = web, then native with `if: !cancelled()` (15). No `needs`.
- `feeds-worker-ci.yml` (worker, packages/core, dfo_salmon, gauges, fixtures, requirements, lockfile): `worker` = `npx vitest run --root .` after the venv (15).
- `docs-ci.yml` (handoff/**, AGENTS.md, the guard files): `ledger` = `pnpm run tickets`, then the guard's two test files (8).
- Unchanged jobs: pipeline-ci `check`, feeds-hydat, feeds-runtiming, staging-security, staging-smoke.
Dropped from app-ci triggers: dfo_salmon / gauges / requirements / handoff. A requirements-only or dfo_salmon-only push no
longer runs the app gates (the generated-file gates over pipeline code are also pipeline-ci's `emit --check`, which
watches those paths) and runs only the worker job. `check:fast` still runs `tickets` on app pushes, so the ledger is
checked on every app push as before.

### Owner decisions and notes
- `feeds-sync-inputs` (workflow deleted from the tree, still "active" with its 4 old failed runs): disabling needs the
  token, so it is NOT done here. Command: `gh workflow disable feeds-sync-inputs.yml` (or the Actions tab).
- `staging-smoke.yml` and `staging-security.yml`: DELETED (owner 2026-11-21: never run; the deploy scripts run the same checks). The T994 6 hardening of both is moot.
- A red `docs-ci` stays the newest finished run for that workflow until a `handoff/**` push goes green (ci_status falls
  back to it when a sha has no docs run). A ledger fix is always such a push.
- F2-owned, left on the shrinking ALLOWLIST in pipeline-ci's grep (remove an entry when fixed): every
  `cache/dfo_salmon` default in `pipeline/regs/dfo_salmon/` (fetch.py:94, wording.py:174-175, untangle.py:619,
  splitwork.py:35-36, churn.py:55, validate.py:202, entries.py:1204) and `closure_mouths.py:164`
  (`data/source/bc_fisheries_data.gpkg`); `scripts/update-hydat.sh` (shell: the gate scans only *.py, so not checked), `pipeline/deliver/release/publish.py` and
  `curation-review/backend/` held no hit for the new pattern. Also allowlisted, by reason: `gauges/feed/publish.py:194`
  ROSTER_PATH (printed in a command for the owner) and `regs/review/tidal.py:146` DEFAULT_CACHE (a provenance label in
  the output). Not caught by the pattern, for G3/F2: `pieces_gate.py:156` `Path("data/release")` default.

### Could not verify
No push: no workflow ran on GitHub. `actionlint` is not installed; I parsed every workflow and the composite action with
PyYAML (structure, a timeout on every job, run-xor-uses on every step), pinned their shape in
`pipeline/tests/test_workflow_pins.py`, and read them. Unverified on a runner: the `filter: blob:none` clone with the
tests' `git show <commit>:<path>` (it fetches blobs on demand, which needs the checkout's persisted credentials: the
default), the composite action's working directories, the saved minutes. The first push will show them; watch the
four workflows. The new grep ran locally (macOS grep -E); CI's is GNU grep, same patterns. Tests run: test_workflow_pins,
test_ci_status, test_test_env (with PIPELINE_PY), hydat_daily / ingest tests, feeds-gate / check-tickets / start-here.

## Lane G3 (deploy gates), 2026-11-21, `lane/t993-g3-deploy` a3ca419e2
Every item verified against the code first; all were real. Tests: `pipeline/tests/test_deploy_gates.py` (stub trees;
each test names its mutation, and all failed on the code before), `scripts/tests/heavy.test.sh` §12,
`scripts/tests/start-workflow.test.sh` (the blip cases), plus the edited pins in `test_dfo_closure_build.py`.
- **T788 #1:** `pieces_gate.fetch_status` raises `StatusUnread` with the cause (HTTP status, DNS, timeout, not JSON),
  said in the refusal; a pass prints what it rests on. Staging `upload.sh` and `sync-inputs.sh` name the staging worker
  themselves (both took `FEEDS_URL` from the shell).
- **T788 #4:** one `deploy/lib/site-stamp.sh` for both `site.sh`: staging refuses a dirty `app/` on its own
  (`SITE_DIRTY_OK`); the stamped commit is the export's, read from its bundle (`sha:"…"`, version-stamp.js), and a HEAD
  that moved since the export is said, with whether `app/` differs.
- **T788 #5:** `pipeline.deliver.release` refuses `--no-basemap` / `--no-synopsis` when `--out-root` or
  `--tiles-release-root` is the live release root (`deliver all` cuts into a staging folder but names the live root for
  the tiles, so both count). The mini release is unaffected.
- **T788 #11:** `scripts/heavy.sh` refuses a start with < 20 GB free on the `$PWD` or `$TMPDIR` volume (exit 73, before
  the lock, after a script's own precheck; names the leftovers to clear) and stops a running job below 3 GB
  (`HEAVY_MIN_DISK_GB`, `HEAVY_KILL_DISK_GB`); `--status` shows free disk; an unreadable `df` refuses the start.
- **Plan 14:** the headers of `deploy/prod/feeds.sh` and `deploy/prod/site.sh` now say Claude may run them as release
  steps (memory 11-21 / 11-18); `deploy/prod/upload.sh` and `sync-inputs.sh` stay owner-only. `PRODUCTION.md` "Every
  later release" step 4/5: the new site gate, and the site before the data when the release needs a newer app.
- Left for others: `pipeline/release.py` maps upload rc 75 only; the new rc 76 ("dispatched, run unreadable, may be
  running") reaches its generic "upload failed (rc=76), read above" (lane G2 may name it). The runbook rewrite (plan 11)
  still has the old "owner deploys the site" wording in `PRODUCTION.md` step 6 and `release-by-hand.md`.

## Lane G2 (release command), branch `lane/t993-g2-release`, 2026-11-21
| item | what | commit |
|---|---|---|
| 2 | min app derived before the build: `build.built_formats` (held to the mini release) + `app_reads.json` → default = max(live stamp, derived), said with the apps a rise leaves behind; preflight stops on an unread format or a flag below it; the site gate reads the candidate's formats, else the code's | ea6d534f2 |
| 2 gap check | none: the code derives 0.8.4 = what r-741815c6090d needs. Build-dependent files: `lakes` (no producer), `flow-own` (`SHIP_OWN_GAUGE_FLAG`), empty salmon shards (same format). Not gated by `app_gate` at all: the manifest's `regulations.versions` / layout versions | ea6d534f2 |
| 4 | the pre-build rehearsal prints NOT SEEN and the step: `--dry-run --build [--rehearse-follows]` or the three steps | ea6d534f2 |
| 9 | `build.headroom`: every file / web moment / offline total vs its gate, tightest first, WARN < 5 % | ea6d534f2 |
| T788 #2 | pieces gate in preflight (candidate else LATEST) and in checks on the candidate | ea6d534f2 |
| 8 | renumber-only golden pages pass with the mapping printed, each index's rule compared (stop 8 passes); a binding diff passes only when no binding code / atlas moved and each change is its own binding input's, one way (`pipeline/release_attribute.py`, `release.binding_moved`). Stop 7 (Lost Lagoon) would now STOP: T981's commit changed reach code (`regs/parsing/io.py`, `local_closures.py`) — conservative by the review | 60ed4ac17, d3272d735, see below |
| T788 #8 | the accepted diff's record survives a rerun after the promote | 60ed4ac17 |
| 15 | preflight fast-fail leaves `check:fast` + the worker suite to the follows run on the candidate (`FAST_FAIL_FOLLOWS_NEXT`; 2 guard lines in `scripts/fast-fail.sh`, T978's file) | d0f0d6ab1 |
| 16 | follows row `data-tier`: the needs_* tier on the candidate in chunks, rule-found files deselected | d0f0d6ab1 |
| T788 #10 | a rerun does not rebuild a candidate this release built when no build input was committed since | d0f0d6ab1 |
| 7 | `scripts/before_push.py` (pipeline-ci steps read from the yml, the tier, `check:fast`, app tree scanners, worker suite); `--hook` prints the install, installs nothing | 31d770acf |
| G3's exit 76 | `RC_WORKFLOW_UNREADABLE`: "dispatch accepted, run not readable: check it, don't re-dispatch" | 5d6478a09 |

Left: the hook (owner's call); the real-data rehearsal (`--dry-run --build --rehearse-follows`) not run here (a lane
worktree has no data; the `--dry-run` was run without fast-fail / fresh copy); the data tier's time on a candidate is
not measured; `regulations.versions` are not app-gated (above); the human runbook (item 11) comes after T978.

## Lane T768 (plan items 10 and 12), branch `test/T768-follow-tests`, 2026-11-22
| item | what | commit |
|---|---|---|
| 10 | the app anchor scanner refuses a field's absence asserted on a release (`absent`: `not.toHaveProperty`, `in` / `hasOwn` false, `Object.keys` = a typed list), mutation-pinned; 3 live hits rewritten | ff946c86e |
| 10 | a column the writer adds without a mini re-cut: `--check` fails naming the file and the re-cut command (test + an end-to-end planted column); before a push it runs in fast-fail's mini chain and `before_push.py` | c5d66fccc |
| 10 | the candidate-shaped run for part ends: `app/tools/part-end-proof.mjs` (a fixture with one more part end; only the content pin may fail), T768 | 1716bcb87, f8916d70f |
| 12 | `WaterMode` across-waters: no failure since T849 (9 CI logs, 1.3–2.3 s); confirmed, nothing to change | — |

Left (not done here): the pipeline scanner's `absent` kind (`pipeline/tests/anchors.py`, another lane's file this round);
a whole record typed or `toBeUndefined()` on a release field stays unscannable (the scanner header says why).

## Plan item 11: the one human runbook (2026-11-22, lane docs/T993-one-human-runbook)
- `handoff/PLAN/runbooks/release-by-hand.md` rewritten as THE runbook: who runs what; before you start; Step 0 the
  version (T998 §0: computed from the app's commits since the last `v*` tag, the owner confirms) and the release branch
  (`--cut`); Step 1 the changelog bullets; Step 2 the rehearsal (`--dry-run --build --rehearse-follows`, the derived
  min app, the site first); Step 3 the data release; Step 4 staging site / feeds / data worker; Step 5 the owner's look;
  Step 6 phones (production AAB to Internal, `play-upload.py --promote … --track alpha` to Closed, Play production;
  TestFlight `--production` with the external group automatic, App Store); Step 7 production (rehearse, the site / feeds
  worker first when their gates say so, data, smoke, site, feeds worker, data worker, phones, watch; every deploy the
  owner's, below); Step 8 tags and the merge-back PR; a fix during the release (cherry-pick); a data-only release
  (`--cut --data-only`); when something breaks; rollback; checklist. Every command checked against the code.
- `release.md` keeps the flag detail (links to the runbook and the gates page; the stale "hook is the owner's call"
  line fixed). `PRODUCTION.md`: the first transition removed (history: the file at a3ca419e2), its one open owner item
  (the GitHub production option of feeds-hydat / feeds-runtiming) kept under "Not yet"; "who does what" updated;
  "every later release" replaced by the runbook's Step 7 and the order rules the gates enforce.
- T992's explanation: `handoff/PLAN/runbooks/release-gates-explained.md`.
- Not done here: the claude.ai doc twin of the runbook (https://claude.ai/artifact/5r4yEqNwqygin1oKo5Zxjk) still has
  the 2026-11-18 text; it is synced when this lands. T1003 (R8) changes no release step yet.

## Owner answers 2026-11-22 (applied to the runbook pages in the review of plan item 11)
1. CONFIRM_PROD, verbatim: "only me unless i say otherwise that time". The production website (`deploy/prod/site.sh`)
   and the production feeds worker (`deploy/prod/feeds.sh`) are owner steps like the production data: Claude prepares
   and verifies (staging, tests, smoke, `--dry-run`, the exact command) and hands it over; it runs one only when the
   owner says so that one time. AGENTS W46 stands as written; the "W46 vs permissions" conflict is settled. Changed:
   `release-by-hand.md` (who runs what, Step 7, checklist), `release-gates-explained.md`, `release.md`,
   `PRODUCTION.md`, and the header comments of `deploy/prod/site.sh`, `deploy/prod/feeds.sh`, `deploy/staging/feeds.sh`.
2. Closed testing gets the PRODUCTION Android build ("Production build (Rec.)"): the production AAB to Internal, then
   that build promoted to Closed. 0.8.5's staging build 43 on Closed was the exception. Said in Step 6.
3. Version: "let it be 0.9 but we wont release yet until we have more stuff from next chat in it". The next release is
   0.9.0 as computed (from T998's tooling commit touching `app/package.json`); no tool change. Step 0 says it is
   settled for this release; the "stop and ask when a deciding commit is not an angler change" advice stays.

Review of plan item 11 (2026-11-22, second agent): the code pointers to PRODUCTION.md's removed steps were moved
(`site_gate.redeploy_steps` and its test, `deploy/prod/site.sh`, `deploy/prod/drop-v1.sh`); the runbook's Step 2 now
names the `pieces` stop beside `site` (staging feeds worker before the data); the merge-back commands match what the
release prints; the gates page gained the staging fingerprint stop, the tag stops and the push gate's someone-else case.
