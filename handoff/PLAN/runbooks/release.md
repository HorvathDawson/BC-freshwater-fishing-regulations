# Runbook: a data release (T295/T297, T408)

One command builds a release, checks it, makes it LATEST and puts it on staging. Production later re-uploads
**the same release folder**. Nothing is rebuilt for production ("build once, promote the same artifact").

Claude runs the staging release after a batch of curated changes lands. A human runs the same command the same way.
Code: `pipeline/release.py`.

## 1. Rehearse it (T666)

```bash
.venv/bin/python -m pipeline release --staging --dry-run
```

The rehearsal runs every stage's **checks** and builds, commits, promotes and uploads nothing. Preflight's whole table
runs (the same one the real run reads), fast-fail and the fresh-copy test run included (minutes; the second goes
through `scripts/heavy.sh`): a pass is recorded, so the real run skips it on the same tree. It ends with **every stop the real run would make now**, as one list. Before a build it covers preflight, the
baseline and the build's plan. Run it again once the build has left a `CANDIDATE`: then it also checks the manifest,
the app gate, the binding diff, the in-season matches (`resolve --check`), the first step of the fixtures re-cut
(into a temp file) and the upload's first refusals. Not rehearsed: the build itself, whether a golden page changes,
the tests on the candidate, the upload and its smoke test.

## 2. Run it

```bash
.venv/bin/python -m pipeline release --staging
```

| stage | what it does | when it stops |
|---|---|---|
| 1 preflight | **Every check runs and every failure is listed together** (T666). Code (`pipeline/`, `scripts/`, `deploy/`) and `data/curated/` must be committed (`marks.jsonl` and `closed.json` may be dirty: no stage reads them). The atlas is not behind its curated inputs. Review marks have landed. HEAD is on `origin/redesign/stream-sections` (T484: the upload starts a GitHub workflow). CI is not red. The staging site is new enough. Last, `scripts/fast-fail.sh` (T483; it also lists every failed step; `--no-fast-fail "<reason>"` skips it and prints the reason). Its pass is remembered by the **content of the tree**, not by HEAD: a rerun on the same files skips it, and after a commit it checks only what changed since the commit it last passed on. Then the **fresh-copy test run** (owner 2026-11-11): HEAD cloned into a temp folder and the pipeline's no-data tests run there as CI runs them (`scripts/fresh-copy-tests.sh`, through heavy.sh), once per release; `--no-fresh-copy "<reason>"` skips it and prints the reason. | One stop with the numbered list of everything that failed, each with its next step. Fix them all, then rerun. |
| 2 baseline | Copies the reach run the live release (LATEST) was cut from to `data/generated/release/baseline/<LATEST>/`. That copy is the "before" of the diff. | The reach run on disk is not LATEST's and no copy was kept. Give one with `--baseline DIR` (its `report.json` digest must match). |
| 3 build | `scripts/heavy.sh release 50 -- python -m pipeline build --hold-latest`. The build skips any stage whose inputs did not change. It writes the new release to `data/release/<r-id>/` and names it in `data/release/CANDIDATE`. **LATEST does not move.** | A build stage failed. `python -m pipeline build --status` shows the stage and its log. |
| 4 checks | Manifest sanity: every file is there at its size, the size gate passes, and the bundle names the reach run on disk. The app-version gate (`pipeline/deliver/release/app_gate.py`, the same one `upload.sh` runs). The rule-binding diff against the live release. | Any check fails. **A non-empty binding diff always stops here** (see 3). |
| 5 follows | **Refreshes what follows the release, on the candidate, before LATEST moves** (T666; the list is `python -m pipeline.release_follows`). (a) The in-season matches are resolved; a rewritten `notice_waters.json` is committed alone. (b) The app's release-backed fixtures are re-cut (`node app/tools/regs-recut/sync.mjs --release <candidate>`); a change that only renumbers is committed with the release id. (c) The tests that ask for the release under test run with `RELEASE_DIR` = the candidate (both suites; `--no-follow-tests "<reason>"` skips them and prints the reason). (d) The push gate: the commits the release made must be on GitHub. | A match does not resolve. A **golden page changed** (what an angler is told): nothing is committed; read the diff and rerun with `--accept-fixtures <token>` (see 3a). A test fails on the candidate. Commits are not pushed: one stop with the one `git push` command, or pass `--push` and the release pushes its own commits (never anyone else's). |
| 6 promote | LATEST := the candidate. `CANDIDATE` is removed. | — |
| 7 staging | The upload's first refusals are asked up front (run-timing base for these tiles, `gh` logged in, the `r2:` remote). Then `deploy/staging/upload.sh <r-id>` with `MIN_APP_VERSION`: the staging bucket only, with the smoke test. It starts the run-timing workflow for the release's tiles while the files upload, and writes the release pointer last, after run timing for those tiles is on R2 (T412). If GitHub refuses to **start** the workflow, the upload retries the start itself (`scripts/start-workflow.sh`: 3 tries, 120 s apart, each said; `WF_START_TRIES` / `WF_START_WAIT` change the bound). | The upload failed. Fix it and rerun: stages 1–6 are skipped. When only the workflow start failed, the message says so; rerun when GitHub Actions answers. |

When a stage fails, the run prints what failed and what to do. **Rerun the same command to resume.** A finished stage
is skipped: the build has its own stage cache, and the ledger `data/generated/release/ledger.json` holds the result of
each check, the promote and the staging upload for each release id.

Before the checks, the build has already rewritten the live working outputs: the reach run, tiles, bundle, export and
run-timing base. The review app reads these. Only the release pointer waits. So a release that fails its checks sits on
disk, named by `CANDIDATE`, and nothing ships it.

`--min-app-version X.Y.Z` sets the oldest app that may apply the release. The default is the live release's stamp,
and the plan prints it. A rerun after the promote keeps the min app this release's checks passed with (T484).

**The command remembers its flags (T666).** `--baseline`, `--accept-diff`, `--accept-fixtures`, `--min-app-version`
and `--push` given on any run are kept in the ledger until the release is on staging; a rerun is the bare command and
says which flags it remembered. A flag given again replaces the remembered one; `--forget-flags` drops them. A
`--baseline` left on the command line from before the promote is ignored, with a line saying so. The escapes that skip
a guard (`--no-fast-fail`, `--no-fresh-copy`, `--no-ci-check`, `--ship-stale-atlas`, `--no-follow-tests`) are never remembered: give the
reason on each run that needs it.

## 3. A binding diff

```
[4/7] checks FAILED: the rule bindings differ from the live release r-…
  next: read data/generated/release/checks/<r-id>/binding_diff.txt (full: …/binding_diff.json); …
```

Read the summary. Every changed rule must be one that the batch meant to move. If all of them are:

```bash
.venv/bin/python -m pipeline release --staging --accept-diff data/generated/release/checks/<r-id>/binding_diff.json
```

The accept only works if that file has exactly the content written for **this** diff. An accept from an older diff
fails. If a change was not intended, fix the cause, commit, and rerun.

## 3a. A changed golden page

```
[5/7] follows FAILED: the re-cut on r-… changed 2 golden page(s) — what an angler is told changed; nothing was committed: …
  next: read `git diff -- app/packages/ui-native/src/regs/fixtures`; if every change is intended, rerun with
    --accept-fixtures <token>
```

The re-cut files are in the working tree, uncommitted. Read the diff. If each changed page is what the batch meant:
rerun with the token printed. It names exactly this re-cut; another re-cut needs its own. The release then commits the
fixtures and goes on.

## 4. Production (human only)

The whole production run (data, site, feeds worker, Play, rollback) is `handoff/PLAN/release/PRODUCTION.md` (T404).
The data step:

```bash
deploy/prod/upload.sh <r-id> --bucket bc-fishing-regulations-staging --dry-run   # rehearse: nothing sent
CONFIRM_PROD=<r-id> .venv/bin/python -m pipeline release --prod <r-id>
```

`--prod` refuses unless every one of these holds:

- the ledger shows the release passed its checks and reached staging;
- the release folder is unchanged since then (content fingerprint; the upload stamps `published` and `minAppVersion`
  are ignored);
- `CONFIRM_PROD` names the release;
- it runs in an interactive terminal, and you type the id again;
- there is no agent environment (`CLAUDECODE`, `AI_AGENT`).

It then runs `CONFIRM_PROD=canifishthis.ca deploy/prod/upload.sh <r-id>`: it refuses unless staging serves the same
manifest bytes, copies the files (and staging's run timing for the same tiles), keeps the old pointer as
`release/manifest.previous.json`, writes the manifest last and `data/release/PUBLISHED`. Undo: `deploy/prod/rollback.sh`.

## 5. The goldens, and the strict parity (T432, T666)

The app's rules→app parity suites compare against the release `app/fixtures/golden/taps.json` was cut from. On any
other release they SKIP, and a skip is green. **The release re-cuts them itself, in stage 5, on the candidate**, and
runs the tests that read the release with `REGS_STORE_REQUIRED=1`. So nothing is re-cut by hand after a release.

Outside a release (LATEST moved another way, e.g. a promote by hand):

1. `node app/tools/regs-recut/sync.mjs` (T441: ONE step re-cuts every release-backed fixture — taps.json, hydrate.json,
   the fixture release `app/packages/data/dev/regs-release/`, `dev/section-lines.json`, the regs VM fixtures; it reads
   `--release <dir>`, else `RELEASE_DIR`, else LATEST), then
   commit them with the release id in the message. No test types a section id (they name stream key + start metre, `sectionNames`), so a renumbering needs no test edit.
2. `scripts/heavy.sh test-full 40 -- ./scripts/test-full.sh`. Its app step runs with `REGS_STORE_REQUIRED=1`, so a
   release that is not paired with the goldens FAILS there, naming the `sync.mjs` command, instead of passing on zero
   cases.

## Not in this command

- **A new atlas.** Build and promote it first (`python -m pipeline build --atlas`, read the parity, then `--promote`).
  The release then builds on it.
- **Run timing (T412).** Deliver stages only the binding base; the workflow publishes run timing, started by the staging upload.
