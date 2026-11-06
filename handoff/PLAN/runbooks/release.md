# Runbook: a data release (T295/T297, T408)

One command builds a release, checks it, makes it LATEST and puts it on staging. Production later re-uploads
**the same release folder**. Nothing is rebuilt for production ("build once, promote the same artifact").

Claude runs the staging release after a batch of curated changes lands. A human runs the same command the same way.
Code: `pipeline/release.py`.

## 1. Look at the plan

```bash
.venv/bin/python -m pipeline release --staging --dry-run
```

This runs nothing. It prints each stage and what it would do now: uncommitted files, the heavy-job lock, the build's own
plan (which stages run and why), the min app version, and the upload command.

## 2. Run it

```bash
.venv/bin/python -m pipeline release --staging
```

| stage | what it does | when it stops |
|---|---|---|
| 1 preflight | Code (`pipeline/`, `scripts/`, `deploy/`) and `data/curated/` must be committed. `marks.jsonl` and `closed.json` may be dirty, because no stage reads them. | Anything else is uncommitted. Commit it. |
| 2 baseline | Copies the reach run the live release (LATEST) was cut from to `data/generated/release/baseline/<LATEST>/`. That copy is the "before" of the diff. | The reach run on disk is not LATEST's and no copy was kept. Give one with `--baseline DIR` (its `report.json` digest must match). |
| 3 build | `scripts/heavy.sh release 50 -- python -m pipeline build --hold-latest`. The build skips any stage whose inputs did not change. It writes the new release to `data/release/<r-id>/` and names it in `data/release/CANDIDATE`. **LATEST does not move.** | A build stage failed. `python -m pipeline build --status` shows the stage and its log. |
| 4 checks | Manifest sanity: every file is there at its size, the size gate passes, and the bundle names the reach run on disk. The app-version gate (`pipeline/deliver/release/app_gate.py`, the same one `upload.sh` runs). The rule-binding diff against the live release. | Any check fails. **A non-empty binding diff always stops here** (see 3). |
| 5 promote | LATEST := the candidate. `CANDIDATE` is removed. | — |
| 6 staging | `deploy/staging/upload.sh <r-id>` with `MIN_APP_VERSION`. This is the staging bucket only, with the smoke test. It starts the run-timing workflow for the release's tiles while the files upload, and writes the release pointer last, after run timing for those tiles is on R2 (T412). The branch must be pushed (the workflow runs GitHub's copy). | The upload failed. Fix it and rerun. Stages 1–5 are skipped. |

When a stage fails, the run prints what failed and what to do. **Rerun the same command to resume.** A finished stage
is skipped: the build has its own stage cache, and the ledger `data/generated/release/ledger.json` holds the result of
each check, the promote and the staging upload for each release id.

Before the checks, the build has already rewritten the live working outputs: the reach run, tiles, bundle, export and
run-timing base. The review app reads these. Only the release pointer waits. So a release that fails its checks sits on
disk, named by `CANDIDATE`, and nothing ships it.

`--min-app-version X.Y.Z` sets the oldest app that may apply the release. The default is the live release's stamp,
and the plan prints it.

## 3. A binding diff

```
[4/6] checks FAILED: the rule bindings differ from the live release r-…
  next: read data/generated/release/checks/<r-id>/binding_diff.txt (full: …/binding_diff.json); …
```

Read the summary. Every changed rule must be one that the batch meant to move. If all of them are:

```bash
.venv/bin/python -m pipeline release --staging --accept-diff data/generated/release/checks/<r-id>/binding_diff.json
```

The accept only works if that file has exactly the content written for **this** diff. An accept from an older diff
fails. If a change was not intended, fix the cause, commit, and rerun.

## 4. Production (human only)

```bash
export R2_ACCOUNT_ID=… R2_ACCESS_KEY_ID=… R2_SECRET_ACCESS_KEY=… R2_BUCKET=<production bucket>
.venv/bin/python -m pipeline.deliver.release.publish --release data/release/<r-id>   # the plan, nothing sent
CONFIRM_PROD=<r-id> .venv/bin/python -m pipeline release --prod <r-id>
```

`--prod` refuses unless every one of these holds:

- the ledger shows the release passed its checks and reached staging;
- the release folder is unchanged since then (content fingerprint; the upload stamps `published` and `minAppVersion`
  are ignored);
- `CONFIRM_PROD` names the release;
- it runs in an interactive terminal, and you type the id again;
- there is no agent environment (`CLAUDECODE`, `AI_AGENT`).

It then runs `python -m pipeline.deliver.release.publish --release data/release/<r-id> --yes`. That uploads the
manifest last and writes `PUBLISHED`. If the website must follow, deploy it separately with `deploy/prod/site.sh`.

## 5. After a promote: re-cut the goldens, then the strict parity (T432)

The app's rules→app parity suites compare against the release `app/fixtures/golden/taps.json` was cut from. On any
other release they SKIP, and a skip is green. So after LATEST moves:

1. `node app/tools/regs-recut/sync.mjs` (T441: ONE step re-cuts every release-backed fixture — taps.json, hydrate.json,
   the fixture release `app/packages/data/dev/regs-release/`, `dev/section-lines.json`, the regs VM fixtures), then
   commit them with the release id in the message. No test types a section id (they name stream key + start metre, `sectionNames`), so a renumbering needs no test edit.
2. `scripts/heavy.sh test-full 40 -- ./scripts/test-full.sh`. Its app step runs with `REGS_STORE_REQUIRED=1`, so a
   release that is not paired with the goldens FAILS there, naming the `sync.mjs` command, instead of passing on zero
   cases.

## Not in this command

- **A new atlas.** Build and promote it first (`python -m pipeline build --atlas`, read the parity, then `--promote`).
  The release then builds on it.
- **Run timing (T412).** Deliver stages only the binding base; the workflow publishes run timing, started by the staging upload.
