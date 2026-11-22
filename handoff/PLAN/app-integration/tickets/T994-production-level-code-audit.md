# T994 — Production-level audit: non-standard practices and data-flow issues across the code

Status: IN PROGRESS (main chat 2026-11-21: read-only audit agent first; findings become numbered lines here or new tickets).

Source: owner 2026-11-21, verbatim: "You could also get an agent to do a basic audit for non standard practices or
issues with the flow of data etc. we want to make our code production level. Also clean up gha workflows and make sure
we just waste less time with bad ci/cd of failed runs." (The GitHub Actions half is in T993's scope.)

## Work
1. Read-only audit of `pipeline/`, `app/`, `curation-review/`, `deploy/`, `scripts/`: non-standard practices
   (error handling, config, typing, logging, dependency use, dead code, copy-pasted logic), and data-flow issues
   (a fact computed twice, a reader of curated files at run time, a hidden fallback, an untyped boundary).
   Start from the 2026-11-16 audit (`handoff/PLAN/audit-2026-11-16/`) and report only what it and the open tickets
   do not already hold.
2. Each finding: file:line, what is wrong, why it matters in production, the fix, its size. Ranked.
3. The owner sees the list; fixes go into this chat's lanes or new tickets.

## Acceptance
A ranked findings list with no item already in an open ticket; each kept finding fixed or ticketed.

Depends: —. Size: M. Owner: reads the ranked list.

## Audit done 2026-11-21 (read-only agent): 30 findings, none already ticketed
Report: `handoff/PLAN/audit-2026-11-21/t994-audit.md` (ranked, file:line, fix, size; "(sub)" = sub-auditor reading not
re-opened by the lead). Top 5: `update-hydat.sh --upload` publishes ungated to any bucket; review backend CSRF on
`POST /api/inseason/resolve`; worker backups keep report text 400 days (privacy promise is 180); a failed in-season feed
read resets history as a first run; one failed read wipes the water-temperature history (worker + Python twin).

Fix lanes (disjoint files):
- **F1 feeds worker + its Python twins:** findings 3, 4, 5, 23, 24, 25, 26, 30.
- **F2 pipeline + review backend:** 1, 2, 14, 15, 16, 17, 21, 27.
- **F3 app data seam:** 8, 11, 12, 13, 22.
- **With T993 (workflows, deploy scripts):** 6, 7, 9, 10, 18, 19, 20, 28, 29.

## Lane F3 (app data seam): all five confirmed against the code and fixed
- **8** (no timeout on salmon / in-season / run-timing / HYDAT-day / weather reads): ONE helper `timedFetch` (`app/packages/data/src/net/timeout.ts`, 30 s, headers and body, an environment error so it is shown and not reported); `App.tsx` wraps those five reads and the manifest recovery. The gauge feed and release files keep their own limits. `conditionalJson` rides the injected fetch, so in-season/run-timing/salmon conditional reads are covered. Commit: see git log "T994 F3: timed fetch".
- **11** (synopsis-PDF recovery swallowed): the alternate reads through `loadManifest` (ok check, parse, timed); the native catch reports (`reportFailure`); `tools/failures.test.ts` FINE count for `synopsisPdf.native.ts` 4 to 3. Same commit as 8.
- **12** (`storedHydat` format): `parseHydatDaily` checks `hydat-daily/1`, an integer `from`, `q` as arrays of arrays; named throw. Commit: "T994 F3: validate at the seam".
- **13** (`parseManifest`): each file entry checked for url, bytes, sha256, format; `parseManifestText` makes a non-JSON body a `ManifestError`; the four JSON.parse-then-parse call sites use it. Same commit as 12.
- **22** (`release/previous`, `release/refused`): `readHeld` shape-checks the files map; both boot reads get the OPENED pattern (report, then absent). Same commit as 12. The boot catches have no test (boot has no harness for these reads); `readHeld` is tested.
- `pnpm check` stops at `build-coho` here (no `.venv` in the lane worktree); `tsc -b`, the data package and `tools/` vitest run pass, apart from `regs-fixture` (also needs `.venv`).

## Lane F2 (branch `lane/t994-f2-pipeline-backend`, 2026-11-21): every finding verified against the code, all real
Each fix has a test that fails without it (checked by putting the old code back).
- **1** `a125140a9`: `deliver/release/publish.py` is the upload PLAN only; `--yes` / `--min-app-version` refused
  with the release commands. `update-hydat.sh` drops `--upload`, builds with `--hold-latest`, prints `pipeline
  release --staging`. Updated: `MAP.md:29`, AGENTS 64, the annual runbook. Tests: `test_publish.py`,
  `test_update_hydat_script.py`, `test_feed_maintenance.py`, `test_release_stamp.py`.
  - Not changed here (other lanes' files): the feeds worker still tells the owner `scripts/update-hydat.sh --upload`
    (`app/workers/feeds/src/email.ts:253`, `index.ts:126` and the email snapshot). The script now refuses that with
    the right command. `.github/workflows/feeds-hydat.yml:17` (T993) has the same stale comment.
- **2** `af396ea5d`: one middleware, `app.browser_refusal`. A browser request (one carrying Origin or Sec-Fetch-Site)
  needs a loopback Host, and every non-GET needs an Origin of Vite's port on 127.0.0.1 or localhost (review nit:
  localhost now accepted too); anything else is a 403 that says why, and is logged. Review nit 2: `update-hydat.sh`
  refuses to hand over a CANDIDATE that an earlier build left (it compares the pointer before and after the build). Route table unchanged. Test: `test_curation_review_origin.py`.
- **14, 15** `a424dc8a0`: `_fwa_lake_poly` now raises on a missing gpkg and returns None only when no row matches;
  it closes its connection with `closing()`. The other functions now catch only the refusal they mean:
  - `awaiting_water`: no catch left.
  - `reports`: `_vintage` raises; `_node_of` catches KeyError only; `_bundle_items` raises on a missing or
    other-vintage bundle.
  - `dfo.walks_now`: ValueError only.
  - `dfo.rule_labels`: ValueError or TypeError only, plus a `refused` field.
  - `inseason._derived_label`: KeyError only.

  Test: `test_review_backend_no_quiet_answers_t994.py`.
- **16** `2399453d8`: `split_coords` has no catch, and its dead locals are gone. Test:
  `test_dfo_split_coords_t994.py`.
- **17** `a49c54d9e`: only an HTTP 404 counts as "not in season". Every other failure is counted and printed by
  `with_series`. Test: `test_forecast_series_failure_t994.py`.
  - The worker twin (`app/workers/feeds/src/gauges/forecast.ts` `fetchSeries`: `!r.ok` and `catch` both give null)
    has the same shape and is NOT fixed (F1's files).
- **21** `b0bcc2eda`: the backend's requirements are `-r ../../requirements.txt -c ../../constraints.txt`, and
  `uvicorn[standard]` is in the root requirements. Test: `test_curation_review_deps_t994.py`.
  - Seen, not changed: `boto3` is in requirements.txt but not pinned in constraints.txt.
- **27** `347d3388e`: `_read_texts` raises an urgent `dfo-text-parse` notice, and the last texts stand. Test:
  `test_dfo_live_texts_t994.py`.
- Also in this lane (main chat's ask): T788 items 6 (`a461fbc50`) and 7 (in `a424dc8a0`); see T788.
- Not verified: the data-reading tiers of the touched review/DFO test files. The worktree has no atlas, bundle or
  gpkg, so only the no-data tier ran. `awaiting_water._doc` (a missing T89 file reads as `{}`) is the same shape and
  was left alone: it is outside finding 15's lines.

## Fixed by T993 lane G3 (`lane/t993-g3-deploy` a3ca419e2; tests in `pipeline/tests/test_deploy_gates.py`)
- **9** (format gate fails open): fixed. `input-formats.sh` refuses a staged file jq cannot read, stops without jq
  (and both `sync-inputs.sh` look before staging), and keeps the worker's cause (curl's words, a body that is not JSON).
- **10** (no prod site-version gate): fixed. `deploy/prod/upload.sh` runs `site_gate --site https://canifishthis.ca`
  before any copy, with its own `PROD_SITE_CHECK_SKIP`; a rehearsal bucket asks `SITE_URL` or says it did not ask.
  The refusal names the production site and PRODUCTION.md step 6; the runbook order (site first) is noted there.
- **18** (`--dry-run` from `$1` only; lock race): fixed. Prod's argument loop (unknown = exit 2); a dead holder's lock
  is taken over under a mutex with `mv`, as heavy.sh.
- **19** (`|| true` / `2>/dev/null`): fixed. Removed in staging `site.sh` (hub rewrite, now post-checked; bundle and
  water page said), both `site.sh` bundle reads, staging `upload.sh` run-timing read (`deploy/lib/r2-object.sh`:
  absent vs rclone failed), `restore-test.sh` (one check read for counts; diff exit 2 said), both `feeds.sh` secret
  lists (read once, exit checked) and staging's deploy log (whole on failure), `rotate-run-token.sh`, `sync-inputs.sh`'s
  resolved.json read. The rest are justified and pinned by `test_every_hiding_left_in_the_deploy_scripts_is_justified`
  (lock probes, log filters after a checked copy, the feeds gate's empty-means-unread). The build scripts are T978's.
- **20** (gh blip after an accepted dispatch): fixed. `start-workflow.sh` says each failed look and exits 76 ("may be
  running: look before a rerun"), also for the old accepted-but-no-run case; `upload.sh` words 76 apart from 75.
- **28** (prod precondition after the uploads): fixed. HYDAT is checked beside the manifest in both uploads, before any
  copy; staging's optional `if -d` is gone (the pair agree).
