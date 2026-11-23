# T994 — Production-level audit: non-standard practices and data-flow issues across the code

Status: DONE — a125140a9, af396ea5d, a424dc8a0, 2399453d8, a49c54d9e, b0bcc2eda, 347d3388e, 0a610610c, 67e723567, 5b6dd066b, bd4400513, cefdb3fc5, a6d97778a, 21c1a6ed5, 12be2fa14, 80c389059, a3ca419e2, 8a89a73a9, 77ee9ca7d (every finding fixed, ticketed or closed: see "Closure 2026-11-22") (Earlier status: IN PROGRESS (main chat 2026-11-21: read-only audit agent first; findings become numbered lines here or new tickets).)

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

## Lane F1 (feeds worker + Python twins), branch `lane/t994-f1-worker`, 2026-11-21
Every finding was read against the code first; all eight held. Each fix has a test that fails without it.
- **3** (report words in 400-day backups) → `0a610610c`. Every outbox copy (daily and monthly) now leaves out the queued
  `report` / `crash` events (`backup.ts scrubOutbox`, `events.ts PERSONAL_EVENT_KINDS`). A restored outbox raises them
  again from the stored records. The prune scrubs copies made before the rule. An outbox that does not parse is not
  copied and the run fails. `forget-report` also takes the report's queued event out of the live outbox
  (`dropPending`). The privacy page is unchanged: the fix keeps its 180 / 90 / 30-day words true.
- **4** (in-season feed read) → `67e723567`. Read through `readStored`. Only an absent file or another format counts
  as a first feed.
- **5** (temperature history wipe, worker + Python) → `5b6dd066b`. The temperature history, a station's
  year-to-date file and (Python) the previous index: an absent file is a first run, and any other file that does not
  read fails the run, naming the file. One shared fixture: `app/workers/feeds/test/fixtures/state-reads.json`, read by
  `state-reads.test.ts` and `pipeline/tests/test_feed_state_reads.py`. Two Python tests that pinned the old reset were
  rewritten.
- **26** (reminder dedupe resends) → `5b6dd066b`, the same fixture. A sent-keys file that does not read sends nothing
  that run: every due key is `failed` and `error` says why. Both twins changed. The Python writes the file with
  `atomic_write`. The worker's copy never runs in production (`notifyUrl: null`; the outbox tells the owner).
- **23** (APNs token per wake) → `bd4400513`. The provider token is kept at `private/push/apns-jwt.json` (never backed
  up) and reused for 50 minutes with the same key id and team. It is dropped when APNs answers 403
  Expired/InvalidProviderToken. Apple's 20-minute rule was not checked against the live service.
- **25** (push register total) → `cefdb3fc5`. A NEW token counts against `PUSH_NEW_TOKENS_PER_DAY_TOTAL` (1,000 a day);
  a known token re-registering does not. Past the total the route answers 429, and `capEvents` tells the owner once.
- **24** (backup subrequests) → `a6d97778a`. Every bucket call is counted against `BACKUP_SUBREQUESTS` (6,000; the
  purge shares the invocation). The state is copied first and the personal records last. Copying stops `PRUNE_RESERVE`
  short of the budget, the manifest names what was left (`partial`), and a prune that would pass the budget fails the
  run. An object over the size cap or a partial snapshot fails the run; an object gone between the list and the get
  does not (the purge runs beside the backup). NOT done: incremental report copies. A daily snapshot stays whole for
  `restore-test.sh`, so a store past about 4,500 reports now fails the backup every day, which is told, not silent.
  The purge's own per-crash-kind reads are not budgeted.
- **30** (copy-paste in public routes) → `21c1a6ed5`. `jsonResponse`, `readCapped` and `readJsonBody` live in `io.ts`
  (push gains the Content-Length check). `checkToken` answers `{ok: false}` on a network error.

## Closure 2026-11-22 (lane L13): every finding accounted for
Findings 1-5, 8-28 and 30 are fixed (sections above; F1/F2/F3/T993 lanes). The three the ticket did not mark:
- **6** (workflow `permissions:`, the `inputs.slug` splice): fixed by T993 G1 (77ee9ca7d): every workflow declares
  `permissions: contents: read` and the staging-smoke workflow with the splice is gone.
- **7** (the data-path gate misses multi-segment literals): `hydat_daily.py` and `ingest_catalogue.py` were moved onto
  the config keys (8a89a73a9); the gate pattern is unchanged and about 10 literals remain: T1021.
- **29** (build-apk.sh: the APK email result on the full-build path): closed as by design, not a defect. The script's own
  header says "a failed email only warns": on the full-build path the upload already succeeded and the owner is told by
  the printed `email: HTTP <code>` line; `--publish-only` is the path whose whole job is the notify, so it exits 1.
- Side notes the lanes left: the worker twin of 17 and the stale `--upload` text are fixed (f20e250e3 and the T994 F1
  review fixes); the incremental report backup (24's remainder) is T999; `boto3` is no longer in `requirements.txt`;
  `awaiting_water._doc` reading a missing T89 file as `{}` is a note on F2's lane, left as it was (outside finding 15).

