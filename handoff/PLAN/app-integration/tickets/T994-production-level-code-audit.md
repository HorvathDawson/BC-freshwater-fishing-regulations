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
