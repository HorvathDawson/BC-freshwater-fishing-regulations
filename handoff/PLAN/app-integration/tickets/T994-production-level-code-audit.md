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
  needs a loopback Host, and every non-GET needs `Origin == VITE_ORIGIN`; anything else is a 403 that says why, and
  is logged. Route table unchanged. Test: `test_curation_review_origin.py`.
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
