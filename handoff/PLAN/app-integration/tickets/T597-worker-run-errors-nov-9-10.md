# T597 — Two feeds worker run failures on 2026-11-09/10 are explained and guarded

Status: OPEN

Source: staging `/status.json` read 2026-11-10 19:37 UTC. All three feeds are OK now; these are their last errors.

## Today
1. **Gauges, 2026-11-10 00:05 UTC**: "no station answered (asked 428); index.json kept (last good)". The station list
   and all 428 hourly files answered 404 in that one run. Last 24 h: hourly 428 × 404 + 4 × 522; daily 36 × 404;
   temperature 7 × 522. The last good index was kept, as designed. Is 00:05 UTC a daily turnover at the source (files
   absent for a few minutes)? If so the run should not ask then, or should retry a few minutes later.
2. **In-season, 2026-11-09 22:54 UTC**: "matches.json is inseason-links/3, not inseason-links/2". The data file moved to
   format 3 before the worker that reads format 3 was deployed. It recovered on the deploy. The publish order allowed a
   file the live worker cannot read.
3. Seen while here (2026-11-10, T580): staging 404 for `feeds/live/08MF0xx.json` and a staging `obs.json` with 0
   stations. Check with T550.

## Work
1. Gauges: read the source's behaviour at 00:00–00:10 UTC over a few days (the archive has the run log); move or retry
   the run. A whole-run 404 files one digest line, not 428 source failures.
2. Format bumps: the upload of a curated/feed input refuses a format the deployed worker does not list (the worker
   serves its accepted formats on `/status.json`; the upload script checks). Fits T558 (worker state) if that ticket
   already covers it; then close this half there.

## Acceptance
- A week of status shows no whole-run gauge failure at the day turn.
- Uploading a newer-format input before its worker fails at upload with a plain message.

Depends: T550, T558 (overlap). Size: S–M. Owner: no.
