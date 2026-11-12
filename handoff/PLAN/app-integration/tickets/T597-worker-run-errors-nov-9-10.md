# T597 — Two feeds worker run failures on 2026-11-09/10 are explained and guarded

Status: PARTIAL — items 1-3 built (5c7b9719d, ecd2de06a, 7834e740e), the prod sync-inputs twin in 5325db02f, staging worker deployed 2026-11-12; left (Claude): read a week of /status for item 1 (no whole-run gauge failure at the day turn)

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

## Done 2026-11-10
1. 5c7b9719d (with T550 item 4): 00:05 UTC whole-run 404 confirmed on two nights (11-10, 11-11) and ≈ 6 of 7 by the 7-day
   count; the gauge crons skip 00:05; a late run meeting the turn is one degraded line, not 428 failures. Acceptance
   (a week of status with no day-turn failure) needs the deploy and a week.
3. ecd2de06a: obs.json keeps the last rows of stations not rewritten (0-station obs.json on an all-304 run). The
   `feeds/live/08MF0xx.json` 404 did not reproduce: 08MF005/040/062/065/068 answer 200 (23:35Z run).

## Done 2026-11-11
2. 7834e740e: the worker serves `inputs` on /status.json (also on its 503): R2 key → the formats it reads
   (`private/feeds/dfo/wording.json` dfo-wording/2, `feeds/inseason/matches.json` inseason-links/3; status.ts
   INPUT_FORMATS). deploy/staging/sync-inputs.sh refuses an input naming a format the deployed worker does not list, or any
   formatted input when the worker cannot say (unreachable / deployed before this), with a plain `FAILED <key>: …` line;
   the other inputs still go up and the script exits 1. DFO entry files and the roster carry no format and are not checked.
   deploy/prod/sync-inputs.sh is unchanged (the prod worker is not live; it would refuse every formatted input).

## Done 2026-11-11 (the prod twin, 5325db02f)
- The format check is one copy, `deploy/lib/input-formats.sh`, sourced by both uploaders. `deploy/prod/sync-inputs.sh`
  asks `https://feeds.canifishthis.ca/status.json` and refuses an input format it does not list. The first seed before
  the production worker exists is an explicit claim, `FEEDS_WORKER_NOT_LIVE=1`, honoured only while the address gives no
  answer or the 503 placeholder answers. Tested on stubs in a temp tree; nothing was run against production.
- The staging worker with `inputs` is deployed (2026-11-12 00:08 UTC), so `deploy/staging/sync-inputs.sh` puts formatted
  inputs again.
