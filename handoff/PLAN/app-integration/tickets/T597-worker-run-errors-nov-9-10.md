# T597 — Two feeds worker run failures on 2026-11-09/10 are explained and guarded

Status: PARTIAL — items 1-3 built (5c7b9719d, ecd2de06a, 7834e740e), the prod sync-inputs twin in 5325db02f, staging worker deployed 2026-11-12; left (Claude): read a week of /status for item 1 (no whole-run gauge failure at the day turn)

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T597 | Two feeds worker run failures 11-09/10 explained and guarded (gauges all-404 at 00:05 UTC; in-season format ahead of the worker) | PARTIAL — items 1-3 and the prod twin built; left (Claude): a week of /status for item 1 | 5c7b9719d, ecd2de06a, 7834e740e, 5325db02f | /status.json 2026-11-10; overlaps T550, T558 |

Source: staging `/status.json` read 2026-11-10 19:37 UTC. All three feeds are OK now; these are their last errors.

## What
1. **Gauges, 2026-11-10 00:05 UTC**: "no station answered (asked 428); index.json kept (last good)". The station list
   and all 428 hourly files answered 404 in that one run (a daily turnover at the source).
2. **In-season, 2026-11-09 22:54 UTC**: "matches.json is inseason-links/3, not inseason-links/2". The data file moved to
   format 3 before the worker that reads format 3 was deployed.
3. Staging 404 for `feeds/live/08MF0xx.json` and a staging `obs.json` with 0 stations (2026-11-10, T580).

Done: 1 (5c7b9719d, with T550 item 4): 00:05 UTC whole-run 404 confirmed on two nights (11-10, 11-11) and ≈ 6 of 7 by
the 7-day count; the gauge crons skip 00:05; a late run meeting the turn is one degraded line, not 428 failures. 2
(7834e740e, prod twin 5325db02f): the worker serves `inputs` on /status.json (status.ts INPUT_FORMATS); both
sync-inputs scripts (one copy, `deploy/lib/input-formats.sh`) refuse an input format the deployed worker does not list,
with a plain `FAILED <key>: …` line; prod's first seed needs `FEEDS_WORKER_NOT_LIVE=1`. Staging worker with `inputs`
deployed 2026-11-12 00:08 UTC. 3 (ecd2de06a): obs.json keeps the last rows of stations not rewritten; the 08MF0xx 404
did not reproduce.

## Work (left)
Read a week of /status (the archive has the run log) for item 1: no whole-run gauge failure at the day turn.

## Acceptance
- A week of status shows no whole-run gauge failure at the day turn.
- Uploading a newer-format input before its worker fails at upload with a plain message.

Depends: T550, T558 (overlap). Size: S–M. Owner: no.
