# T851 — The DFO salmon feed reads "stale" for every region after one failed daily fetch

Status: DONE — the app half: the card's 'Checked with DFO' names the oldest last good read when a region's fetch failed in the run (`lastRead`, core `salmonFeed.ts`; runs within 15 min of each other still say the run's time), and `SalmonFeedRegion.stale`'s doc comment is corrected. Code 94f388d0a; worker/Python half d66e0bfdb.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T851 | The DFO salmon feed reads "stale" for every region after one failed daily fetch (owner: big issue) | DONE — worker/Python fix d66e0bfdb; the app's 'Checked with DFO' line names the last good read and the stale doc comment is corrected (0.9.0 batch, L8) | d66e0bfdb, 94f388d0a | owner 2026-11-18 13:43 |

Seen 2026-11-18: at the 14:15Z run every DFO region page answered 5xx (`http_5xx`, "fetch failed 1× in a row; the last
answers stand"); the staging feed's index.json then marked every region stale, though the last good check was ~18 h
old and the pages answered 200 again by 20:16Z. The next scheduled try was a day later. A redeploy's job run cleared
it (feeds.sh, 2026-11-18 ~13:20 PT).

## To decide and fix (feeds worker, app chat)

1. A transient 5xx should be retried inside the run (bounded backoff) and again later the same day, not left for 24 h.
2. "Stale" should mean the data is old enough to doubt (by age of the last good answer, against the feed's own cadence),
   not "the last fetch failed once". Whatever the app shows for a stale DFO region must stay honest.
3. Python twin (pipeline/regs/dfo_salmon live) and the parity tests stay in step; the owner email says what happened.

## Done (branch worktree-agent-a0196bdd50aa909fd, not merged or deployed)

- **Stale = age, not one failure.** A region is `stale` only when its last good read (`checked`) is older than
  `STALE_AFTER_HOURS = 30` at a run whose fetch failed — the app's own DFO line (`STALE_AFTER.dfo`, daily cron + 6 h).
  `failures` (in a row) still says a check failed; `issues.unread` still names the region; /status lists every failed
  region (`dfoHeld`, feed `degraded`); the urgent owner email (`dfo-fetch:<slug>:<checked>`, once per outage) now fires
  when the region turns stale (was: the 2nd failure in a row). Worker `dfo/live.ts` + Python `live.py` alike.
- **In-run retry:** `fetchRegion` / `fetch_page` wait 5, 15, 45 s between the same 4 tries (~1 min; was 2, 4, 8 s; no
  more subrequests). The worker stops at once when its breaker is open (no waiting on `circuit`).
- **Same-day retry:** a DFO run that leaves a region unread (or throws) writes `private/feeds/dfo/retry.json`; a new
  hourly cron `50 * * * *` (both wrangler files) runs DFO for those regions only until they are read (file deleted) or
  12 tries failed; a regular run restarts the count. An hour with nothing due is one R2 read.
- **App:** contract unchanged (same fields). The card's "(Region X could not be read; its earlier rules shown)" now
  appears only once the region is past 30 h. Not changed (app/packages is another lane's): the card's "Checked with DFO
  <at>" still names the run time when a region's fetch failed within the 30 h; `SalmonFeedRegion.stale`'s doc comment
  in `packages/core/src/salmonFeed.ts` still says "the last read failed".
- Tests: `test/dfo.stale.test.ts` (new), `dfo.worker`, `status`, `scheduled`; `pipeline/tests/test_dfo_live.py`.


State 2026-11-22 (stopped for credits): review fixes done (6 items, see PR #43); left: nothing from the review, device looks/shots still owed; branch fix/T970-ui-smalls.
