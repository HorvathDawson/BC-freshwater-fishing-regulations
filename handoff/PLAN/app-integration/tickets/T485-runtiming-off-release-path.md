# T485 — Run timing off the release path: no outside site can block a release

Status: DONE in code (step 3, 8ef2eebf4; the first live refresh remains) — was PARTIAL: step 1 done (d948fa0a9); step 2 built (cb20055c6) and in use: a kept copy (staging private/runtiming/src/sources.tar.gz, fetched 2026-10-29, seeded by hand 2026-11-08 while open.canada.ca was down) let r-25a89ce9f8e6 reach staging without open.canada.ca. left (Claude): see the workflow save its own copy after a successful refresh, and step 3 (the refresh as its own worker-started monthly job; a failed refresh emails the owner and never blocks a release). Owner 2026-11-07.

On 2026-11-07 open.canada.ca was down for hours. Every staging upload starts `feeds-runtiming.yml`, which re-downloads
DFO NuSEDS (open.canada.ca) and the PSF pages, and the release pointer waits for it — so r-1b4f54fb6ed9 could not reach
staging, though its tiles (and so its run timing) were unchanged. NuSEDS changes about once a year.

## Do
1. **DONE (d948fa0):** `deploy/staging/upload.sh` skips the workflow when `runtiming/index.json` already names the
   release's tiles and the binding base is byte-identical (said in the log).
2. **Last good sources on our storage:** the workflow keeps the last successful NuSEDS + PSF download in R2
   `private/runtiming/src/` with its fetch date; a release-triggered run builds from that copy. The report and the
   feed state the source date (no silent fallback: the date is shown, and a copy older than N months fails loudly).
3. **Refresh is its own job:** fetching new sources runs monthly (worker-started, never a GitHub cron — AGENTS / memory
   no-github-schedules) or on the owner's ask; a failed refresh emails the owner and never blocks a release.
- Fast-fail (AGENTS 20a): none needed for step 1; for 2, a test that a release-triggered run makes no outside request.

## Notes
- 2026-11-22, step 3 DONE (8ef2eebf4): the weekly worker check (`runTimingCheck`, src/runtiming.ts `checkSourceRefresh`)
  reads `private/runtiming/src/FETCHED` (the workflow now writes it beside the kept copy, after the run passed) and starts
  feeds-runtiming.yml with `upload: true, refresh: true` once it is 30 days old or missing (the hand-seeded copy has none:
  the first week after deploy starts one refresh). One start per month (`refresh:<YYYY-MM>`, its own record under
  private/feeds/dispatch-refresh/), redone once if still due after 6 h, then the owner is told weekly it did not finish;
  without the token the email carries the button. Not asked when the NuSEDS check just started its own refreshing run or
  failed (open.canada.ca down would fail a refresh too). No new cron, never a GitHub schedule; release runs never refresh
  and are only queued behind a refresh by the workflow's concurrency group. Tests: dispatch.test.ts (5 cases).
- Left: the first live refresh (needs the deployed worker + token); a refresh run that FAILS is noticed by the next weekly
  check (up to a week), not by the run itself.
