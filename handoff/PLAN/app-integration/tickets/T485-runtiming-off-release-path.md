# T485 — Run timing off the release path: no outside site can block a release

Status: PARTIAL — step 1 done (d948fa0); steps 2–3 open. Owner 2026-11-07.

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
