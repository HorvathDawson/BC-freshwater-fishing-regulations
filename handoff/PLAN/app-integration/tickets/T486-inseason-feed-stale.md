# T486 — Staging in-season feed stopped refreshing (last fetch 2026-11-06 15:28 UTC)

Status: OPEN — found 2026-11-07 by the staging smoke (feeds fresh: in-season < 30 h).

`feeds/inseason/index.json` on staging: fetchedAt 2026-11-06T15:28:52Z (changedAt 10-29). The source page
(www2.gov.bc.ca fishing-regulations) answers 200 from the Mac. The feeds worker's `45 ` cron (app/workers/feeds/src/
index.ts:479) has not written since.

## Do
- Read the worker's logs / last-run record (`wrangler tail`, or its own status) for the inseason run; find why it stops
  (gov.bc.ca blocking Cloudflare egress? a parse refusal? the cron itself?).
- The failure must be reported (no-silent-fallbacks: reportFailure / owner email), not just found by the smoke.
- Fast-fail (AGENTS 20a): the feeds-fresh check is cheap; run it before a release upload, not only after.
