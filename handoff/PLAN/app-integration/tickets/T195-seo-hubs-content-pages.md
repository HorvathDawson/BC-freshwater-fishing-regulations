# T195 — SEO region hubs, weekend and changes pages, share images

Status: PARTIAL - built and green, uncommitted (see Outcome).

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 1 and 5.
## Work
Region hub pages linking every water; "open this weekend", season-start calendar, "rule changes this year" from the
release diff; a generated share image per water.
## Acceptance
Every water page is reachable from a hub in two clicks or fewer; dated pages state their release.

Depends: T193. Size: M. Owner: no. Default: regenerated with each release.

## Outcome (built and green but UNCOMMITTED because the sandbox refused the commit step; commit the listed paths, then mark DONE with the hash)
- Per region (those with waters): `/region/<n>` (every water, one click), `/region/<n>/open-this-weekend` (statuses for the coming
  Sat/Sun from the app's presenter for those days), `/region/<n>/season-changes` (opens/closes in the next 60 days, read from the strip's
  year runs; WATER-level, not by species), and `/changes` (rule changes vs the previous release's `snapshot.json`; absent without one).
  Every page states its release and as-of date. Footer on every page links all region hubs.
- NOT done: a species-by-region season-start calendar (needs the per-species year strips, not the water strip); raster share images.
