# T195 — SEO region hubs, weekend and changes pages, share images

Status: DONE — 21a2d17ff, b1a5c7fa8 (+ 1c4dd582, 435ce2d8); hubs, weekend, season-changes, /changes (snapshot kept between releases). Raster share images + species season calendar moved to T406.

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 1 and 5.
## Work
Region hub pages linking every water; "open this weekend", season-start calendar, "rule changes this year" from the
release diff; a generated share image per water.
## Acceptance
Every water page is reachable from a hub in two clicks or fewer; dated pages state their release.

Depends: T193. Size: M. Owner: no. Default: regenerated with each release.

## Outcome (committed 1c4dd582, 435ce2d8)
- Per region (those with waters): `/region/<n>` (every water, one click), `/region/<n>/open-this-weekend` (statuses for the coming
  Sat/Sun from the app's presenter for those days), `/region/<n>/season-changes` (opens/closes in the next 60 days, read from the strip's
  year runs; WATER-level, not by species), and `/changes` (rule changes vs the previous release's `snapshot.json`; absent without one).
  Every page states its release and as-of date. Footer on every page links all region hubs.
- NOT done: a species-by-region season-start calendar (needs the per-species year strips, not the water strip); raster share images.

## Hub split 2026-10-27
A region over 400 waters lists A-Z letter pages (`/region/N/waters-<letter>`, kind "letter", in the sitemap) instead of every water; the
weekend pages cap each list at 250 with a pointer to the hub. Real build (19,718 pages): largest page now 7.6 KB gzip (was region 6 hub
27.5 KB, weekend 27.1 KB). `budget.json` gained a `quality` block (19,718 unique titles and descriptions, 0 empty, 0 descriptions over 155,
23 titles over 60: long names are not cut, see T192) and the build fails if any title or description repeats. Test: site.test.ts (hubSplitAt).

## Staging 2026-10-28
Live on https://staging.canifishthis.ca with release r-46be3c135e68 (19,774 pages, budget clean; v1 redirects 19,147 water /
162 region / 22 home, each one 301, spot-checked). Lighthouse mobile: A11y 100, CLS 0, Performance 69-76 from Cloudflare-injected
bot/analytics script (zone setting, owner). Still PARTIAL: production deploy and Search Console are the owner's. Record, findings
and redo steps: handoff/PLAN/seo/staging.md; deploy scripts: deploy/staging/.

## First real-release build + staging (2026-11-05, 21a2d17ff, b1a5c7fa8)
Release r-5a5999c94588 (LATEST), built from a clean worktree at b1a5c7fa8 (export:web:staging + `build-seo.mjs --app`, 4 threads,
872 s): 19,927 pages (19,635 waters), largest 13,184 B gzip (/region/3/season-changes), p95 10,298 B, css 4,686 B, critical 284 B,
budget problems 0, 19,927 unique titles + descriptions (84 titles > 60: names are never cut). Slug map committed (+382 waters, none
moved, 19,864). v1: 19,147 -> water / 162 -> region / 22 -> home. Deployed to https://staging.canifishthis.ca (version 20c0a32b),
smoke 18/18. Verified there: sitemap index + sitemap-1.xml parse, 19,928 `<loc>` (pages + `/`), all https://canifishthis.ca, no
slash, no dupes; 150 random sitemap URLs 200 with self-canonical and parseable JSON-LD; `/region/2/`, `/about/?x=1`,
`/water/vedder-river/` -> ONE 301 (was 307); `/waterbody/930-055749/` -> /water/san-juan-river, `/waterbody/328961697/` ->
/region/7, `/waterbody/100/` -> /; removed slug `/water/aliceville-trib-1` -> 301 /region/2; `/region/6/waters-a` (A-Z pages now
lowercase); favicon, /about, /privacy, topic + weekend + season-changes 200; unknown -> 404 page.
Fixed in this pass: trailing slash = permanent 301 everywhere (worker runs first for `/*/`); host worker for .com/www generated
and held to `redirectFor`; snapshots kept between releases (`app/dist/seo-snapshots/`, `--snapshot-only`, automatic
`--previous`) so /changes and sitemap lastmod work; no /changes page when nothing changed; prod wrangler + guarded
`deploy/prod/site.sh` (owner runs). Record: handoff/PLAN/seo/staging.md "Phase C".
