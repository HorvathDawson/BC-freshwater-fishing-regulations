# T198 — SEO hosting redirect and measurement setup

Status: BLOCKED (owner; merged into T409, 2026-11-05 audit) — was: BLOCKED (owner) — 21a2d17ff, b1a5c7fa8 (+ 1c4dd582, 435ce2d8); all code/config done (host worker, prod wrangler, HSTS, 301s, measurement checklist seo/MEASURE.md); left: owner steps in OWNER-TODO (www.canifishthis.ca rule drops the path, Search Console/Bing, prod deploy).

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 2, 4 and 8.
## Work
Hosting config: canifishthis.com and www -> 301 to https://canifishthis.ca (path kept), HSTS, cache headers, a real 404
page; wire the Search Console / Bing verification tokens the owner supplies; choose cookie-less analytics; commit a
monthly measurement checklist.
## Acceptance
curl -I shows the 301s and the canonical host; Search Console shows the sitemap read; the checklist is committed.

Depends: T86 hosting, T194. Size: S. Owner: yes (registrar/DNS, Search Console and Bing accounts). Default: owner steps are in OWNER-TODO.md.

## Outcome (committed 1c4dd582, 435ce2d8)
- `hosting.ts` emits with every build: `_headers` (HSTS `max-age=31536000; includeSubDomains`, nosniff, referrer policy; assets immutable 1 y;
  HTML 5 min revalidate), `_redirects` (removed slug -> region page, 301), `redirects.json` (canifishthis.com, www.canifishthis.com,
  www.canifishthis.ca -> canonical, path + query kept) and a real `404.html` (noindex, links the regions). `redirectFor` is the pure
  spec the edge is checked against (test: com/www/http/trailing slash/removed slug/canonical). Preload is left to the owner.
- STILL OWNER/LATER (stay in OWNER-TODO): registrar/DNS, the edge rule itself, Search Console + Bing accounts and tokens (paste into
  `app/seo/site.json`), IndexNow, the analytics choice, the monthly measurement checklist, `curl -I` against the live host.

## v1 redirects (2026-10-27)
`legacy.ts` emits `legacy-redirects.json` + `_worker.js` (Cloudflare `_redirects` caps at 2,000 rules; 19,331 v1 paths). Hosting
must set `run_worker_first = ["/waterbody/*"]`; check with `curl -I https://canifishthis.ca/waterbody/930-055749/` -> one 301 to a /water/ page.
Counts and rationale: T192, seo/PLAN.md 2b.

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
