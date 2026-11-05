# T194 — SEO sitemap, robots, canonicals, structured data

Status: DONE — 21a2d17ff, b1a5c7fa8 (+ 1c4dd582, 435ce2d8); sitemap/canonicals/JSON-LD verified on staging against the real release.

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 2 and 3.
## Work
sitemap.xml (chunked; lastmod only on changed content), robots.txt, canonical + title + description per page, JSON-LD
(BreadcrumbList, WebPage, Place/BodyOfWater, FAQ only where the answer is visible text).
## Acceptance
Build gate: sitemap count == page count; every canonical is the https canifishthis.ca URL; JSON-LD parses and every
fact in it appears in the page HTML.

Depends: T193. Size: M. Owner: no. Default: FAQ markup dropped if it ever mismatches the page.

## Outcome (committed 1c4dd582, 435ce2d8)
- `site.ts`/`jsonld.ts`/`html.ts`: `sitemap.xml` index + `sitemap-N.xml` (<= 50,000), `robots.txt` (Disallow /api/, /data/), self-canonical
  `https://canifishthis.ca<path>` (no trailing slash), title <= 60, unique description <= 155, OG/Twitter tags. JSON-LD: WebSite, WebPage
  (dateModified), BreadcrumbList, Place (+River/LakeBodyOfWater by certain kind, geo), FAQPage ("Can I fish X today?" with its visible
  answer), Organization on About. `lastmod` moves only when a page's content hash moves (snapshot.json from the previous release).
- Gates that throw: JSON-LD fact not on the page / missing field, duplicate title or description, a water no hub links, sitemap count !=
  page count. Tests plant each fault. Verification tokens come from `app/seo/site.json` (null = nothing rendered).
- Share cards are SVG (`/share/<slug>.svg`); a raster PNG for Facebook/X needs an image library (no new dependency added): owner call.

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
