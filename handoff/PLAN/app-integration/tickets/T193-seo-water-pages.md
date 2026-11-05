# T193 — SEO static water pages and About page

Status: DONE — 21a2d17ff, b1a5c7fa8 (+ 1c4dd582, 435ce2d8); real-release pages on staging. Gauges: the water page IS the app (T269), its Conditions tab shows them; wiring the build into the prod release = T240/T404 runbook.

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 1 and 5.
## Work
app/tools/build-seo.mjs renders /water/<slug> HTML (full rules in plain words, today's line, book page, gauges, Open in
the app, not-a-legal-document line) and /about; it runs in the release job after the bundle is cut.
## Acceptance
Pages read without JS; page count == waters with rules; pages refresh on every release.

Depends: T192. Size: M. Owner: no. Default: no maps or interactive parts on the page.

## Outcome (committed 1c4dd582, 435ce2d8)
- `app/tools/build-seo.mjs` (`pnpm seo --release-dir <folder>` | `--fixture`) loads `tools/seo/*.ts` through vite's SSR loader (vitest's own
  engine; knip allows it) and writes `app/dist/seo` (gitignored). Page = h1, "Can I fish X today?" + the verdict, as-of date and release id,
  the full rules by part (presenter wording, synopsis page linked per rule), connected waters, more in the region, centre point, the
  not-a-legal-document line, prominent "Open X in the app" (`/?regs=<item>`, the web start link). No JS, no map.
- Fixture run 2026-10-27: 39 water pages for 39 waters with rules, plus About, 8 regions x 2 hubs. A test checks each page's verdict class
  against the app's status for its parts, with a planted wrong verdict caught.
- NOT done: "nearby gauges" (gauges are not in the regs bundle files; needs a gauge read from the release) and the optional client refresh
  script (the static text is already right on release day). Wiring `pnpm seo` into the publish job is the release owner's step (publish.py
  is human-run).
- Paths: app/tools/build-seo.mjs, app/tools/seo/{html,site,build}.ts, site.test.ts, app/seo/site.json, app/knip.jsonc, app/package.json.

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
