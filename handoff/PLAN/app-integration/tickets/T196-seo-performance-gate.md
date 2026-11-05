# T196 — SEO page performance budget and Lighthouse CI

Status: DROPPED — merged into T409 (2026-11-05 audit; the work continues there) — was: PARTIAL — 21a2d17ff, b1a5c7fa8 (+ 1c4dd582, 435ce2d8); budget gate passes on the real release; Lighthouse: hubs/About 100, water pages fail (hand-over LCP/CLS) -> T405.

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 6.
## Work
Gate in the release step beside T129's WEB_BUDGET: HTML <= 30 KB gzip, CSS <= 10 KB gzip, no blocking JS; Lighthouse CI
mobile run on sample pages (LCP <= 1.8 s, CLS <= 0.05, Performance >= 95, SEO 100).
## Acceptance
The release fails when a budget is exceeded; the numbers are recorded.

Depends: T193. Size: S. Owner: no. Default: targets as in the plan.

## Outcome (committed 1c4dd582, 435ce2d8)
- `budget.ts`: html <= 30 KB gzip, p95 <= 16 KB gzip, css <= 10 KB gzip, critical css <= 2 KB, no blocking script; `buildAll` returns the
  problems and `pnpm seo` fails on any. `budget.json` records the numbers; `lighthouserc.json` (mobile, simulated 4G, LCP <= 1.8 s, CLS <=
  0.05, TBT <= 200 ms, Performance >= 95, SEO 100, A11y >= 95) is generated for sample built pages.
- Fixture numbers (39 waters): largest page 5,072 B gzip, p95 3,909 B, css 791 B, critical css 281 B. Mutation tests: oversized page,
  fat css, blocking script each fail.
- NOT run: `lhci autorun` (Lighthouse needs Chrome and is the release job's step; the CLI is not a workspace dependency).

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

## Lighthouse on staging (2026-11-05, lighthouse 12.8.2 mobile)
/region/2 and /about: Performance 100, A11y 100, LCP 1.3-1.4 s, CLS 0 (Cloudflare's injected script no longer costs us).
Water pages: /water/vedder-river Perf 45, LCP 38.9 s, CLS 0.154; /water/cowichan-river Perf 40, LCP 3.4 s, CLS 0.718 — the app
taking over the prerendered pane (T269) shifts it and paints a later largest text. Ticket T405. SEO 58-61 on staging only from
the staging noindex/robots (expected).
