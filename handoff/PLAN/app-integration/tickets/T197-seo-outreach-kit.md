# T197 — SEO outreach kit (drafts only, no sending)

Status: DONE — 1c4dd582, 435ce2d8; drafts complete and tested; sending is the owner's (OWNER-TODO), after prod is live.

Source: T188 plan, handoff/PLAN/seo/PLAN.md section 7. Owner rule: no spam bots, link schemes or auto-posting.
## Work
Honest copy and templates for BC angling clubs, forums, the Freshwater Fisheries Society, tackle shops (QR counter card)
and one r/bcfishing maker post; a club-list tracker. No scripts, no automated posting, no link schemes.
## Acceptance
The owner reads and sends by hand; every template says the app is a free non-profit and asks for feedback, not links.

Depends: T193 live. Size: S. Owner: yes (sends the messages). Default: drafts only.

## Outcome (committed 1c4dd582, 435ce2d8)
- `handoff/PLAN/seo/outreach/`: README (the rules), club email, FFSBC/fish-and-game email, forum post, one r/bcfishing maker post, tackle
  shop counter card (QR to the region page), and an empty `tracker.csv`. Nothing sent or scheduled; no scripts. `outreach.test.ts` holds
  every template to free + non-profit + feedback + not a legal source, and to never ask for a link, share, vote or review.
- Owner: fill the brackets and send each by hand once the pages are live.

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
