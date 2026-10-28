# T198 — SEO hosting redirect and measurement setup

Status: PARTIAL - committed 1c4dd582, 435ce2d8; first real-release build, v1 redirects and owner steps left (see Outcome).

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
