# T198 — SEO hosting redirect and measurement setup

Status: OPEN.

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 2, 4 and 8.
## Work
Hosting config: canifishthis.com and www -> 301 to https://canifishthis.ca (path kept), HSTS, cache headers, a real 404
page; wire the Search Console / Bing verification tokens the owner supplies; choose cookie-less analytics; commit a
monthly measurement checklist.
## Acceptance
curl -I shows the 301s and the canonical host; Search Console shows the sitemap read; the checklist is committed.

Depends: T86 hosting, T194. Size: S. Owner: yes (registrar/DNS, Search Console and Bing accounts). Default: owner steps are in OWNER-TODO.md.
