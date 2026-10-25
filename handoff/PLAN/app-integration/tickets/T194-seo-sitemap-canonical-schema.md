# T194 — SEO sitemap, robots, canonicals, structured data

Status: OPEN.

Source: T188 plan, handoff/PLAN/seo/PLAN.md sections 2 and 3.
## Work
sitemap.xml (chunked; lastmod only on changed content), robots.txt, canonical + title + description per page, JSON-LD
(BreadcrumbList, WebPage, Place/BodyOfWater, FAQ only where the answer is visible text).
## Acceptance
Build gate: sitemap count == page count; every canonical is the https canifishthis.ca URL; JSON-LD parses and every
fact in it appears in the page HTML.

Depends: T193. Size: M. Owner: no. Default: FAQ markup dropped if it ever mismatches the page.
