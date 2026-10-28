# T194 — SEO sitemap, robots, canonicals, structured data

Status: PARTIAL - committed 1c4dd582, 435ce2d8; first real-release build, v1 redirects and owner steps left (see Outcome).

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
