# T188 — Get found in search: an SEO plan and the work it needs

Status: OPEN. Owner 2026-10-24: "how to create bots or something that promote SEO or build a plan to do SEO and get
us up in the search results."

## Principle
Rank by being the best answer to what anglers search ("can I fish the Vedder River today", "Thompson River steelhead
closure", "Kootenay Lake regulations"). No automated posting or link-spam bots: search engines penalise them and they
would put the site at risk. Automation is for OUR pages (generating one good page per water) and for monitoring.

## Plan (ordered)
1. Indexable pages: today the web app is a client-rendered map, which search engines read poorly. Pre-render one static
   page per named water (and per part where it matters): the water's name, region, today's open/closed line, the rules
   in plain words, the book page cited, nearby gauges, a link into the app at that water. Generated from the release at
   publish time (the bundle is the only source), so they refresh with every release.
2. Technical: sitemap.xml (all water pages), robots.txt, canonical URLs, titles/descriptions per water, schema.org
   structured data (Place / BodyOfWater, plus FAQ where the page answers a question), fast first paint (T129/T182
   budgets), mobile-first, HTTPS, clean URLs (https://canifishthis.ca/water/vedder-river).
3. Measure: Google Search Console + Bing Webmaster (owner accounts): submit the sitemap, watch impressions/queries.
4. Content that earns links: "what's open this weekend" per region, opening-day calendars, closures that changed this
   year (from the release diff) — pages people share and fishing clubs/forums link to.
5. Outreach (human): fishing clubs, guides, tackle shops, regional forums, Reddit communities — an honest "free app"
   announcement, not automated posts. App-store listings (T86/T88) with keywords.
6. Local trust signals: an About page (who made it, data sources: the provincial synopsis, ECCC, DFO, PSF), the
   "not a legal document — check the synopsis" line, contact.
Decided 2026-10-24: domain canifishthis.ca (canifishthis.com redirects); water pages show the FULL rules in plain
words + today's line + the book page + "Open in the app" (decisions.md).
Depends: T86 hosting (a domain), T182 (lighter pages). Size: M (pages + sitemap + data) + ongoing.
