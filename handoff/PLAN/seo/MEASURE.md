# Monthly SEO measurement checklist (T198)

Once a month for the first six months after canifishthis.ca serves the new site, then each quarter. About 20 minutes.
Record the numbers in one row of `measure.csv` beside this file (create it the first time; one row per month).

## Analytics choice (T198 default)
No analytics script on the pages. Self-host-everything rules out third-party scripts, and Cloudflare's injected Web
Analytics beacon is one of the two scripts that took Lighthouse Performance from >= 95 down to 69-76 on staging. What we
read instead needs no script and no cookie:
- Google Search Console and Bing Webmaster Tools (queries, clicks, indexing);
- Cloudflare zone analytics (dashboard → canifishthis.ca → Analytics & Logs → Traffic: requests and visits by path,
  computed at the edge from the requests, nothing added to the page).
If the owner later wants per-page sessions, the only kind that fits the rules is a self-hosted, cookie-less counter
served from our own worker. That is a new ticket, not a default.

## Each month
1. **Search Console → Pages (Indexing)**: indexed vs not indexed. Write down: indexed, "Crawled - currently not
   indexed", "Duplicate, Google chose different canonical", "Not found (404)", "Page with redirect". For the first two
   months the 19,331 old `/waterbody/` URLs should move into "Page with redirect", not into 404.
2. **Search Console → Sitemaps**: `/sitemap.xml` status Success; discovered URLs = the build's page count
   (`app/dist/seo-prod/budget.json` → `measured.pages` + 1 for `/`).
3. **Search Console → Performance (Search results), last 28 days**: clicks, impressions, CTR, average position. Filter
   Page contains `/water/`, then `/region/`, then `/topic/`, and record each. Note the top 10 queries.
4. **Search Console → Core Web Vitals (mobile)**: poor / needs improvement / good URL counts.
5. **Bing Webmaster → Search Performance and Site Explorer**: clicks, impressions, crawl errors.
6. **Cloudflare → Analytics → Traffic**: requests to `/water/*` vs `/` (search landings vs app opens).
7. **Quality**: search the top 20 queries from step 3 yourself; does our page answer each one in its first screen?
   Count wrong-rule reports received this month (feeds worker digest).
8. **Build gates of the last release** (`budget.json`): pages, largest and p95 gzip bytes, `problems` empty,
   titles/descriptions unique. Lighthouse (step below) after any zone setting change.

## Targets (guidance, not guarantees; PLAN.md section 8)
- >= 90 % of submitted URLs indexed within 8 weeks of launch.
- Top 3 for "<water> fishing regulations" on the 20 most-searched waters within 6 months.
- Organic clicks rising month on month.

## Lighthouse (after a zone change, or each quarter)
`npx --yes lighthouse@12.8.2 https://canifishthis.ca/water/vedder-river --form-factor=mobile --only-categories=performance,seo,accessibility --output=json --output-path=./lh.json --chrome-flags=--headless`
Gate (T196): Performance >= 95, SEO 100, Accessibility >= 95, LCP <= 1.8 s, CLS <= 0.05.
