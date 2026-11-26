# SEO: what is built and how to measure it

## What is built

- **The pages:** `app/tools/build-seo.mjs --release-dir data/release/<r-id> --app <web export> --threads 4` writes the static
  water, region, topic and gauge pages, `sitemap.xml` and `budget.json` (pages, largest and p95 gzip bytes, `problems`,
  unique titles and descriptions) beside the web export; it has run at about 20,000 pages with the largest page 13 KB gzip.
  A slug map is committed (`app/seo/slug-map.json`); `--update-slugs` rewrites it. Old v1 `/waterbody/<id>/` URLs answer
  one 301 each to the water or its region, and a trailing slash on any path is one 301.
- **Staging:** `deploy/staging/site.sh` (or `site-all.sh`) merges the app and the SEO pages into a staging site at
  https://staging.canifishthis.ca, data from https://data-staging.canifishthis.ca. Staging is never indexable:
  `robots.txt` is `Disallow: /` and every response carries `X-Robots-Tag: noindex, nofollow`, while canonicals stay on
  https://canifishthis.ca and the "Open in the app" link is relative. `deploy/staging/upload.sh <release>` fills the
  staging bucket (it spares `feeds/**` and `private/**`) and `node app/tools/smoke-staging.mjs` checks the site;
  `--production` checks production. `app/tools/check-public.mjs` checks that `private/` is not served and no secret is in
  the site folder or the public bucket objects (the data worker's own rate limit may answer 429 on a few feed files).
- **Outreach drafts:** `handoff/PLAN/seo/outreach/` holds five templates, a README and a tracker, drafts only; the owner sends
  every message by hand (`app/tools/seo/outreach.test.ts` pins the folder).
- **Index requests:** `index-requests-top-100.md` is the ordered list of URLs to submit by hand in Search Console.

Findings for production: the Cloudflare managed `robots.txt` (a zone setting) is prepended to ours, opens with
`Allow: /` and blocks AI crawlers, so it is the owner's call whether to keep it; Cloudflare's injected bot and RUM scripts
were what cut Lighthouse Performance on staging to 69 to 76; `/favicon.ico` answered 404 on every page; `www.canifishthis.ca`
301s to `/` and drops the path (an owner redirect rule).

## Measure monthly

Once a month for the first six months after the site goes live, then each quarter; about 20 minutes. Record one row per
month in `measure.csv` beside this file (create it the first time). There is no analytics script on the pages (the
self-host rule, and Cloudflare's beacon cost Lighthouse points); a self-hosted cookie-less counter on our own worker would
be a new ticket.

1. **Search Console, Pages (Indexing):** indexed, "Crawled - currently not indexed", "Duplicate, Google chose different
   canonical", "Not found (404)", "Page with redirect" (the old `/waterbody/` URLs belong in the last, not in 404).
2. **Sitemaps:** `/sitemap.xml` status Success; discovered URLs equal `budget.json` `measured.pages` plus 1 for `/`.
3. **Performance, last 28 days:** clicks, impressions, CTR, average position, filtered by Page contains `/water/`, then
   `/region/`, then `/topic/`; the top 10 queries.
4. **Core Web Vitals (mobile):** poor, needs improvement, good URL counts.
5. **Bing Webmaster:** clicks, impressions, crawl errors.
6. **Cloudflare, Analytics, Traffic:** requests to `/water/*` against `/` (search landings against app opens).
7. **Quality:** search the top 20 queries yourself; does our page answer each in its first screen? Count wrong-rule reports
   (feeds worker digest).
8. **Build gates of the last release:** `budget.json` `problems` empty, titles and descriptions unique.

Targets (guidance): 90 % of submitted URLs indexed within 8 weeks of launch; top 3 for "<water> fishing regulations" on the 20
most-searched waters within 6 months; organic clicks rising month on month.

Lighthouse after a zone change or each quarter:
`npx --yes lighthouse@12.8.2 https://canifishthis.ca/water/vedder-river --form-factor=mobile --only-categories=performance,seo,accessibility --output=json --output-path=./lh.json --chrome-flags=--headless`.
Gate: Performance 95 or more, SEO 100, Accessibility 95 or more, LCP 1.8 s or less, CLS 0.05 or less. Measured on staging
after the fish thumbnails: water pages Performance 77 to 88 (LCP 2.1 to 2.5 s), region hubs 97 to 100.
