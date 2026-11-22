# T1011 — Website: big waters have no page (Columbia, Kootenay, Similkameen, Kettle, Peace rivers; Chilko Lake); the home page has no crawlable links

Status: OPEN, next release batch.

Source: owner 2026-11-22 asked for the top 100 pages to submit for indexing, ranked by how much BC searches for fishing
each water (`handoff/PLAN/seo/index-requests-top-100.md`, written that day). The research found these two gaps.

## 1. Waters people search for that have no page
`https://canifishthis.ca/water/<slug>` returns 404, and the live sitemap (20,340 URLs, 2026-11-22) has no page under
any slug for: **Columbia River, Kootenay River, Similkameen River, Kettle River, Peace River, Chilko Lake.** (Chilko
River, Columbia Lake and Kettle Creek do have pages.) The Columbia, Kootenay, Kettle and Similkameen have **gauge**
pages, so their gauge pages point at a water that has no page. Also check: Green Lake (70 Mile House), Sumas River,
Alces Lake, Puntzi Lake.

Where to look: `buildSite` (`app/tools/seo/site.ts:115-119`) keeps a water only when `waterModel` (`model.ts:308`)
returns a model with `region >= 1`. `waterModel` returns null when no part is live (`liveParts`) or every live part is
`outside` B.C. A big river that crosses a border (Columbia, Kootenay, Similkameen, Kettle, Peace all do) may have every
part read as `outside`, or be split into items whose names differ. Find the cause for each water, say whether the app
itself shows it (if the app is missing it too, it is a data defect, not an SEO one), and fix it at the source.
Acceptance: each listed water has a page in the sitemap, or this ticket says why it must not have one.

## 2. The home page leads crawlers nowhere
`/` is the JavaScript map. Its HTML has no plain `<a href>` to `/gauges`, the region pages or any water, so Google
reaches the site's pages only through the sitemap. Add a small crawlable footer or a no-script block on `/` that links to
`/gauges`, every `/region/N`, `/about` and `/changes`. It must not show over the map (visually hidden is fine only if
Google's rules allow it; a real footer link row in the page's static HTML is the safe choice). Pin it in
`site.test.ts`, keep the Lighthouse SEO score no lower, and include it with T1010's region-page work if that is easier.

## 3. Redo the ranked list from real data
Once Search Console has a few weeks of impressions, rebuild `index-requests-top-100.md` from its Performance report
(queries and pages), not Google Trends.

Risk: low (part 2), unknown until the cause is found (part 1). Size: S–M. Owner: none.
