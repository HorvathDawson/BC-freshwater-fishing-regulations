# T1011 — Website: big waters have no page (Columbia, Kootenay, Similkameen, Kettle, Peace rivers; Chilko Lake); the home page has no crawlable links

Status: PARTIAL — 54778147e: parts 1 and 2 built (lane L2, 2026-11-22). left (Claude): part 3 (rebuild index-requests-top-100.md from Search Console once it has weeks of data; move the Columbia, Kootenay, Similkameen, Kettle and Peace in) and the former name under the name in the app's pane (needs `alias` in the core region slices: a pipeline change).

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

### Cause found 2026-11-22 (read from the published release r-741815c6090d, `water` file)
**(a) 156 waters partly outside B.C. are dropped by a wrong test.** `PAGE_ORDS_SQL`
(`app/packages/data/src/regs/pageable.ts:15`) keeps a water only when `i.outside_bc = 0`. But `item.outside_bc` is the
COUNT of the water's sections outside B.C. (ANSWERS-SPEC: "N sections outside B.C."), not a flag. In this release:
outside_bc 1 = 135 waters, 2 = 14, 3 = 4, 4/5/6 = 1 each; all 156 shown waters with a keyed part have
`outside_bc < sections` (none is wholly outside). Examples: Columbia (2 of 9 sections), Kootenay, Peace,
Similkameen, Kettle (3), Flathead, Ashnola, Hay, Sumas rivers. Since 1c4dd5827 (2026-10-27) none of them had a page.
- **Fix:** `i.outside_bc < i.sections` (drop a water only when every section is outside); `waterModel` already skips the
  parts whose page is `outside`. The same rule gates the app's gauge Share (`waterHasPage`, T530), so gauges on these
  rivers start offering Share too. Pin it: a test with a water half outside B.C. gets a page; a wholly outside one
  does not. Expect ~156 new pages (sitemap, region hubs, the slug/title uniqueness checks; the build fails on a
  clash, so read its problems list). Check that the page says the part outside B.C. is not covered.
- Then redo the top of `index-requests-top-100.md`: Columbia, Kootenay, Similkameen, Kettle, Peace move in.

**(b) Renamed waters never print their former name.** Chilko Lake is `wbk:329079684` "Tŝilhqox Biny"
(`/water/tsilhqox-biny`), Puntzi Lake is "Bendziny" (`/water/bendziny`), Alces Lake is a "Moose Lake"
(`wbk:329247797`). The release keeps the old names as aliases (`alias`: "CHILKO LAKE", "Chilko Lake (formerly)"), but
the page's text, title and description never contain them, so a search for "Chilko Lake fishing" cannot match.
- **Fix:** where a water has a former-name alias, the page says "formerly Chilko Lake" under its name and in the meta
  description (and the JSON-LD `alternateName`). Additive only; the official name stays as sourced (owner rule: never
  rename or drop names without asking).
- Green Lake (70 Mile House), Sumas River: Green Lake has 7 waters of that name, all with pages; Sumas River is one of
  the 156 in (a).

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

## Done 2026-11-22 (lane L2, 54778147e)
- **(a) the 156:** `PAGE_ORDS_SQL` is `i.outside_bc < i.sections` (`pageable.test.ts`: in B.C. and partly outside get a page,
  wholly outside / unshown / keyless do not; mutation-checked with `<=`). Bare build on r-741815c6090d: **19,610 -> 19,766
  waters (+156, none lost), 20,338 -> 20,498 pages** (+156 water pages, +4 gauge pages on those rivers); no title,
  description or slug clash. Columbia `/water/columbia-river`, Kootenay `kootenay-river`, Similkameen `similkameen-river`,
  Kettle `kettle-river`, Peace `peace-river`, Sumas `sumas-river`, Flathead `flathead-river`. The parts outside B.C. are left
  off as before (`waterModel` skips a part whose page is `outside`); NOT checked in a rendered water page (bare build).
- **(b) former names:** 9 waters carry a core `alias` "<name> (formerly)" (Chilko, Tatlayoko, Eagle, Stum, Anah, Alexis,
  Yohetta, Puntzi, Chilcotin lakes). Their title ("Tŝilhqox Biny (formerly Chilko Lake) fishing regulations"), meta
  description, JSON-LD `alternateName` and their line on the region / letter / topic lists ("formerly Chilko Lake") say it.
  Not under the name in the pane itself: the pane is the app, and the core region slices the app reads on a tap carry no
  `alias` table (a read there would fetch the whole 1.5 MB core), so that needs the slices to carry it (pipeline). Alces
  Lake's alias ("ALCES LAKE", the synopsis's name for that Moose Lake) is not a former name and is not printed.
- **2. home links:** `web-shell.mjs` `withHomeLinks`: a static `<nav id="site-links">` after `#root` with plain links to
  /gauges, /region/1-8, /about, /privacy (not /changes: no page when a release changed nothing). It sits below the
  full-window root, so it never shows over the map; without script the root is hidden and the row shows. Water pages take
  it out (`appDocument`). The SEO build refuses a row naming a page it did not make or missing a region (`homeLinkProblems`,
  pinned in `site.test.ts`). Lighthouse not re-run (no deploy from a lane).

## Dropped 2026-11-23 (T1045, owner: "Drop the hidden row (Recommended)")
The home page's crawlable link row (`#site-links`: `withHomeLinks`, `homeLinkProblems`, and their tests) is REMOVED. It sat clipped under
the app's root and was `inert` with script: links meant only for crawlers, which Google's spam policies ("Hidden text and links",
developers.google.com/search/docs/essentials/spam-policies#hidden-text-and-links) name. The sitemap already lists the gauges, regions,
About and Privacy; the pages stay reachable that way.
