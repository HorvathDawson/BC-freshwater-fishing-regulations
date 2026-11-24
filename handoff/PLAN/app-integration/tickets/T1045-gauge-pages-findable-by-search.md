# T1045 — Gauge pages should come up for "<river> river forecast / level / flow" (0.9.0)

Status: DONE in PR #84 (review fixes in; independent re-review pending) — 859b10591, fc3626905, 4af1ba636, 0922cb1f3
`gaugeClient.ts`) to merge, then is built from main in its own PR.

Source: owner 2026-11-23: "we want to make sure our gauge data pages (or condition tab for a river) come up when someone
searches for a river forecast on google."

## Measured on the live site (GET only)
- Gauge pages ARE indexable and in the sitemap: sitemap-1.xml lists 20,340 URLs, 452 of them `/gauge/<station>/<slug>`, plus
  the `/gauges` index. robots allows them. Canonical is the page's own URL (checked on 08MH005).
- Title: "<Gauge name> conditions (<station>)", e.g. "Alouette River Near Haney conditions (08MH005)". H1: the gauge's name
  alone. Neither says flow, level, forecast or river. The gauge name is WSC's ("Alouette River Near Haney", "Near" capitalised).
- Description: "Live flow, the last 7 days, the flow forecast and the weather at the <name> gauge (<station>), BC, for <water>.
  Free, from Water Survey of Canada data." It has the words; the title and H1 do not.
- Visible text of the static HTML: "forecast" once, "flow" 0, "level" 0, "river conditions" 0. The numbers, chart and forecast
  are drawn by script after load ("The live reading, chart and weather need JavaScript"); Google renders script, but the
  static page has no words a crawler can match a query against.
- Structured data: WebSite, WebPage (dateModified), BreadcrumbList. No Place / dataset type naming the river or the gauge.
- Gauge page to waters: "Waters this gauge serves", descriptive anchors ("Alouette River fishing regulations"). Good.
- Water page to gauge: NONE. The water page's static HTML has zero `/gauge/` links; its Conditions view is the app's tab
  (`water-view-conditions`, a button in the pane, state not URL), so a crawler cannot reach a gauge page from a water page, and
  the app's Conditions tab has no URL of its own to index.
- The gauge page's text calls it "conditions", the owner's queries say "river forecast", "river level", "<river> flow".

## Proposed small fixes (build after #73, from main, one PR)
1. Title and H1 in the searcher's words, once each, no stuffing: "<River> river level and flow forecast (<station>)"
   (the water's name where the gauge serves one river, else the gauge's), inside 60 characters (the station is cut last
   today; keep that rule).
2. A visible one-line lede in the static HTML naming what the page shows: "Water level, flow and the 5-day forecast for the
   <name>, from Water Survey of Canada gauge <station>." and the gauge's last built date, in words the page really has.
3. Each water page links its gauge page(s) with descriptive anchor text ("<River> river level and flow, gauge <station>"):
   a plain `<a href>` in the water page's static head of the Conditions section (the pane has no static conditions); never
   hidden, never a keyword list. The app's Conditions tab stays state, not URL; the gauge page is the indexable face of it
   (canonical stays the gauge page).
4. JSON-LD: a Place / RiverBodyOfWater for the gauged river with the station as an identifier, linked from the WebPage
   (`about`), as water pages already do. No fabricated Dataset / rating markup.
5. A gate in the SEO build: every gauge page's title and H1 hold the query words; every water with a gauge links it.

## Added 2026-11-23 (coordinator, from the T1042 review) — hold the build to these
- The title must not say "river" twice: a gauge or water already named "Alouette River" reads "Alouette River level and flow",
  never "Alouette River river level". Build the title from the name with a trailing "River" / "Creek" / "Lake" etc. kept once.
- "5-day forecast" is the WEATHER's horizon (Open-Meteo, 5 days); the flow forecast's horizon varies by gauge and run. Word
  each by its actual horizon (the weather 5 days; the flow forecast as many days as that gauge's run holds), never one
  "5-day forecast" for both.

## Google's guidance to follow (read when building)
developers.google.com/search/docs/appearance/title-link (a descriptive, unique title that names the page, not stuffed);
/appearance/snippet (a per-page description); /fundamentals/seo-starter-guide (descriptive anchor text, crawlable `<a href>`
links, content that is in the HTML); /appearance/structured-data/intro-structured-data. Never keyword stuffing.

## Limits
Google recrawls in days to weeks; ranking for "<river> river forecast" also faces BC River Forecast Centre, WSC and weather
sites: this makes the pages eligible and clear, it does not promise a position. The owner's Search Console can show the
queries once the production site is deployed (the owner's).

## Not done yet
No code change to any gauge-page file until #73 merges (coordinator, 2026-11-23).

## Owner answers on PR #84 (2026-11-23, verbatim)
- On the visible "River gauges for this water" list: "I don't like those there gauges only linked from conditions tab and even there I don't think we should have links. Gauge tabs are kind of separate and from the share. I don't know either way regulations are separate"
- On the home page's hidden link row: "Drop the hidden row (Recommended)"
So: NO water -> gauge link anywhere (not on the regulations view, not from Conditions); gauge pages are reached by share links, the `/gauges` index and the sitemap. The T1011 home row is dropped (T1011's ticket says why).

## Built (0.9.0, PR #84)
- Gauge page title "<gauge name> <words> (<station>)" and H1 "<name> <words>", where the words are what the RELEASE holds for the station
  (`gaugeSubject` of `gauge_clim`): "level and flow", "flow", "level", or "conditions" when it holds nothing. A level-only gauge says
  "level". "Forecast" is NOT in the title or heading: whether the BC River Forecast Centre runs one for a gauge is live data, not in
  the release; the lede and description say it only as "the flow forecast where the BC River Forecast Centre runs one, for as many
  days as that run covers". The weather has its own horizon, `WEATHER_DAYS` (5), the one constant. The gauge's own name already
  holds the water's word, so nothing says "river river" (the build gate refuses a doubled word).
- A visible lede in the static HTML; the gauge's coordinates shown ("at 49.0973, -121.9638").
- Gauge JSON-LD: a Place named by the gauge and its station, with GeoCoordinates, the WebPage's `about`; held to the page by the existing gate.
- Build gate (`gaugeWordsProblems`): title and heading words against the station's parameters, no doubled word, no "forecast" in title or
  heading, a lede, the forecast only hedged. There is NO gate that asks for a water -> gauge link (a test asserts a water page has none).
- Removed after the owner's answers: the water-view gauge list (`pickWaterGauges`, `readWaterGauges`, `useWaterGauges`, the web/native
  anchor pair and their tests) and the T1011 home row (`withHomeLinks`, `homeLinkProblems`).
- Tests proved by mutation: words not from data, hedge removed (the build refuses), "forecast" in a title, doubled word. `T1045-shots/before-after.txt`.
- Not done: a ranking (Google decides; the recrawl is the owner's after the production deploy).
