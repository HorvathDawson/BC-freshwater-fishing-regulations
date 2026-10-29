# T269 — Water pages ARE the app (prerendered regs pane), the separate text site retires

Status: DONE (f0357560b, e9ae79c82, 9573da40c; owner approved 2026-10-29: "yes that sounds good lets change the seo stuff to this").

Source: owner 2026-10-29: "make it so that all the regulation/condition pages that open when we click on a item double as seo
pages … i just dont want users here they should be in the app instead"; "it shouldnt change look of pane".

## Work
1. The web app serves `/water/<slug>` (slugs = committed `app/seo/slug-map.json`, unchanged) and opens that water's regs pane,
   exactly as a tap would; the URL updates when a named water is opened in the app (unnamed waters keep today's URLs, unindexed).
2. Build step prerenders, per named water, the app's own HTML shell + that water's regs pane markup (the same components/words,
   no second template) + title/description/canonical/JSON-LD from tools/seo (model.ts, jsonld.ts); the app hydrates and the
   map loads behind it. No visual difference from a tapped pane (screenshot diff gate).
3. Region/topic hubs and the sitemap stay (reuse site.ts/topics.ts) but link to `/water/<slug>` app pages; legacy 301s
   (`legacy.ts`) unchanged. The separate text-only water pages (html.ts page body) are retired.
4. Budgets: keep T196's per-page budget on the prerendered HTML (pane markup + critical CSS; app JS deferred).
5. Staging: site.sh builds this; staging stays noindex.

## Acceptance
`/water/<slug>` on staging shows the pane identical to the in-app tap (screenshot pair, phone + desktop); view-source holds
the rules text; legacy redirect test passes; hubs + sitemap link only app pages; no text-only water page is emitted.

Depends: T192–T198 (built). Size: L. Owner: no.

## Outcome (2026-10-29)
- **The pane is the app's own render.** `app/tools/seo/pane.tsx` server-renders the real `Shell` (react-native-web
  `AppRegistry` + react-dom/server) on the water's Regulations view: the same route a tap / `?regs=<item>` makes. The hooks are
  answered before rendering through one seam in `@app/ui` `useAsync` (`setPrerenderAnswers`: render, ask what it asked, render
  again; null in the app). No second template; the text-only body (`html.ts` `waterBody`) is deleted.
- **One markup per page:** light + dark renders merge into CSS variables (`prerender.ts` `mergeThemes`, the app picks its theme
  by `prefers-color-scheme` too); phone / tablet / desktop renders merge into one tree (`layouts.ts`: a node only one layout draws
  is hidden on the others, a wrapper one layout adds is `display: contents` on the others, per-layout styles as media classes).
  The page = the web export's own index.html (`build-seo.mjs --app <export>`), pane in `#prerender` over `#root`, the map's
  CSS non-blocking, a module hand-over that drops the pane when the app's own regs pane is up (keeps the scroll).
- **App side:** `/water/<slug>` is a start link (`startLink.web.ts`, item from `<meta name="cift:item">`); the address bar
  writes `/water/<slug>` for a named water (`systemBack.web.ts` + `waterSlugs.ts` over `water-slugs/<0..63>.json`, FNV shard
  `@app/ui` `slugShard`), `/` + `?regs=` otherwise (unnamed stays unindexed). **Owner add-on:** a page that started on a
  water (or `?show=<item>`) returns to the map framed on that water, nothing lit — as closing a tapped pane leaves it (owner
  correction 2026-10-29; `useShellSearch` `landOn`, `arriveAt` prop; 9573da40c); before
  the script runs the pane's ‹ and × are plain links to `/?show=<item>`.
- Two app components changed without changing their look: `YearStrip` takes a width hint when unmeasured (`StripWidthHint`, 0 in
  the app; viewBox + percent marker), PinBar / RegsHeader give `useSyncExternalStore` a server snapshot. Faces' back has
  `testID="water-back"`.
- Kept: slug map (unchanged), legacy 301s (19,146 water / 163 region / 22 home), hubs + topic hubs + sitemap linking
  `/water/<slug>` (topic links lost their `#anchor`). JSON-LD of a water page = WebPage + Place (name) only — the pane shows no
  breadcrumb/date/FAQ, and the gate holds JSON-LD to visible text.
- **Budget (T196, on the prerendered HTML), r-62e2f3a8c81e:** 19,936 pages (19,643 waters), largest 10,763 B gzip (limit 30 KB),
  p95 9,615 B (16 KB), shared pane CSS 4,598 B gzip (10 KB), inlined critical CSS 284 B (2 KB), no blocking script. Build 26.5
  min (was ~4), dist 1.5 GB raw. Water pages stream to `<out>.partial`, renamed only when every gate passes.
- **Staging** (version 74d9bd2f, release r-62e2f3a8c81e): pairs in `T269-shots/` (`<water>-<phone|desktop>-page.png` =
  `/water/<slug>` with JS off vs `-tapped.png` = the app): 0.12–0.53 % pixels differ, all in the year bars' hatch phase (a desktop
  browser's 15 px scrollbar vs the phone hint). Hand-over 1.1–1.2 s; ‹ from a direct landing → `/`, map at the water, lit
  (`vedder-river-phone-back-to-map.png`, re-shot after the correction: staging 5de003db); a tap writes `/water/elk-creek-r2-near-rosedale`.
- Known differences until the app takes over: live feeds (DFO salmon feed, in-season banners) are the release's at build time
  — e.g. Kootenay Lake's Salmon row says "check DFO" on the page, "closed" once the app reads today's DFO feed; first-time
  visitors then get the app's own "Before you fish" sheet.
- Tests: `prerender.test.ts` (merge rules + mutations), `site.test.ts` rewritten for app pages (pane present, no text body,
  ‹/× links, verdict = app's, JSON-LD, budget), `startLink.test.ts` (`show`, shards).
