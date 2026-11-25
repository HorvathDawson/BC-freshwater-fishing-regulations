# T1039 — The public gauge page draws the web app's own chart (same data, windows, zoom, every forecast model)

Status: IN REVIEW — 9f8603f17 (ELF facts), 44af0d16f + 1b983b5ad (the app's chart on the page; the hook file came in the second), f181f241f (Get the app = the Spots tab's), 085ce5e6f + 145cf8542 (reviews of #73), 6a4ebf3ab (almanac); owner: OWNER-TODO "Look (you): T1039 gauge page after-shots". Shows on staging after the STAGING feeds worker deploy (ELF facts) and the 0.9.0 staging site build (the chart).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1039 | The public gauge page draws the web app's own chart (same data, windows, zoom, every forecast model) | IN REVIEW — owner: OWNER-TODO "Look (you): T1039 gauge page after-shots"; staging feeds worker deploy + 0.9.0 staging site build to show | 9f8603f17, 44af0d16f, 1b983b5ad, f181f241f, 085ce5e6f, 6a4ebf3ab, 145cf8542 | owner 2026-11-23; lane L25 |

Source: owner 2026-11-23, verbatim: "the gauge website in 9.0.0 should also have the same chart as the website. right
now it is only a lower resolution 7 day chart with no forecast?"

## What staging shows (GET only, 2026-11-23 ~13:00 PT)

The page (`app/tools/seo/gauges.ts` + `gaugeClient.ts`, T487/T564/T908) draws its own chart from the share-card facts
`<data>/feeds/cards/<station>.json` (gauge-card/1, the feeds worker's `cardFacts`, `workers/feeds/src/gauges/card/card.ts`).
The web app's Conditions chart is `Hydrograph` (`packages/ui-native`) over `useChartView` (`packages/ui/src/chart.ts`) over
`WaterSource.gaugeSeries` = the live station file `<data>/feeds/live/<station>.json` + the release's climatology
(`gauge_clim` in the `climate` pack).

**The missing forecast is in the facts file, not the page.** Of the 452 gauge pages, 361 gauges have a forecast in the
app's live feed. Their facts files carry: every model the app has on 8; ELF dropped on 340; CLEVER and ELF dropped on 11;
COFFEE + ELF on 1; CLEVER + COFFEE + ELF on 1. **46 pages draw no forecast at all** while the app draws one (e.g.
08NM240 Two Forty Creek: "No current flow forecast: the latest ELF run (issued 2026-11-19 at 12:22 PT) is likely out of
date"). Cause: `card.ts` drops a run judged `stale` by the working-day line (`byWeekdays`) — ELF is a weekly run, so it is
always "old" there — and keeps discharge only, points cut at now + 10 days. The page itself draws every run the facts hold
(Chilliwack 08MH001: CLEVER + COFFEE drawn; the app also offers ELF · 30d).

What the page lacks against the app, besides the runs: only the last 7 days (no 72 h / Year window, no daily year line, no
prior years), Flow only (no Level), no zoom / pan / read-out, no forecast uncertainty ribbon, no median line; the normal
band is only the facts' 18 rows over the 7-day window.

Pictures (`handoff/PLAN/app-integration/T1039-shots/`): `before-page-chilliwack-desktop.png`,
`before-page-chilliwack-phone-390.png`, `before-page-two-forty-no-forecast-desktop.png`, and the web app's chart for the same
gauge `webapp-chilliwack-chart-desktop.png`.

## Owner answer (Q183, 2026-11-23, verbatim)

- Chart: "App's chart, ~260 KB (Recommended)" — option B.
- ELF: "Yes, fix in 0.9.0 (Recommended)" — the facts file keeps every run the app offers, by the app's own rule.

## Owner follow-ups (2026-11-23, verbatim)

- "chart should use same buttons as spots too for now" (with a shot of the web app's Spots tab "Get the app" block: both
  "Join the Android test" and "Join the iPhone test"). "Spots" is the WEB APP's Spots tab notice (ui-native
  `WebSpotsNotice`), not a tools/seo page. Built: its "Get the app" part is now ui-native `GetAppBlock`, drawn by the Spots
  notice and mounted on the gauge page by the chart's script (the same component, compared whole in
  `gaugePage.parity.test.ts`); the page's own static copy (`openInAppControl`, `joinClient.ts`, the join script and its CSS)
  is gone. The Android join needs the app's contact: the page uses core `STORE_CONTACT` (support@canifishthis.ca, the
  value every app build, the web export included, is made with; a test holds the export scripts to it). Shots:
  `T1039-shots/after-get-app-desktop.png`, `after-get-app-phone-390.png`.
- "could add other almanac info too?" (below the weather line). What the app shows for a gauge or water, and what the page
  now carries (shots `T1039-shots/after-almanac-desktop.png`, `after-almanac-phone-390.png`):
  - ADDED sunrise and sunset at the gauge for the reader's B.C. day: the water page's own sun line (core `sunLine`, written
    into the page script from its source like every core word), in its spoken words ("Sunrise 8:30, sunset 17:18 PT").
  - ADDED the weather measures a spot's weather window shows: cloud cover, pressure, humidity (Open-Meteo, the fields the
    app's spot capture asks for; words core `SPOT_WINDOW`, figures core `weatherFigure`, which gains `humidity` as the window
    rounds it). Temperature, sky, wind, precipitation and the 5-day table were already there.
  - LEFT OUT moon phase: the app has none (a moon would be new arithmetic, not the app's). Daylight-only fishing hours
    (`daylightRange`): a water's rule, not a gauge's. Wind direction: the app records it for a spot but shows it nowhere.
    A spot's lead-up rain line and "like your visit" comparisons: about a visit, not a place.
  - Not static HTML: both are for the reader's day (sun) or the live hour (weather), so they are worked out when the page
    is read, as the reading is; a value fixed at the release build would be wrong the next day (T605). Without JavaScript
    the page says these need it.

## Built (lane L25, 2026-11-23)

- **ELF facts (9f8603f17).** `card.ts` `cardModel` lists every run the app offers in the card's quantity by ONE core rule
  (`runOffered`, also behind data `forecastFor`): old runs too (the page says "likely out of date" by `dueBy`), whole (no
  10-day cut), level runs on a level card, any model the file names. The PNG keeps its first current flow run cut at 10
  days. `CARD_REV` 4 redraws every card once. Needs a STAGING feeds worker deploy.
- **The app's chart (44af0d16f).** `ConditionsPanel`'s chart block became `ui` `useStationChart` + `ui-native`
  `StationChart` (one chart, two callers). The page mounts them (`tools/seo/gaugeChart.tsx`) on `data` `stationChartSource`:
  the app's `httpFeed` reading `<data>/feeds/live/<station>.json` (+ its index) over the station's `gauge_clim` rows,
  which the site build reads from the release (`readGaugeClim`) and writes into the page config (~1 KB base64). The series
  is the release source's own `climSeries`. esbuild (new dev dependency, deps.md) bundles it into one deferred,
  self-hosted script: **946 KB raw, 292 KB gzip** (the estimate was 260); the coho loader's sheets ship beside it under
  `/assets/chart/` (the web one, 145 KB, is fetched only while the chart loads). The page's own 7-day chart, model chips,
  key, day table and their CSS are gone; the Province's credit line stays (`gauge-credit`). The chart's box keeps 550 px
  while it loads (measured 553 desktop / 573 phone) so nothing below jumps. Page HTML 4.0 KB gzip (was 3.2), inside
  `budget.ts`; no budget changed.
- **Tests.** `gaugePage.parity.test.ts`: the page source's `gaugeSeries` / `gaugeParameters` equal the release source's for
  every span x quantity x model; the page's chart markup equals `StationChart` over the release source (useId normalised);
  the page reads the live file, never `/cards/`; a 500 is said ("no record") and reported, an undecodable climatology is
  shown and reported. Mutations killed: no climatology, own drawing, reading the cards, no reporter, one quantity, failure
  not shown. `cards.test.ts` +4 (ELF stays whole, every model, level runs, unknown model), `change.test.ts` `runOffered`;
  mutations killed: stale skip, 10-day cut, quantity ignored, known models only.
- **Not verified:** Lighthouse (TBT, Performance) on the throttled mobile profile (lhci is the release job's); the page on
  staging itself (pictures are a local build of the live release over staging's data); iOS Safari.

## Independent review of #73 (head 44170e16c), fixed

1. MEDIUM: the page told the chart nothing about the station's reading, so a level-only gauge with no climatology (T830,
   Capilano 08GA031) charted "discharge" (1.6 m drawn as m³/s, no toggle). Fixed: the page reads the station's reading
   (`useStationReading` over `stationChartSource.gaugeNow` = the feed's `now`, as the release source) and passes its
   reports and quantity through ONE rule (`ui` `readingQuantityOf`, which the Conditions card's reading now uses too); the
   app's no-record line is shown. The parity test now renders the real `ConditionsPanel` over the release source and
   compares its chart part and no-record line with the page's, for a flow gauge and for the level-only, no-climatology one.
   The review also showed the page lacked the gesture root the app runs under: the chart is now mounted under
   `GestureHandlerRootView` (`GaugeChartRoot`), so pinch / drag / double-tap work.
2. W46: the chart's script marks `#gauge-chart` when it runs; the page's own script checks at `load` and, when the mark is
   missing (404, parse error), says so in the box and reports it. A page without its config shows the failure (no endpoint
   to report to).
3. No JS: the box is hidden (`<noscript><style>`), the existing JavaScript note stays.
4. The no-climatology refusal is a unit (`chartInputsOf`), no second site build.
5. `budget.ts` `chartJsGzipBytes` 320 KB (measured 298 KB with the Get the app block, 292 before); a gauge page is in the
   Lighthouse sample. index.json (380 KB raw, 48 KB on the wire) is not in `budget.ts`: that file is live feed data the
   worker writes, not a site build output; the Lighthouse run on the gauge page counts it on the wire.
6. The native coho sheets (1.5 + 1.9 MB) stay in the upload: `coho.ts` imports them for the frame size, so leaving them
   out means splitting the coho generator's output (tools/build-coho.mjs), not a clean cut here. The web never fetches
   them (the web loader draws `cohoWeb.generated.ts`'s 145 KB sheet); the site sync skips them while their hash holds.
7. CORS: `index.json` and the station files answer `access-control-allow-origin: *` (staging, GET, 2026-11-23).
8. Deploy order: none needed. The production page (0.8.x) reading the new facts gets ELF and level runs as chips, old ones
   marked "likely out of date" (its `dueBy` rule), and draws as before; the 0.9.0 page reads facts only for the reading
   and the credit, so it works with the old worker too.

## Re-review of #73 (6b6db85ec): MERGE after two W46 fixes, done

- A page whose config parsed but lacks `#gauge-chart` or `#gauge-get-app` is shown AND reported (the run mark no longer
  hides it from the page's load check without a report).
- Both React roots (the chart, Get the app) are made with `onUncaughtError` (`renderInto`): a render error writes the
  failure line into its box and is reported, never an empty box. Proved in `gaugeChart.failure.test.ts`: a root without
  the handler, a handler that only shows or only reports, and an unreported missing place each fail it.
- The `feed` pass-through for #77's year scale is L28's (coordinator, 2026-11-23): #73 lands first.

## Options (sizes measured 2026-11-23; today's page = 3.2 KB gz HTML + 9.6 KB gz script + 3.1 KB gz facts)

- **A. The app's chart AND the app's data path, as is.** New page bundle: React + react-dom + react-native-web +
  react-native-svg + gesture-handler + `Hydrograph` + `useChartView` = **260 KB gz JS** (esbuild, minified; 888 KB raw;
  before the release WaterSource code), plus sql.js **322 KB gz wasm**, plus the release `climate` pack **986 KB gz**, plus
  the live station file **49 KB** = **~1.6 MB on a first view**. No worker change. Fails the page's own performance goals
  (Lighthouse 0.95 / TBT 200 ms in `budget.ts` `lighthouseConfig`) almost surely.
- **B. The app's chart, the app's series code, the station's climatology written into the page (recommended if a bundle
  is accepted).** Same 260 KB gz bundle (self-hosted, `defer`, loaded after the static content); data = the app's live
  station file (49 KB, read by the app's feed code) + this station's `gauge_clim` bands, which the SEO build reads from the
  same release and writes into the page config (~1 KB); the series is the app's own `seriesOf(pentadsOf(…))`. Same
  component, same data, every model with its toggle, 72 h / Year, Flow / Level, zoom. No worker change. Cost: 27× today's
  script; LCP unaffected (static HTML first) but TBT on a throttled phone is the risk to measure.
- **C. The app's arithmetic, the page's own SVG.** The page's script gains the app's `seriesOf` + `buildHydrograph` +
  `chartTicks` (the exact shape the app draws: **+37 KB gz**), over the same two reads as B, and draws that shape as SVG
  strings. Same numbers, windows and models; a second renderer of the drawing (dashes, legend, zoom buttons re-wired) —
  pinned by a parity test against `Hydrograph`'s output, but it IS a re-implementation of the drawing.
- **D. Embed the web app.** A gauge-only route in the web app (the existing 1.1 MB gz bundle, cached once for the whole
  site) shown on the page. App change; heaviest first view; the page stops being a light static page.
- **Independently of A–D (small worker change):** keep ELF (and level runs) in the facts file, judged by the app's rule
  rather than the working-day line, so the 46 pages stop saying "no forecast" now. Needs a feeds worker deploy.

## Work (once an option is picked)
1. Build the picked option; `gaugePage.parity.test.ts` extended so the page's chart is the app's (for B: the same
   `Hydrograph` element tree / shape for one station fixture; a page drawn from the card facts only fails it).
2. W46 stays: a failed read is shown in its section and reported (`pageReporter`), offline shown not reported.
3. Self-hosted only (Open-Meteo stays the one outside origin); `budget.ts` gates the HTML; measure TBT on the throttled
   mobile profile.
4. After-shots desktop + 390 px phone, one gauge with a forecast, in `T1039-shots/`.

## Acceptance
The page's chart and the web app's chart for one station show the same series, windows, models (with toggle) and normal
band; parity test mutation-proved; shots.

Depends: —. Size: M. Owner: answered (Q183: B + the ELF fix).
