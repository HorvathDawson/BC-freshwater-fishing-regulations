# T1039 — The public gauge page draws the web app's own chart (same data, windows, zoom, every forecast model)

Status: OPEN — stopped before building (lane L25, 2026-11-23): the only route to the app's own chart on the page is a
big new page bundle; options with measured sizes below, owner call Q183.

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

Depends: Q183 (owner pick). Size: M (B or C) to L (A, D). Owner: yes (which option; whether a ~260 KB gz page bundle is
acceptable). Default: none built — the lane stopped as briefed; recommended B, plus the ELF facts fix.
