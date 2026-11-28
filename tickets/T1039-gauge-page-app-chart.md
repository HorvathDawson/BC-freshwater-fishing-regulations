# T1039 — The public gauge page draws the web app's own chart (same data, windows, zoom, every forecast model)

Status: DONE — 9f8603f17 (ELF facts), 44af0d16f + 1b983b5ad (the app's chart on the page; the hook file came in the second), f181f241f (Get the app = the Spots tab's), 085ce5e6f + 145cf8542 (reviews of #73), 6a4ebf3ab (almanac); his look moved to T1194's shared gauge page (#354, ## Owner).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1039 | The public gauge page draws the web app's own chart (same data, windows, zoom, every forecast model) | DONE — his look moved to T1194's shared gauge page (#354) | 9f8603f17, 44af0d16f, 1b983b5ad, f181f241f, 085ce5e6f, 6a4ebf3ab, 145cf8542 | — |

Source: owner 2026-11-23, verbatim: "the gauge website in 9.0.0 should also have the same chart as the website. right
now it is only a lower resolution 7 day chart with no forecast?"

What was wrong (staging, 2026-11-23): the page (`app/tools/seo/gauges.ts` + `gaugeClient.ts`) drew its own 7-day,
Flow-only chart from the share-card facts (`workers/feeds/src/gauges/card/card.ts`), and `card.ts` dropped a run judged
`stale` by the working-day line, so ELF (a weekly run) went from 340 pages and 46 pages drew no forecast at all (e.g.
08NM240 Two Forty Creek). Before-shots in `docs/app-integration/T1039-shots/before-*.png`.

## Owner answer (Q183, 2026-11-23, verbatim)

- Chart: "App's chart, ~260 KB (Recommended)" — option B.
- ELF: "Yes, fix in 0.9.0 (Recommended)" — the facts file keeps every run the app offers, by the app's own rule.

## Owner follow-ups (2026-11-23, verbatim)

- "chart should use same buttons as spots too for now" (with a shot of the web app's Spots tab "Get the app" block:
  both "Join the Android test" and "Join the iPhone test"). Built: ui-native `GetAppBlock`, drawn by the Spots notice
  and mounted on the gauge page (compared whole in `gaugePage.parity.test.ts`); the Android join uses core
  `STORE_CONTACT`. Shots `T1039-shots/after-get-app-desktop.png`, `after-get-app-phone-390.png`.
- "could add other almanac info too?" (below the weather line). Built: sunrise / sunset (core `sunLine`) and cloud cover,
  pressure, humidity (core `SPOT_WINDOW` / `weatherFigure`), worked out when the page is read (T605); left out moon
  phase (the app has none), `daylightRange` (a water's rule), wind direction (shown nowhere in the app). Shots
  `T1039-shots/after-almanac-desktop.png`, `after-almanac-phone-390.png`.

## Built (lane L25, 2026-11-23; PR #73)
- 9f8603f17 ELF facts: `card.ts` `cardModel` lists every run the app offers by ONE core rule (`runOffered`, also behind
  data `forecastFor`): old runs too (marked by `dueBy`), whole, level runs on a level card, any model. `CARD_REV` 4 redraws
  every card once. Needs a STAGING feeds worker deploy.
- 44af0d16f + 1b983b5ad the app's chart: `ui` `useStationChart` + `ui-native` `StationChart` (one chart, two callers),
  mounted by `tools/seo/gaugeChart.tsx` on `data` `stationChartSource` (the live station file + the station's `gauge_clim`
  rows written into the page config, ~1 KB). esbuild bundle **946 KB raw, 292 KB gzip** (estimate was 260); page HTML 4.0
  KB gzip (was 3.2); `budget.ts` `chartJsGzipBytes` 320 KB (measured 298 KB).
- f181f241f Get the app = the Spots tab's; 6a4ebf3ab almanac.
- 085ce5e6f + 145cf8542 review fixes: level-only gauge with no climatology (Capilano 08GA031) no longer charts
  "discharge" (`ui` `readingQuantityOf`); `GestureHandlerRootView` (`GaugeChartRoot`); W46: a missing `#gauge-chart` /
  `#gauge-get-app` is shown AND reported, both React roots use `onUncaughtError` (`renderInto`), proved in
  `gaugeChart.failure.test.ts`. Tests: `gaugePage.parity.test.ts` (renders the real `ConditionsPanel` over the release
  source), `cards.test.ts` +4, `change.test.ts`; mutations killed.
- Kept on purpose: the native coho sheets (1.5 + 1.9 MB) stay in the upload (`coho.ts` imports them; the web never
  fetches them). Deploy order: none needed (the 0.8.x page reads the new facts as chips).
- **Not verified:** Lighthouse (TBT, Performance) on the throttled mobile profile (lhci is the release job's); the page on
  staging itself (pictures are a local build of the live release over staging's data); iOS Safari.

## Acceptance
The page's chart and the web app's chart for one station show the same series, windows, models (with toggle) and normal
band; parity test mutation-proved; shots.

Depends: —. Size: M. Owner: answered (Q183: B + the ELF fix).

## Owner

- [x] 2026-11-23 — Look at the gauge page's app chart (`docs/app-integration/T1039-shots/after-page-chilliwack-*.png`, `after-page-two-forty-elf-*.png`, `after-almanac-*`, `after-get-app-*`) and say if it is right. → superseded by T1194 (#354): "one shared piece used by both website and app is a yes. ..."; Q16 "way better ..."; the foot: "Variant B" (2026-11-26). The look is T1194's.
- [x] 2026-11-23 — Gauge page chart (Q183) → "App's chart, ~260 KB (Recommended)"; ELF → "Yes, fix in 0.9.0 (Recommended)" (2026-11-23). Also: "chart should use same buttons as spots too for now", "could add other almanac info too?" (built).
