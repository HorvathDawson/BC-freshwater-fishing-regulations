# conditions-layout: gauge switch prefetch (2026-10-21)

Owner: "when clicking between the gauges in the condition panel it flickers a millisecond before showing the chart on
first load (maybe get all the data when the page opens rather than one gauge at a time when clicked)".

## Cause

- `useAsync` set LOADING on every new key and only learned the answer a microtask later. Even a series the feed
  already had cached drew the spinner for a frame or more.
- The panel asked for a newly picked station's series twice. It asked for "auto" while the station's quantities
  loaded, then for the picked quantity once they arrived. That was two loads with a spinner between them.
- `useChartView` and `useSeries` each called `gaugeSeries` themselves. A comment said they shared one request; they
  did not.

## Fix (commit on this branch, `conditions:` group)

- `@app/ui` `answers.ts`: an ask-once store per source. A second asker shares the first one's in-flight promise, and
  a settled answer can be peeked synchronously. The TTL is 5 min (the feed's own file reuse), at most 96 entries
  per source (least recently used go first), and failures are never kept.
- `useAsync(..., peek)`: an answer already in hand is returned in the same render the key changes, and the question is
  not asked again. When the state is already loading, setting loading again keeps the same state object, so a key that
  changes every render cannot cause a render loop.
- `useChartView`, `useSeries`, `useGaugeParameters` and `useStationReading` all ask through the store, under one
  key each (`seriesKey`, `params:`, `reading:`).
- `usePrefetchGauges` (used by the Conditions panel): for every donor in the panel it fetches the quantities, the
  series for the current span (in the quantity the chart will pick, `chartParameter`) and the reading. Once all of
  those have settled, it fetches each series for the other span. It only covers the panel's donors.
- A gauge that is not yet loaded keeps the last chart up: `ChartView.pending`, drawn at 45% opacity with the line
  "Loading the record" under it. The spinner appears only for the first chart a view draws.
- The panel waits for a station's quantities before it asks for that station's series, so there is no auto-then-picked
  double load.

## Web check (own Expo web on :39338, data from :39217 read-only; "before" = the owner's :39218)

The recorder is `prefetch-frames.py`. It opens Conditions, taps the Vedder River (Chilliwack panel: 08MH001, 08MH016,
08MH056, 08MH047), samples every animation frame with requestAnimationFrame, and clicks through the gauge switch:
016, 056, 047, 001, 016. A "bad" frame is one with no chart drawn or with a spinner up.

| switch to | before (:39218) bad frames | after, 72 h | after, This year |
|---|---|---|---|
| 08MH016 (first visit) | **4** | 0 | 0 |
| 08MH056 (first visit) | **9** | 0 | 0 |
| 08MH047 (quiet, no record) | 88 (says "no recent readings": correct, no chart) | 86 (same) | 86 (same) |
| 08MH001 | 2 (frames before the click landed, coming from 047's no-chart) | 2 (same) | 2 (same) |
| 08MH016 (revisit) | 0 | 0 | 0 |

- "Loading the record" note frames after: 0. Every gauge was already loaded, so the dimmed state never showed.
- Station files: one request per station (`08MH001/016/056/047.json` once each; `08MH024.json` is the map's) and
  **0 requests after the panel opened**, across switches and the span change. Before: 2 requests after open.
- Shots: `prefetch-before-72h-08MH056.png`, `prefetch-after-72h-08MH016.png`, `prefetch-after-year-08MH056.png`.

## Tests

- `ui/src/answers.test.ts`: ask once and share, TTL, failures not kept, per source plus cap, `seriesKey`,
  `chartParameter`.
- `ui/src/async.render.test.tsx`: a peeked answer is drawn in the render the key changes and is not asked again;
  without one the old behaviour holds; a key that changes every render does not loop.
- `ui-native/src/ConditionsPrefetch.test.tsx`: one request per donor per span (this span first, then the other) and
  none on switching; a loaded gauge draws its chart and reading synchronously after the click (no await); a gauge
  that is not loaded keeps the old chart with the note and no spinner.
- Mutation-pinned: removing the peek, removing the stale-chart hold, or removing the prefetch call each fails a test.

## Not done

- The dimmed "keep the last chart" state was never seen in the browser, because everything was loaded before the
  first click. It is covered only by the render test.
- The model chips (`runs`) still disappear while an unloaded gauge loads, because `useSeries` is not held.
- Native (Android/iOS) was not run.
