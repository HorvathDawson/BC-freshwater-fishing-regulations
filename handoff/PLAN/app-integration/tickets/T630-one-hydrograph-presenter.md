# T630 — One hydrograph presenter for the app and the public gauge page

Status: OPEN — read 2026-11-11, not built: one presenter changes what the public gauge page (or the app) draws; needs a pick first, listed at the end

Source: T564 report 2026-11-11. The public gauge page still draws its own SVG; the app's presenter lives in `ui`,
which the site build cannot import. T564's parity test compares shared `core` words, not the live app panel.

## Work
1. Move the hydrograph presenter (scales, ticks, marks, range band, words) into `core`; `ui` and `tools/seo` both draw from it.
2. The parity test compares the page's chart model with the app panel's for the same gauge and day.

## Acceptance
- One presenter; a change to it changes both. The parity test fails when either side adds its own mark or word.

Depends: T564. Size: M.

## Read 2026-11-11: why it was not built as a refactor
The brief was "one presenter, same output on both surfaces, proven by the existing tests". The two charts are not one
picture drawn twice, so no shared presenter can leave both as they are. What differs, in code
(`app/packages/ui/src/hydrograph.ts` `buildHydrograph` vs `app/tools/seo/gaugeClient.ts` `chartSvg`):

| | the app panel | the public gauge page |
|---|---|---|
| window | the series it is handed (72 h, or the year) plus the forecast; zoomable | fixed: 7 days back from now, plus the forecast |
| y scale | includes the usual-range band; linear pads 18 % below, 7 % above; a year is log | the readings and forecast only (the band is clipped to the plot); 10 % both ways; always linear |
| y tick words | its own `fmt`: 1,500 reads "1.5k" | core `figure`: 1,500 reads "1500" |
| x ticks | the panel's span labels / a zoomed window's days | a tick every B.C. day, weekday names, thinned by width |
| usual range | five percentiles per sample, three nested bands and a median line | daily rows by date, two bands, no median |
| also drawn | earlier years, the forecast's own min–max ribbon, other models, the Conditions and regulations day marks, a probe | an area under the line, the dot, Today |
| shared today | `niceTicks` (linear ticks), `layoutDayMarks` (the Today label), `normalRangeOf`, `figure` (page), the day words | the same four, from core |

So unifying means one of these changes on screen: the page's scale and tick words become the app's (or the app's
become the page's), and the band joins or leaves the scale. The tick labels are strings a reader sees, so this stops
here per the brief ("if strings would change, stop and report").

A move of `hydrograph.ts` into `core` alone is possible with no output change (it imports only `@app/core`, no React),
but it was not done: it does not make the page draw from it, and the page ships as one plain script whose core
functions are written in by name (`gauges.ts` `clientScript`, `CORE_INLINE`), so `buildHydrograph` and everything it
calls would have to be inlined there too. Better done with the pick below, in one change.

## Picks needed (owner or lead), then it is size M
1. Which picture is the one: the app's rules on the page (the usual range inside the scale, the app's padding, "1.5k"),
   or the page's rules in a "7 days + forecast" mode of the app's presenter? Recommended: the presenter gets a
   `week` mode with the PAGE's window and day ticks, and the APP's scale rule (band inside the scale: a reading far
   under the usual range is the story) and one tick wording for both.
2. One tick wording: `figure` ("1500") or the chart's `fmt` ("1.5k").
3. Does the page gain the median line and the third band, or stay at two bands?
After the pick: move the presenter to `core`, add the mode, have `chartSvg` draw the presenter's paths, and replace
`gaugePage.parity.test.ts`'s chart case with a model comparison (same gauge, same day: ticks, marks, band rows, line).

## Owner 2026-11-12
Leave both charts as they are (accepted as built).
