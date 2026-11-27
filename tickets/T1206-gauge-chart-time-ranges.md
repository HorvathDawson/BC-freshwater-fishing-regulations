# T1206 — Gauge chart time ranges: 72 h / 1 week / 1 year, forecast on the first two, measured data not diminished

Status: OPEN. Proposal only; nothing built. Depends on T1194 (the shared gauge page) so the website and the app change together.

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1206 | Gauge chart time ranges: 72 h / 1 week / 1 year, forecast on the first two, measured data not diminished | OPEN | — | owner picks an option (see ## Owner); then build once in the shared chart |

Source: owner 2026-11-26, verbatim, on the T1194 gauge page: "also i think we might want to change the short term forecast to show slightly more
data? like it could have the more coarse data from 72hr to 1 week? or we have 3 time periods 72 hrs 1 week and 1 year. the first 2 will show forecast.
the week will use coarser data for the rest. think about it. but when forecast shows it feels like the measured data leading up to it is diminished."

## What the chart does today (`StationChart` over `useStationChart`, one component for the water card, the app's gauge page and the website's)

- Two ranges: "72 h" and "Year" (`ChartSpan`). Flow / Level is a separate switch.
- Measured data comes from the gauge's station file in the feed (`feeds/live/<station>.json`): `recent` (30-minute readings, about the last 72 h), `daily`
  (daily means, the year to date) and `priorYears` (the normal range). Forecast: `forecasts` per model (CLEVER, COFFEE, ELF; a daily step, up to 10 days, ELF 30 d on the legend).
- On the 72 h range the forecast is drawn on the same x axis as the readings: 3 days measured then up to 10 days forecast, so the measured line is
  about a quarter of the width (23% on 08MH103). That is why the lead-up feels diminished: it is not drawn less, it is given less room.
- On the Year range no forecast is drawn.

## Options (sketch: tickets/T1206-shots/options-sketch.png, drawn, not rendered)

- **A. His three ranges: 72 h | 1 week | 1 year.** 1 week = 7 days of daily means (coarser, from `daily`) plus the forecast; 72 h keeps 30-minute readings; year has no forecast.
  Measured share rises to about 41% on the week. Cost: a third span in the chart's series code and its switch (`chartSpan`, `climSeries`), the legend and tests. Drift risk: none (one shared chart).
- **B. Tie the forecast horizon to the measured span** (72 h shows 3 days ahead, 1 week shows 7, year none), so measured is always at least half of the width; the full 10-day forecast stays in the Change view. Cost: small (a horizon cut in the chart's x range), no new data. Risk: a reader who wants day 8 to 10 goes to Change.
- **C. Split the plot:** measured on a fixed 60% of the width and the forecast squeezed into the rest, with a visible axis break. Cost: medium (non-linear x axis, hover read-out must say it). Risk: a bent time axis misleads; not recommended.

Pick: **A with B's horizon rule** (his three ranges, forecast on 72 h and 1 week cut to the measured span). It answers "measured data diminished" directly and needs no new data: the week uses the daily means the feed already carries.

Checks for the build: the week's daily-mean step against the 30-minute line must be labelled as coarser (no silent mixing of resolutions); a gauge with fewer than 7 days of record says so; the website chart and the app chart are one component so both change at once.

## Owner

- [ ] 2026-11-26 — Which time-range change: A (three ranges), B (forecast cut to the measured span), C (split plot), or A with B's rule (default we build with)? Sketch: `T1206-shots/options-sketch.png`.
