# T126 — Forecast reading bugs found by T52: CLEVER times 8 h off, a 5-day summary on a 10-day run, dots ignore horizon

Status: OPEN — found 2026-10-22 while building T52 (correctness, band 2).

1. **CLEVER timestamps are Pacific Standard Time, read as UTC.** Best fit against 305 gauges is 8 h off UTC. The
   +1/+3/+5/+10 chips (`publish.py` `forecast_at`, nearest step within half a step) therefore read the forecast 8 h
   off. Fix the parse at the source (pipeline/gauges/feed/forecast.py `series()`), pin with a recorded CSV fixture and
   the measured fit; republish the feed.
2. **CLEVER's summary reads `Forecast_maximum_in_5_days` while its horizon is 10 days** (`forecast.py:66`): the summary
   `value`/headline can miss a later peak (08MH090: summary max 0.263 m³/s, series peak 5.43 m³/s on day 6). Take the
   summary from the series (one source — T52's change measure already reads every day).
3. **Gauge dots ignore the chosen horizon** (they always show now). Decide: dots follow the horizon like the rivers, or
   stay "now" and say so in the legend. Default: follow the horizon.

Acceptance: recorded-fixture tests for 1 and 2; whole-value test for 3; feed republish (no deliver).
