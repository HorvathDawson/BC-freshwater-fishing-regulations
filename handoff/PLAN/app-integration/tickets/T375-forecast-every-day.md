# T375 — The forecast feed publishes every day, +1 to +10

Status: DONE in code 2026-11-03 (feed size + worker redeploy left) — data chat (owner 2026-11-03: "add this to the list for us to do here"); with the UI chat's forecast day
chips (T367: "Now · Tue 4 · Wed 5 · … · Fri 14 · Change"), before 0.5.0.

Today the feed ranks only +1, +3, +5, +10 (`pipeline/gauges/feed/publish.py` `HORIZONS`, the worker's copy
`app/workers/feeds/src/gauges/publish.ts` `HORIZONS`; emitted to the app as `FORECAST_DAYS` by
`pipeline.tools.emit.gauge_policy` → core `gauge-policy.generated.ts`). The chips need every day.

Work:
1. `HORIZONS = tuple(range(1, 11))` in both (worker parity test), regenerate the gauge policy; each day ranked as today
   (`change.combine_day`, absent where no fresh run reaches it — never the previous day's value).
2. Feed size: 10 horizons instead of 4 per gauge — measure the gauge feed before/after (gz) and say it; check the
   app's readers (`ui/src/panel/horizon.ts`, `data/src/panel.ts`, `conditions.ts`) take any day, not the four.
3. Tests: test_forecast_percentiles / test_forecast_change / test_gauge_policy, the worker's; the golden if any.
4. Tell the UI chat (T367) when the feed carries all ten (staging worker redeploy is the data chat's).
Size: S–M.

## Done 2026-11-03
`HORIZONS = tuple(range(1, 11))` (publish.py) and the worker's copy; `FORECAST_DAYS` regenerated. Pipeline tests updated
(percentiles, change — the spike day 10-26 is now +4 and ranked, under the Change's peak —, combine, gauge_policy): 105
pass; worker 184 pass. The UI chat's horizon tests are theirs (T367 e). Left: measure the gauge feed's size on the next
feeds run, and the staging worker redeploy (with 0.5.0).
