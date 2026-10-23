# T126 — Forecast reading bugs found by T52: CLEVER times 8 h off, a 5-day summary on a 10-day run, dots ignore horizon

Status: DONE — a4d0d040 (pipeline) + 88a097b4 (app) + 01d2f772 (adversarial review), lane-T126; live after a feed republish (below). Found
2026-10-22 while building T52 (correctness, band 2).

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

## As built (lane-T126)
1. **The Centre's clock, read in one place** — `pipeline/gauges/feed/forecast.py` `instant()` (naive stamp = PST wall
   clock, `CENTRE_UTC_OFFSET_H = -8`; a bare day = that Centre day). `series()` → `parse_series()` writes CLEVER/COFFEE
   stamps ZONED (`2026-10-22T05:00:00-08:00`, `zoned()`), so the app's `Date.parse` reads the same instant on any
   device (it read naive stamps on the device's clock). Runs carried from older station files are zoned on the next
   publish (`settle()` in `with_series`), no re-fetch. `publish._parse` (UTC) is gone: `forecast_at` and
   `newest_forecast_issue` ask `instant`; `change.py` lost its own `PST`/`FORECAST_UTC_OFFSET_H` and asks it too.
   Also fixed on the way, same decision: a bare-day ELF step now answers for its WHOLE Centre day in `forecast_at` (it
   took whichever midnight was nearer, so an afternoon publish read +1 as the day after).
   Measured fit, pinned on a recorded fixture of all 304 CLEVER stations in the 2026-10-23T03:50Z feed
   (`pipeline/tests/fixtures/gauge_feed/clever_clock/observed_vs_forecast.json`): median error 0.88% at UTC-8, 0.99% at
   UTC-7, 1.78% as UTC; UTC-8 is the minimum of all 25 whole-hour zones, and `instant` lands on it.
2. **CLEVER's summary from its series** — `MODELS["CLEVER"]["summary_from_series"]`; once the series is attached,
   min/ave/max/value are taken from the steps at or after the issue instant (`ahead_of_issue`, `summary_from_series`).
   Recorded CSV `fixtures/gauge_feed/clever_clock/08MH090.CSV` (run issued 10:35 2026-10-22): value 0.263 → 5.431
   (10-28 10:00), min 0.207, ave 0.46. COFFEE/ELF keep their layer columns (they cover their horizons).
3. **Gauge dots follow the horizon** (the default) — `app/packages/ui/src/gaugePoints.ts` reads each station through
   `@app/data` `reading(row, q, horizon)` (the panels' one reader; the station-default rule it repeated is gone), and
   writes `ramp` = `toRamp(value, horizon)`, which the dot expression (`runtime-style.ts`) now reads instead of
   `percentile × 100`, so under Change a dot sits in the 200–300 change domain beside its river. A day no run reaches
   draws no dot. Under Change the label is the station's own ratio, `changeDotLabel` in core ("3× today"; the coded
   colour value clamps at ~36×). `useGaugeGeoJSON` takes the horizon (cache key too); Shell passes
   `mm.standingHorizon` (Now under temperature). The feed `IndexFile` row type declares `ahead`/`change`.

Tests (whole values; mutation → failing test):
- `pipeline/tests/test_forecast_clock.py` — `CENTRE_UTC_OFFSET_H = 0` → the fit test (1.78 ≠ 0.88) + 13 others; drop
  `zoned` in `parse_series` → the recorded-CSV stamp test; drop `summary_from_series` from CLEVER → the 08MH090 summary
  test; drop the issue-time filter → summary + before-issue tests; drop `settle` in `with_series` → publisher test and
  `test_forecast_series_carry.py::test_a_carried_run_from_an_older_file_has_its_stamps_zoned`; drop the bare-day
  branch in `forecast_at` → the whole-Centre-day test. No second copy: `publish.py`/`change.py` may not contain
  `datetime.fromisoformat`, `timezone(timedelta`, `def _parse(` or `hours=-8`.
- `app/packages/ui/src/gaugePoints.render.test.tsx` "gaugeGeoJSON at a horizon" — `reading(..., 0)` → +1/+3/+5/+10/Change
  fail; `toRamp(pct, 0)` → Change fails (69.15 vs 269.15). `runtime-style.test.ts` pins the dot expression's input;
  `change.test.ts` pins `changeDotLabel`.

Checks: targeted pytest (feed/forecast/gauge files, 194 pass; `test_no_silent_skips` fails on files this lane did not
touch); `REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh: everything through typecheck green, vitest 7218 pass /
5 fail — all five the regs-store pairing (`data/generated/bundle/regs.sqlite` is store 43115d8c…, taps/oracles cut from
a2307766…: re-run `app/design/regs-v11/port/taps.py`), environment, not this change.

**Feed republish (owner; no deliver, no bundle):** the next scheduled publish does it, or by hand
`.venv/bin/python -m pipeline.gauges.feed.publish --out data/generated/gauges/feeds --clim data/generated/gauges/feeds/clim.json`.
It needs no re-fetch of the series: runs already on disk are zoned and re-summarised as they are carried.

## Adversarial review (01d2f772)
Fixed:
- **"Is this step a day?" was decided twice.** `change.py` took a step as a whole Centre day when bare-dated OR its series
  is daily (`step: "1d"`, COFFEE's `…T00:00:00` midnights); `publish.forecast_at` only when bare-dated, so to the
  horizons a COFFEE day was a midnight instant (15:30 at the Centre read tomorrow's value) — the same defect this ticket
  fixed for ELF. One function now, `forecast.step_day(series, at)`, asked by both. No visible change today (horizons ask
  CLEVER then ELF, `MODEL_ORDER`), but the decision no longer has two copies. Tests: `test_forecast_clock.py`
  `test_a_daily_run_stamped_at_midnight_is_a_day_too` (mutation: `step_day` ignores `step` → fails, plus two
  `test_forecast_change.py` tests); the no-second-copy test now also refuses `"1d"` / `is_day(` in `publish.py` /
  `change.py` (mutation: put `series.get("step") == "1d"` back in `change.base_of` → fails).
- **The Shell's hand-over of the horizon to the dots was unpinned**: dropping `mm.standingHorizon` from Shell's
  `useGaugeGeoJSON(...)` passed all 1099 ui-native tests. `ConditionsLayers.shell.test.tsx` "the gauge dots follow the
  map's horizon" now asks it (rivers and dots at 0, +3 after the map's +3, 0 under Temperature); the mutation fails it.

Checked and sound:
- Every other stamp reader: `grep fromisoformat|timezone(|strptime` over `pipeline/gauges` — the rest are ECCC
  observations (`year_backfill`, `publish._local_day`), already zoned; nothing outside `pipeline/gauges/feed` reads
  forecast series. App: `chart.ts` `Date.parse`s series stamps (now zoned, so device-independent); no string slicing of
  forecast stamps; the forecast summary `value` is not worded anywhere in the app (only `horizonDays`).
- Builder's mutations re-run: offset −7 → 6 clock tests fail; bare-day branch → whole-day test; issue-time filter →
  2 summary tests; `settle` zoning → the carry test. Each fails as claimed.
- `reading()` is the only copy of the station-default rule (`parameter ?? "discharge"` appears only in `panel.ts`
  among index readers); `gauge-dot`/`gauge-label` read only `ramp`, `label`, `minz` — nothing still reads the dropped
  `percentile` property. Negative change → `toRamp` NaN → JSON null → `coalesce -1`, the ungauged colour.
- `REGS_STORE_REQUIRED=1 pnpm -C app check` (heavy.sh): tickets/boundaries/platform/style/…/typecheck green; vitest
  7219 pass / 5 fail — the same five regs-store pairing failures (store 43115d8c… vs taps a2307766…), not this lane.
  Feed pytest: 344 pass; `test_own_gauge_continuation` errors on the missing worktree bundle (environment).

## Left / follow-ups
- ELF's bare-day stamps (`"2026-10-22"`) reach the app as-is; `Date.parse` reads a bare ISO date as UTC midnight, so the
  chart places each ELF day at 16:00/17:00 the previous Pacific day. Pre-existing, cosmetic at a 30-day scale; zone them
  (or place them at the Centre's noon) in the app's chart when someone touches it.
- `issuedAt` is still written naive (`forecast._issued`: "Updated at: 10:35 AM …" → `2026-10-22T10:35:00`). The pipeline
  reads it on the Centre's clock (`instant`), but the app's `forecastRunLine` `Date.parse`s it on the device's clock, and
  whether the layer's wording is PST or local (PDT in summer) is unmeasured. Measure against a summer run, then zone it
  in `_issued`.
- A CLEVER run whose series fetch fails keeps the layer's five-day max as `value` (only until a series arrives).
- The 08MH090 layer value (0.263) is not a clean five-day max of the run either (it matches the first ~2 days); the
  layer column's meaning is not reproduced, only replaced.

## Owner questions (defaults built)
- Q1 Gauge dots under a forecast horizon / Change: built "follow the horizon" (the ticket's default), so a day no run
  reaches shows no dots. Alternative: keep them at Now and say so in the legend.
- Q2 Change dot label "3× today" (the station's own ratio). Alternative wording welcome.
