# T171 — "Like your visit?": every forecast day uses the flow forecast (no weather-only gaps between forecast days)

Status: DONE — 3f43c1dd (feed) + ef762537 (app) + 298cee48 (review fix), lane-T170. Live after a FEED REPUBLISH (owner, below).

Source: owner 2026-10-23: "in the how similar is a spot to when the spot was added section sometimes it has gaps and
says weather only despite gauge forecast on both sides". T51's known gap (its follow-up "+2/+4 flow"): the strip's
coming days read the feed's percentile only at the fixed HORIZONS (+1, +3, +5, +10 — `reading(row, "discharge", h)` in
`flowAhead`, app/packages/data/src/spots/likeVisit.ts), so +2 and +4 are scored on weather alone even though the models
forecast them.

## What the feed already has (measured 2026-10-23, data/generated/gauges/feeds/<station>.json)
`forecasts.<MODEL>.series` (CLEVER hourly for 10 days; COFFEE 5 days; ELF 30 days) and `change.days` (per-day ratios);
only the RANKED percentile is cut to the four horizons (index `ahead`).

## Work
1. Feed (where the data lives): publish a discharge PERCENTILE for every forecast day (daily mean of the series, ranked
   against the same climatology/day-of-year as `ahead`), choosing the model per day by the same one rule T143/T144 use
   (each quantity's first model that forecasts it; ELF only where no flood model reaches, carrying its low-water
   caveat). In the station file (or a compact `aheadDays` in the index if the size allows — measure).
2. App: `flowAhead` reads that per-day series; a day is "weather only" ONLY when no model reaches it (said so); the
   strip's "flow" factor names the model when it is ELF.
3. Tests whole-value (Chilliwack: +2 and +4 now carry flow; a station with only ELF gets the caveat; a day past every
   model stays weather-only); feed test pins one per-day percentile against the horizon value on the same day.
Ships: feed republish (local now; R2 owner) + app. Size: S–M.

## As built (lane-T170)
- **One rule, one function (feed)**: `change.day_model(answers)` — for one forecast day and one quantity, the FIRST
  FLOOD MODEL that answers (`FLOOD_MODELS`: CLEVER, then COFFEE), else ELF with `lowWater`; a model the policy does not
  name never answers. The +1/+3/+5/+10 horizons (`publish.forecast_percentiles`, `MODEL_ORDER` removed) and every ranked
  day (`change.forecast_days`) both ask it; `publish.py` names no model.
- **COFFEE on the gauge's scale**: COFFEE publishes instantaneous peaks (its day 0 sat a median 2.17x the gauge at the
  29 stations it forecasts without CLEVER), so before ranking it is multiplied by `observed / its own today`
  (`change.gauge_scale`, `PEAK_MODELS`); with no reading it cannot answer and the next model does. CLEVER and ELF are
  ranked as published, as the horizons always were.
- **Every day (`change.forecast_days`)**: the next ten Centre (PST) days after today, each the chosen model's DAILY MEAN
  (`day_means`; an hourly day needs 3/4 of its steps, `DAY_COVER`; a daily step is its day), ranked by the station's
  discharge envelope for THAT day (`rank_day`, the same ranking the Change uses). Index row:
  `aheadDays: {from, discharge[10], model[10], lowWater: [day indexes ELF made]}` (lowWater absent when none; a day no
  model reaches is null in both lists). Discharge only.
- **App**: `flowAhead` (`@app/data/spots/likeVisit.ts`, the one read) takes `aheadDays` by its own dates with each
  day's model; a feed without it is read at the four horizons as before. `likeDay` (core) names the model on an ELF day
  ("17th percentile forecast by ELF"), adds `caveat` (`CHANGE_LOW_WATER`); the strip's cell says "low-water model"
  under the score and the caveat closes the day's working. "Weather only" / "No flow forecast for this day" only where
  no model reaches. Review fix (298cee48): in the hour after local midnight the Centre's tomorrow is the reader's
  today; the index day keeps its reading.

## Measured (scratch republish, 2026-10-24T02:08Z, `.../scratchpad/feed`, never into `<repo>/data`)
- **Index**: 266,583 -> 347,427 bytes (+80.8 KB, +30.3 %) raw; gzip 36,676 -> 45,771 (+9.1 KB, +24.8 %). Station files
  unchanged in shape (aheadDays is in the index only). Kept in the index: the strip reads the cached index every surface
  reads (no new fetch, works offline from the like cache). Moving it to the station file would cost one station-file
  fetch per open spot instead (owner question 1).
- **Coverage**: 367 of 433 stations carry `aheadDays`. +1..+4: CLEVER 307, COFFEE 23, ELF 37 (low-water); +5..+9:
  CLEVER 307, ELF 60; +10: ELF 359, none 8 (CLEVER's run ends on day 9 when issued after midnight).
- **Horizons moved** (replay of the 2026-10-23T20:41Z feed through the new rule): only +1 and +3 at 22 COFFEE+ELF
  stations (ELF -> COFFEE, e.g. 08DB013 +1 0.406 -> 0.145); every CLEVER station's `ahead` byte-identical.
- Chilliwack 08MH001: CLEVER for 24 Oct–1 Nov, ELF on 2 Nov (all 5th percentile today). Web shot of the strip with +2
  ("Sun 25 Oct") carrying flow: `../T170-shots/spot-light-after-plus2.png`, `spot-dark-after-plus2.png` (the scratch
  index served by request interception in this lane's own headless Chromium; Expo on :39611, data :39217 GET only).

## Tests and mutation pins
- `pipeline/tests/test_forecast_days.py` (new, 24): the rule whole (10 cases), the no-second-copy pin (only `day_model`
  and `change_source` read the policy; `forecast_percentiles` and `forecast_days` call `day_model`; publish names no
  model), COFFEE scale, Chilliwack / 08DB013 / 08MG013 (ELF only) whole, a per-day percentile equal to the horizon on
  the same day (08DB013 +1/+3/+5/+10), horizons moved only where COFFEE now answers, a day past every model null,
  daily mean, coverage, stale run, publisher index row.
- Mutations (each run, each failed): ELF before the flood models (18 tests); lowWater never set (8); COFFEE ranked raw
  (3); a day ranked against today's envelope (3); a day = its last step (1); no coverage rule (1); stale runs speak
  (1); days start today (7); publisher drops aheadDays (1); horizons skip COFFEE (a second rule) (4).
- App: core `likeVisit.test.ts` (ELF day words + caveat; flood day and today carry none), data `likeVisit.test.ts`
  (aheadDays read whole, Chilliwack +2/+4 carry flow, a day past every model, the midnight case), ui
  `likeVisit.render.test.tsx` (strip with an ELF day: "low-water model" + caveat line). Mutations: flowAhead ignoring
  aheadDays (3 fail), dropping lowWater (2), likeDay never carrying the caveat (2), dropping the midnight guard (1).

## Checks
`pytest` feed/forecast set unfiltered (17 files): 210 passed. `REGS_STORE_REQUIRED=1 pnpm -C app check` via
`scripts/heavy.sh appcheck 30` (read-only `.venv` symlink, removed after): every gate through typecheck green
(gauge-policy `--check` included); vitest 8125 passed / 1 failed — `tools/one-donor-state.test.ts` caught
`r.percentile === null` in the new `dayFlow.ts` (a spot reading, spelled like a donor read); fixed (dropped the clause)
and that file + the touched suites re-run green (171). The regs-store parity files passed this time.

## Adversarial review
- A station with only ELF (08MG013): every day ELF, every day `lowWater` — pinned; app says "low-water model" + caveat.
- A day past every model: null in both lists, the strip's "No flow forecast for this day: scored on the weather only."
- A level-only station: no discharge envelope -> no `aheadDays` (and the old horizons have no discharge) -> weather only.
- Fixed: the Centre-day / local-day edge just past midnight (298cee48, above).
- Left: an ELF day ranks ELF's daily value raw (1.06x the gauge in the median, T144) — same as the horizons always did.

## Rebuild / republish
**Feed republish (owner; the parent republishes the live local feed):**
`.venv/bin/python -m pipeline.gauges.feed.publish --out data/generated/gauges/feeds --clim data/generated/gauges/feeds/clim.json`,
then the R2 upload. No re-fetch, bundle, deliver or release. Until then the app reads the four horizons (old behaviour).

## Owner questions (default built)
1. Index or station file? Default: index (+9 KB gzip, +30 % raw). Alternative: station file, one fetch per opened spot.
2. Horizons now take COFFEE after CLEVER at 22 coast stations (+1/+3 moved from ELF), because one rule decides both.
   This answers T144's Q5 halfway (first model, not CLEVER+COFFEE combined). OK? Alternative: combine CLEVER+COFFEE per
   day as the Change does (geometric mean of ratios × gauge) for both.
3. ELF's +10 at most stations: the strip shows only today..+4, so ELF appears there only at stations no flood model
   reaches (37). Fine as is? Default yes.
