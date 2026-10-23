# T52 — "Expected change": a Conditions view that flags rivers about to change, spikes included

Status: IN PROGRESS — BUILT in lane wf_e8f2adba-933-2 (940b3242 pipeline + the app commit after it), awaiting review
and merge; live only after a feed republish (publish.py writes `change`). Ticket 6ffda443.

Phase 4 · Depends on the gauge feed + forecast (pipeline/gauges/feed/forecast.py), Conditions horizons (Now · +1 · +3 · +5
· +10), the colouring engine's standing channel, T47 palette D · Size 2–3 days (pipeline + app)

## Owner (2026-10-22)
"in conditions maybe we should have a way to look at a view that summarizes expected change in flow in the forecast.
basically even if it is a quick spike then drop we want to show this river will have a big change within the CLEVER
forecast (it could also use multiple forecasts). it could be a last option next to +10 where it is something about
expected change. (this will look at the daily forecast, not just the 1, 3, 5, 10, so that we don't miss day-long spikes)."

## Today
forecast.py reads ONE summary per model (CLEVER freshet hourly 10 d, COFFEE fall daily 5 d, ELF low-flow daily 30 d):
forecast min / average / max over the horizon, and the app samples it at +1/+3/+5/+10. A one-day spike between those
days, or a rise-then-fall inside the horizon, is invisible. The per-station series exists (the Centre's per-station CSV)
but v1 dropped it as ~450 requests every half hour.

## Goal
1. PIPELINE: carry each forecast station's DAILY series over its model's horizon (daily max and min from hourly CLEVER;
   daily values from COFFEE/ELF), fetched politely — only for stations whose model is in season, and only when the model
   issues a new run (not every 30 min), with the Province's ATTRIBUTION verbatim. Where two models speak for a station,
   keep both series (the owner: "it could also use multiple forecasts") and record which ran last.
2. THE MEASURE: per station (then per section through the same donor panels/standing pathway), the largest expected change
   over the horizon relative to now — the peak rise and the trough fall, how big (as a percentile move for the date and as
   % of now), WHEN (which day), and whether it comes back down (spike) or holds (step). Named thresholds for "big".
3. APP: a last horizon chip after +10, e.g. "Change" (word it well — "Expected change"), that colours the map by the
   expected change through the ONE colouring engine (a diverging ramp: falling ↔ steady ↔ rising, palette-D-safe, not a
   status colour), and the panel shows the daily forecast line with the peak marked ("Rises to ~3× now on Thu, back down
   by Sat"), each model named with its attribution. Flow only — never mixed with level.
4. Honest gaps: stations with no forecast in season say so; a section whose donors have no forecast shows nothing, not
   "steady".

## Acceptance
- Pipeline tests on recorded CSVs: a one-day spike between +3 and +5 is detected and dated; a step change; a fall; two
  models; out-of-season; request count per run bounded and logged.
- App: whole-string panel words; the chip and legend; colouring through the engine (no paint calls on switch, as the
  other horizons); a web shot during a real freshet/rain window when one exists (or a replayed recorded run).

## As built (lane wf_e8f2adba-933-2)
- Pipeline `pipeline/gauges/feed/change.py`: per fresh run, daily max/min of the forecast trace over the next 10 days,
  peak and trough against the model's OWN today (COFFEE's day 0 runs 1.45x the gauge), ratio + percentile move for the
  date, spike (back within half by a later day) or step. Named thresholds RISE_RATIO 2, FALL_RATIO 0.5, BIG_POINTS 0.25,
  SPIKE_BACK 0.5, CHANGE_DAYS 10, STALE_HOURS 36 (generated into core by `emit_gauge_policy`). Index row `change`
  (beside `ahead`, which stays keyed by day), each run's `change` in the station file, `forecastAttribution` +
  `forecastIssuedAt` in the index. Runs older than 36 h now answer neither Change nor +1..+10. Series requests: only
  moved, fresh runs; capped at 1,200; logged as requests / fetched / carried / stale.
- The station's change rides the donor pathway as a CODED ratio, Φ(log2(ratio)/2): `estimate` then averages log-ratios,
  so a section's expected change is its donors' weighted geometric mean with the usual weights, lead and refusals.
- App: chip "Change" after +10 (Flow only; under Level it answers as Now and is not offered), map coloured through the
  same `standing` mode in its own 200–300 domain (no paint call on switching; a refusal there is nothing, never steady),
  legend "Expected change, next 10 days · Falling · Steady · Rising", panel words in core, a "what the forecasts expect
  here" block per model with its days as a line and the peak marked, and the Province's attribution from the feed.
- Shots: `handoff/PLAN/app-integration/T52-shots/` on the 2026-10-22 feed replayed through the measure (no network).
  The `-2-change-province` files are the app's home view (Chilliwack / Sumas), not the province.

## Today's real feed (fetchedAt 2026-10-22T22:28:51Z, replayed)
374 stations have an expected change (390 carry a run; 16 have only stale or unanchorable runs). On the map's lead run:
269 steady, 94 rise (86 spikes, 10 steps — incl. both), 9 fall, 2 rise-and-fall. Any model: 141 rise (91 spike in some
model), 25 fall, 5 both. Largest: 08HB011 / 08HB025 / 08HF006 / 08HB086 (CLEVER, 39–48x, 24–25 Oct, all spikes).
CLEVER and COFFEE disagree on a big rise at 40 stations. Index grows 183 KB → 228 KB.

## Owner questions (defaults built)
- Q1 Which model colours the map when several speak? Default: CLEVER, then COFFEE, then ELF (your named model, and the
  calibrated one); the panel names every model's own expectation. Alternative: the largest move of any model.
- Q2 COFFEE answers Change but still not +1..+10 (`MODEL_ORDER`). Default: keep it out of the day horizons — its day 0
  is a median 1.45x the gauge, and the horizons rank the raw value. Change anchors on its own today, so it is safe there.
- Q3 The map colours the RATIO (how much the flow moves), not the percentile move: a river already high for the date
  rising 3x would read "steady" by percentile (the envelope stops at p90). The percentile move is said in the words.
- Q4 Ramp hues: light green (falling) to violet (rising), colour-blind green to magenta, dark olive to teal — the dark statuses hold
  mint, rose and lavender. Dark sits 17 (not 20) from the own-gauge yellow (recorded in tools/change-colours.test.ts).

## Found, not done (need tickets)
- CLEVER's stamps are PST wall clock (measured: best fit +8 h against 305 gauges' observations), but `publish._parse`
  reads naive stamps as UTC, so +1/+3/+5/+10 sample a step 8 h off. Change already reads them as PST.
- CLEVER's summary `fmax` column is `Forecast_maximum_in_5_days` on a 10-day model (08MH090: 0.263 vs a 5.43 peak on day
  6). Change does not use it; the chart headline still does.
- Gauge dots ignore the horizon (as for +1..+10): under Change they still show today's percentile.

## Review (2026-10-22, adversarial)
Held: a one-day spike between +3 and +5 is caught and dated (08HA003, +5 reads the day after); rise-then-fall is a
spike, a held rise a step; two models kept, each against its own today; out of season and stale runs say nothing;
series requests only for moved fresh runs, capped and logged; no paint call on the chip switch (same `modes`); Level
never offered Change; a refusal is nothing, never steady.
Fixed (test-first):
- 694f7c71 light rise1/rise2 sat dE2000 13.7 / 6.5 from `color.highlight` (the ring round a tapped river vanished on a
  rising one — visible in desktop-light-4-panel.png). Gate now holds every stop 20 off the highlight; light rise is
  violet #D080C8 / #A850F8. Shots in T52-shots/ predate this and show the old magenta.
- 985feee9 an extreme on the LAST day read was a "step" worded "and stays up" (30 of 57 big unreturned moves on the
  2026-10-22 feed). Now shape `open` and "still rising/falling on the last day shown".
- f35008e5 the Change block's words moved into core; model order from `CHANGE_POLICY.order`.
Left: light rise2 is dE2000 16 from the you-are-here pin's violet (`color.ui.accent`, a pin, not a line); under
colour-blind simulation several stops sit under 10 from a status (statuses are not drawn on Conditions; the gate is
normal-vision, as for the warm dot); a section's figure is the geometric mean of each donor's own LARGEST move, so a
donor rising and one falling can average to "little change" (the explainer says gauges may peak on different days,
not that they may disagree in direction).
