# T52 — "Expected change": a Conditions view that flags rivers about to change, spikes included
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
