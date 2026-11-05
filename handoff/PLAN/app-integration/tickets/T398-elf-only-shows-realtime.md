# T398 — An ELF-only water opens on real-time data; forecast can always be turned off

Status: DONE in code 2026-11-05 (f2796e37f; owner report 2026-11-05, 0.5.0 build 8, Conditions). Phone check with the next APK.

1. Where ELF is the only forecast (e.g. Capilano), the Conditions pane opens on the real-time data, not the forecast
   (ELF stays the last resort, T144).
2. The forecast can ALWAYS be toggled fully off (a real-time-only choice in the horizons), on every water.

## Done in code 2026-11-05 — phone check with the next APK
Reading taken: ELF is opt-in, and "off" lives in the chart's model control (the legend entries, T367), not in the
estimate's day horizons (those say what the water is doing on a day; turning the chart's forecast off is the chart's).
- Every gauge with a forecast running now shows its model entries PLUS a "Real-time only" entry (core
  `CONDITIONS_WORDS.key.realtime`); a lone model is now a pressable entry too, so off is always one press away.
- Untouched, the chart draws a forecast unless ELF is the only model running (`forecastDrawn`, @app/ui panel/change.ts):
  Capilano opens on the measured line, "Real-time only" checked; a press on ELF draws it with the T300 "Only ELF
  forecasts this gauge" note and its low-water caveat under About this forecast (T144 kept).
- Off, the forecast leaves the chart's time axis too (`useChartView(..., drawForecast)`), and nothing speaks for a run.
- The reader's choice holds across gauge switches in the pane; it resets when the pane reopens.
Tests: change.test.ts (`forecastDrawn`), ConditionsLayout.test.tsx (opens off, on, off again).
Phone: open Capilano's Conditions (measured line only, Real-time only checked), tap ELF, tap Real-time only.
