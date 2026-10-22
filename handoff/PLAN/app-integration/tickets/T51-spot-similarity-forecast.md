# T51 — "Like your visit?": a spot's conditions then vs the next 5 days, as a similarity score
Phase 4 · Depends on S1 (spot lead-up: weather + gauge frozen at capture), the gauge feed + forecast horizons, Open-Meteo
weather, the T48 Spots tab · Size 1–2 days

## Owner (2026-10-22)
"when you view a spot information it should be able to compare how it was at that time to the next 5 days. basically give a
percentage score of similarity to when the spot was gone to. this could just be a little forecast looking widget."

## Goal
On a spot's screen, a small forecast-style strip of the next 5 days (today + 4 or the next 5 — pick and say), each day with
a "how like your visit" percentage and a one-word read (very similar / similar / different), plus the visit's own day for
reference. Tapping a day shows what drives the score (flow vs then, water level, temperature, weather) in plain words.

## The score (decide, document, test)
- Inputs frozen at the visit (S1 lead-up): the gauge reading(s) for that water (flow percentile for the date and/or the
  value, the trend over the lead-up week), air temperature, precipitation over the lead-up, cloud/wind if stored.
- The same quantities for each coming day: the gauge forecast where it exists (the Conditions horizons +1d/+3d/+5d; when a
  horizon has no estimate, the score says so rather than guessing) and the Open-Meteo forecast for the spot.
- A transparent, documented formula (e.g. weighted per-factor similarity, flow weighted most because the owner cares about
  flow), never mixing flow and level (decisions.md "never mix quantities"); named constants for the weights; each factor's
  contribution shown on tap. No score when the visit had no gauge data — say "No flow then to compare" and score weather only
  with that stated.
- Owner rule: computed where the data lives — the app computes from its own frozen lead-up and the live feed/forecast (no
  regulation logic involved); the formula lives in core with unit tests.

## Acceptance
- Whole-string tests for the score on fixed inputs (identical day = 100 %, each factor moving alone lowers it as documented,
  missing data handled and worded), a render test of the strip in light/dark, a web shot of a real saved spot.
- Works offline with the last fetched forecast (says how old it is) and refreshes when online.
