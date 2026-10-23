# T51 — "Like your visit?": a spot's conditions then vs the next 5 days, as a similarity score

Status: DONE — fa7235c0 + review fixes a6fd848d (ticket a7b94f4e). Lane wf_c3f10dea-7b4-2. Follow-ups below are small and none blocks it.

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

## As built (fa7235c0)
- **Days: today and the next four** (Today, Tomorrow, +2, +3, +4), beside the visit's own day. Today's flow is the
  gauge's reading now (horizon 0); +1 and +3 read the feed's `ahead` block; **+2 and +4 have no horizon**, so they are
  scored on the weather only and say "No flow forecast for this day: scored on the weather only." (no interpolation).
- **Formula** (`app/packages/core/src/likeVisit.ts`, all named constants): likeness = max(0, 1 − |then − that day| /
  SPAN); score = Σ weight·likeness / Σ weight over factors known on both days. Weights flow 0.45 · air temperature 0.2 ·
  rain over the 3 days before 0.2 · wind 0.1 · cloud 0.05. Spans: flow 0.5 (50 percentile points), 12 °C, 15 mm,
  25 km/h, 100 %. Reads: ≥ 85 very similar, ≥ 65 similar, else different. Weather is compared at the visit's own hour.
- **Flow = discharge percentile only, station against station**: the visit's from the spot's own gauge's row in the
  frozen donor panel (asked in discharge), else its reading when that reading holds a discharge and no level; the
  coming days' from the same station in the feed index via `reading()` (no new feed fetch; the index is the cached
  object every surface reads). No flow then → "No flow then to compare: scored on the weather only."
- **Offline**: the last fetched forecast is kept per spot (`openLikeStore`: localStorage key / `spots/like-visit.json`),
  read at once, refetched when the spot opens; the age line says "Forecast checked 2h ago." or "Offline: showing the
  forecast checked 5h ago."
- Weather: one Open-Meteo forecast request (`openMeteo().ahead`, past_days=3, forecast_days=5, hourly air/wind/cloud,
  daily rain).
- Tests: core 17, data 10, ui 4 (render), ui-native 5 (light + dark); mutation pins: swapping the flow/temp weights
  (6 fail), reading level instead of discharge (2 fail), moving the 65 threshold (1 fails). Gates: tsc, boundaries,
  platform, colours, style clean. Web shots of a spot saved through the app on Chilliwack River (real feed + weather):
  `handoff/PLAN/app-integration/T51-shots/web-light.png`, `web-dark.png`.
- No rebuild needed (app only; no pipeline, bundle or tiles change).

## Left / follow-ups (not done here)
- **+2 / +4 flow**: when T52's per-station DAILY forecast series lands, those days can read it instead of being
  weather-only (one function: `flowAhead` in `app/packages/data/src/spots/likeVisit.ts`). Default: weather-only, said.
- **Flow trend over the lead-up** is not a factor (the horizons are too sparse to give a coming-day trend without
  guessing). Default: left out.
- **Shares shown can sum to 99 %/101 %** after rounding when factors drop out (e.g. 36+36+18+9). Cosmetic.
- **Cache is never pruned** of deleted spots (a few hundred bytes each). Default: leave.
- **Observed while testing (existing S1 behaviour, not T51)**: a spot backdated to two days ago froze TODAY's gauge
  reading and panel (capture reads the live feed whatever the visit time), so its "flow then" is today's. Worth its own
  ticket: a backdated visit should take the lead-up's daily mean for the visit day, or record no flow.

## Adversarial review (a6fd848d)
- **Fixed — "refreshes when online"** was only "refetches when the spot is opened": a spot opened offline stayed on
  the cached forecast while open. `useLikeVisit` now takes the platform `Connectivity` (Shell passes it) and asks again
  the moment the device says it is back. Test: opened offline → online event → "Forecast checked just now." (pinned:
  dropping the retry dependency fails it).
- **Fixed — "now" was said of a forecast**: today's flow was worded "now" whatever it was, so a cache (or a feed)
  from yesterday put yesterday's +1 forecast in today's column as "the gauge's reading now". `LikeFetched.flowNow`
  keeps the index's own day; only that day's flow is "now". Test pinned (reverting to "first column = now" fails it).
- Checked, no defect: rain windows match (lead-up is visit−7..visit−1, so both sides are the 3 days BEFORE); flow is
  discharge-only on both sides (`answerFrom(…, "discharge")` panel row vs `reading(row, "discharge", h)`); the live
  index carries `fetchedAt` and the Chilliwack 08MH001 row has `ahead` 1/3/5/10 only, so +2/+4 are weather-only as
  claimed. The visit weather is fetched in UTC hours (S1's `at()`, no `timezone`) and the coming days in place-local
  hours; both are the same instant (the visit's hour), so the comparison holds.
- Note on the web shot: that spot is backdated (visit 20 Oct, saved 22 Oct), so its "flow then" is the S1 bug's
  today's reading (5th vs 5th) — the shot shows the strip working, not a real flow comparison.
- Small, left: with a weather source that has no `ahead` (the no-service default) the age line speaks for the flow
  half only.

## Owner questions (each built with its default)
- Q-a: Today + 4, or +1..+5 (which would put +5's horizon in and drop today's observed flow)? Default: today + 4.
- Q-b: Are the weights right (flow 45 %, air 20 %, rain before 20 %, wind 10 %, cloud 5 %)? Default: as built.
