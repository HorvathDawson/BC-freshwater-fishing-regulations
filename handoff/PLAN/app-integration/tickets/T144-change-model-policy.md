# T144 — Change: use CLEVER and COFFEE (combine when both), ELF only as a last resort with a low-water warning

Status: DONE — a156e1c2 (feed) + 89ee4423 (app) + b4fd96a9 (review fixes), lane-T144; live after a feed republish (below).

Source: Owner 2026-10-23 (app walk). "the change could use coffee and clever but no elf (can combine both if available too for more confident
guessing) clever is better model i think but you can research them to see how you should use them. it can use elf if
nothing else is there. but maybe have a warning for it being a low water forecast". Related: T52 (Expected change),
T126 (CLEVER summary), Q59–Q61 (model order).

## Work
1. Research the BC River Forecast Centre / ECCC products named CLEVER, COFFEE and ELF (what each models, horizon,
   skill, which stations, low-flow vs freshet use); write the findings into this ticket with sources.
2. One function decides the Change source per station and horizon: CLEVER + COFFEE combined when both (state the
   combination rule and why; CLEVER weighted higher if the research supports it), one of them alone, else ELF with a
   visible "low-water model" caveat; never ELF beside CLEVER/COFFEE. Feed side (pipeline/gauges/feed) if the choice must
   be made where the data lives; app words and caveat.
3. Whole-value tests per case, mutation pins, web shot.

Owner questions with defaults. Size: M. Rebuild: feed republish (owner) if the feed changes.

## Research (2026-10-23)

All three are BC River Forecast Centre models (the Province, not ECCC), all written by Dr. Charles Luo. ECCC supplies
the observed flows (Water Survey of Canada) and the weather the two flood models are driven by (CMC NWP).

| | CLEVER | COFFEE | ELF |
|---|---|---|---|
| Full name | Channel Links Evolution Efficient Routing | Coastal Fall Flood Ensemble Estimation | Extrapolating Logarithmic Flow |
| What it is | Semi-distributed hydrologic model: lumped instantaneous-unit-hydrograph sub-catchments + physics-based kinematic-wave channel routing; temperature-index snowmelt | Unit-hydrograph model, no channel routing; built for small coastal storm-driven basins | Empirical: exponential recession (log-flow straight line) fitted to the last 30 days, a "twelve-step, twelve-scenario" scheme for min/ave/max |
| Weather input | Yes: observed climate stations + the CMC NWP 10-day forecast (hourly since 2024) | Yes: observed + forecast rainfall | **None** ("an empirical mathematical model without any climate data input") |
| Step / horizon | Hourly, 10 days | Daily, 5 days | Daily, 30 days |
| Calibration | Re-calibrated over the last 20 days each run; forecast **shifted to start at the observed flow** | Re-calibrated over 10 days, after each rain event | Refit each run |
| Output | Discharge with upper/lower bounds | **Instantaneous peaks**: each day's flow × the station's historical (instantaneous peak ÷ daily peak) ratio, ave/max/min | Min/ave/max analogue "ensemble" |
| Stations | 71 sub-catchments over all 7 major basins in 2015, 348 sites by 2023 | 94 coastal watersheds (Vancouver Island, South/Central Coast, North Coast/Skeena–Nass) | 439 stations province-wide, regulated ones included |
| Season | Built for the snowmelt freshet; now run year-round (daily on freshet weekdays, else once or twice a week) | Rain season; "only updated when a rainfall event is occurring or forecast" (the 2025 report says run year-round) | Year-round, once or twice a week |
| Published skill | 2015 freshet, 71 stations: 75% had 10-day relative mean abs. error ≤ 30%, but 75% had r² < 0.5 ("the trend of the streamflow could be very difficult to forecast"); Fraser at Hope 10% / r² 0.61 | "Accuracy … acceptable" on worked examples only; one case (Roberts Creek, 2018-01) observed 17.5 vs forecast lower bound 48.9 m³/s from forecast-rain error | 8 years × ~800 runs × 439 stations, scored by "observed falls inside the min–max band": best in July–August, **worst in April–May** (often 9–30%), better for level than discharge, interior better than coast |
| Rises | Its purpose | Its purpose | "forecasts of rise … are not recommended for management purposes because the ELF Model is not developed for forecasting rises and because the model has no meteorological data input" |

What it means here:
- **Only CLEVER and COFFEE can see a rise coming**; ELF extends the recent fall. A Change made of ELF beside a flood
  model would mix a model that knows tomorrow's rain with one that does not — the owner's "no elf" is the research's
  answer too.
- **COFFEE's day 0 sits a median 1.45x the gauge (T52) because its numbers are instantaneous peaks by construction**
  (daily flow × the station's historical peak ratio), not a model error; CLEVER's day 0 is ~1.02x because it is shifted
  onto the observed flow. Every ratio in the change is a model against ITS OWN today, so COFFEE's factor cancels.
- **Weight CLEVER higher? The research does not support it** for where both run. There is no head-to-head verification.
  CLEVER's published forecast skill is for the interior freshet (and is modest on trend); COFFEE is the model purpose-built
  for coastal storm basins in the rain season, which is exactly where and when the two overlap (122 stations on the
  2026-10-23 feed, all coastal). On that feed their peaks agree in the median (log2 CLEVER/COFFEE peak −0.007) and they
  make the same big-rise call at 82 of 122 stations. So: equal weights, and a one-line change if the owner wants CLEVER
  ahead (Q1).

Sources:
- CLEVER overview / map pages: https://bcrfc.env.gov.bc.ca/freshet/CLM_FOR_overview.html,
  https://bcrfc.env.gov.bc.ca/freshet/map_clever.html, publications list https://bcrfc.env.gov.bc.ca/freshet/cleverm_ref.html
- Luo (2015, upd. 2017), Technical Reference for the CLEVER Model: https://bcrfc.env.gov.bc.ca/freshet/cleverm_ref/CLEVER_TechRef.pdf
  (§3.5 operational settings, Tables 2–6, §4.3); Luo (2021), J. Hydrologic Eng. 26(4), doi:10.1061/(ASCE)HE.1943-5584.0002079
- COFFEE map page: https://bcrfc.env.gov.bc.ca/fallfloods/map_coffee.html; Luo (2018, rev. 2019), Technical Reference for
  the COFFEE Model: https://bcrfc.env.gov.bc.ca/fallfloods/TechnicalRefCOFFEEModel.pdf (abstract, §3.3, §5)
- ELF map page: https://bcrfc.env.gov.bc.ca/lowflow/map_elf.html; Luo (2023), Technical Reference for the ELF Model:
  https://bcrfc.env.gov.bc.ca/lowflow/ELF_TechRef.pdf (abstract, §9 Table 2, §10 "Forecasts of rise", §12)
- Luo (2024, rev. 2025), How BC River Forecast Centre flood forecasting models simulate impacts of climate change:
  https://bcrfc.env.gov.bc.ca/freshet/cleverm_ref/RFC_MODELS_CLIMATE_CHANGE.pdf (§1.4: both flood models run year-round;
  30-day / 15-day windows; COFFEE daily step converted to instantaneous flows)

## As built (lane-T144)

**Where it is decided: the feed** (`pipeline/gauges/feed/change.py`), because the choice is per station and per forecast
DAY and needs the runs' own daily series, which only the publisher holds (the app's index row carries one number).
The app only words it.

- `change_source(runs)` — THE decision, one function. Per day of the 10-day window: the FLOOD MODELS (`FLOOD_MODELS =
  ("CLEVER", "COFFEE")`) that reach that day make it — both → the weighted geometric mean of their ratios to their own
  today (`FLOOD_WEIGHTS`, both 1.0), one → its own; a day past COFFEE's five is CLEVER alone and `reach` records where
  each stops. No flood model → `LOW_WATER_MODEL` ("ELF") alone with `lowWater: true`. Never ELF beside a flood model; a
  model the policy does not name never speaks. `CHANGE_ORDER` (the panel's naming order, still generated into core) is
  now derived from those two names.
  Why geometric: ratios multiply and the donor pathway already averages log-ratios (`coded`), so a joined day is the
  same kind of mean every section already is.
- `run_days` (unrounded base + daily extremes) and `_measure` (peak/trough/back/dir) are each one copy, used by a run's
  own change AND by the station's. `station_change` now returns `(per model, source, summary)`.
- Feed shape: the station file gains `change` (the source: `models`, `lowWater`, `reach`, `base: 1`, `days` as ratios,
  `peak`, `trough`, `dir`); each run keeps its own `change`. The index row's `change` swaps `model` for `models` (+
  `lowWater` only when true). Nothing in the app read the old `model`.
- App: `@app/core` `SourceChange`, `changeSourceName` ("CLEVER and COFFEE together"), `changeSourceLine`,
  `changeReachNote` ("COFFEE runs to Mon; after that, CLEVER alone."), `CHANGE_LOW_WATER` ("ELF is a low-water model: it
  carries the river's recent fall forward and has no rain or snowmelt in it, so it cannot see a rise coming."). The
  station file's `change` rides `Observations` → `Series.change` (`feed/http.ts`, `bundle/shared.ts seriesOf`).
  `@app/ui` `changeView(runs, today, attribution, size, source)`: the source's line first with its day line; under a
  combined source each model's own line, quieter (`part`), no day line; ELF listed only when it IS the source, then with
  `caveat`; `note` for the reach. A feed from before T144 (no `source`) lists every run as before.
  `ConditionsPanel` hands `series.value?.change` over and draws `note` and `caveat` (`testID="change-caveat"`).

**On the 2026-10-23T03:50Z feed, replayed through the new rule (no network):** 122 stations CLEVER+COFFEE, 186 CLEVER,
23 COFFEE, 43 ELF (low-water caveat), 61 none. Against the T52 map (CLEVER-first lead): 20 stations change direction —
18 steady → rise (mostly Vancouver Island / coast creeks where COFFEE sees the weekend storm larger, e.g. 08HB034 2.0x →
5.0x, 08HB029 1.8x → 4.0x), 2 steady → fall (08CE001, 08EF001); none flips rise ↔ fall. 08HB025's 47x (CLEVER) and 544x
(COFFEE) join at 161x on Sun 25 Oct.

Shots (`handoff/PLAN/app-integration/T144-shots/`, the replayed feed, own ports 39417/39418, dark system theme):
- `desktop-dark-1-chilliwack-combined.png` — Chilliwack River: "CLEVER and COFFEE together: little change expected.",
  each model's own line under it, "COFFEE runs to Mon; after that, CLEVER alone."
- `desktop-dark-2-harrison-elf-caveat.png`, `phone-dark-2-harrison-elf-caveat.png` — Harrison River (08MG013, ELF only):
  "ELF: little change expected." and the low-water caveat.

## Tests (whole values; mutation → the tests it fails)
- `pipeline/tests/test_forecast_change.py` — new: joined geometric mean day by day (whole source + summary), one flood model
  alone (ELF present and left out), CLEVER alone equals its run (ratio, day, percentile, points), ELF alone + `lowWater`,
  a stale flood run hands over to ELF (never mixed), an unnamed model never speaks, the policy constants whole, and a
  no-second-copy test (only `change_source` reads the model names; `publish.py` names none). Recorded-run tests updated to
  the joined values (08HA003 31.69x on 10-25, 08HB025 160.66x) and the publisher test pins the station file's whole `change`.
- Mutations (all run, all fail as listed):
  - CLEVER weight 2 → joined-geometric-mean, policy-constants, 08HA003, 08HB025, publisher tests.
  - ELF beside the flood models (`flood + [ELF]`) → 7 tests incl. joined, one-alone, 08EE020, publisher.
  - `lowWater` never set → ELF-alone and stale-handover tests.
  - arithmetic mean instead of geometric → joined, 08HA003, 08HB025, publisher.
  - lead model only (no combining) → joined, 08HA003/08MH029/08EE020/08HB025, publisher.
  - publisher drops the station `change` → publisher test.
  - `changeView` ignores the source → ui joined / alone / ELF-caveat tests + mounted caveat test.
  - `ConditionsPanel` does not hand `series.change` over → mounted caveat test.
  - caveat not drawn → mounted caveat test.
  - feed adapter / `seriesOf` drops `change` → `http.test.ts` "carries the station's change".
  - caveat on any ELF line rather than only an ELF source → ui joined and alone tests.
  - reach note never said → core reach-note test + ui joined test.

## Checks
- Feed/forecast pytest (`test_feed_*`, `test_forecast*`, `test_gauge_policy`, `test_publish`), unfiltered: 183 pass.
  `emit_gauge_policy --check`: up to date (the generated `order` is unchanged).
- `REGS_STORE_REQUIRED=1 pnpm -C app check` via heavy.sh: tickets → … → gauge-policy → … → typecheck green; vitest 7299
  pass / 5 fail — the same five regs-store pairing failures T126 recorded (blob/store/settle/card/inputs parity: the
  province `regs.sqlite` is not the store the live release's taps were cut from), environment, not this change.

## Rebuild / republish
**Feed republish only (owner; no bundle, no deliver):** the next scheduled publish writes it, or by hand
`.venv/bin/python -m pipeline.gauges.feed.publish --out data/generated/gauges/feeds --clim data/generated/gauges/feeds/clim.json`.
No re-fetch needed: the change is recomputed from the runs already on disk. Until then the app shows the old per-model
list (no `source` in the station file) — the T52 behaviour, ELF line included.

## Adversarial review (2026-10-23)

Replayed the live feed of 2026-10-23 (363 stations with a change: 175 CLEVER, 123 CLEVER+COFFEE, 23 COFFEE, 43 ELF)
through `station_change` and served it to the web app on my own ports (tiles 39517, web 39518; own headless browser —
the shared Playwright browser was in use by another lane on :39541 and was left alone). Research spot-checked against
the cited PDFs: the ELF quotes ("not recommended for management purposes … no meteorological data input", lowest
accuracy in April and May, 439 stations, 30 days) and COFFEE's (94 watersheds, five-day forecast of instantaneous peaks,
§3.3 instantaneous-to-daily peak ratios) are verbatim. CLEVER figures not re-checked.

**Defect 1 — a handover was called a spike (feed + app).** Past COFFEE's fifth day the joined line is CLEVER alone, so
the joined value drops the day after COFFEE stops even when neither model forecasts a fall. `_back` read that drop as the
river coming back. On the live feed **48 of 123 joined stations** had a fabricated shape: 25 big rises said "back down by
Tue" that no model said (08HD027: COFFEE 11x and still rising on its last day, CLEVER 1.06x → "rises to about 3x on Mon,
back down by Tue"; 08PA012: neither run comes back → joined "spike"), 21 steady and 2 step. The builder's own recorded
test pinned the defect (08HA003 "spike, back 2026-10-27", a day after COFFEE's last).
Fix: `_measure(..., stops)` — the days, before the window's end, where a joined model stops (from `reach`). An end's
return is looked for only up to the first stop at or after it; not back by then, the end carries `until` (that day), and
the summary `shape` is `open`. One model alone has no stops: every run's own change is unchanged. App: `ChangeEnd.until`;
`changeSourceLine` words it "…, still up when COFFEE's forecast ends." / "still down …" (Englishman River 08HB002:
"CLEVER and COFFEE together: rises to about 8× today's flow by Sun, still up when COFFEE's forecast ends.").
No direction changed: only `shape`/`back`.

**Defect 2 — the chart under Change still drew ELF (app).** The owner said "no elf"; under "CLEVER and COFFEE together"
the chart below drew the freshest run, ELF ("ELF · 30 days", Englishman River and the builder's own Chilliwack shot).
The builder listed it as a follow-up; it is the owner's complaint on screen, so fixed: `changeChartModel(picked, horizon,
source)` in @app/ui — the reader's pick wins; under Change with no pick, `source.models[0]`; else undefined (freshest).
`ConditionsPanel` asks the freshest series once (shared with the default via `askOnce`) for the station change, then
charts and lists that model. Now "CLEVER · 10 days" under the joined line; Harrison (ELF only) still ELF with the caveat.

Tests added (whole values) and the mutation each fails:
- `test_a_handover_is_not_a_return` (+ 08HA003 re-pinned to `open`): `_back` ignoring `until` → both fail; `shape`
  ignoring `until` → both fail.
- core "says a joined move still on where one model stops": the `until` branch removed from `riseWords` → fails ("and stays
  up").
- ui `changeChartModel` whole-value case table: the pick ignored under Change → fails.
- mounted "charts the change's own lead model under Change": the panel passing `model ?? undefined` → fails.

Checks: feed/forecast pytest unfiltered, 211 pass / 5 fail — the same 5 fail on the builder's HEAD (`test_dfo_salmon`
×3, `test_update_hydat_script` ×2: generated data absent in the worktree), none in this change. `REGS_STORE_REQUIRED=1 pnpm -C app
check` via heavy.sh: every gate through typecheck green; vitest 7302 pass / 5 fail — the same five regs-store pairing
failures (blob/store/settle/card/inputs: province `regs.sqlite` f977e8646d7d vs oracles db98ffb0563359a2), environment.

Shots (`T144-shots/review-*.png`, the replayed feed): Englishman River combined in desktop dark, desktop light and phone
light (joined line with "still up when COFFEE's forecast ends", CLEVER charted); Harrison River ELF with the caveat in
light.

Not fixed (left):
- **Map/section level shows no low-water warning** (the builder's "section-level caveat" follow-up stands): an ELF-only
  gauge colours the Change map and leads a section's headline with no caveat; the index row carries `lowWater` for it.
- **The day line has a cliff at the handover** (joined value on COFFEE's last day, CLEVER's own the next); the reach note
  says why, the picture does not mark it.
- Q6 below.

## Owner questions (defaults built)
- Q1 Weighting where CLEVER and COFFEE both run. Built: EQUAL (geometric mean of ratios), because no head-to-head
  verification exists and the overlap is COFFEE's own domain (coast, rain season). Alternative: CLEVER 2 : COFFEE 1 —
  one line, `FLOOD_WEIGHTS` in `change.py` (the policy test pins it, so it is a visible decision).
- Q2 Past COFFEE's five days the joined line is CLEVER alone (said in the note). Alternative: stop the Change window at
  five days whenever COFFEE is in it.
- Q3 Under a joined change the panel still shows each model's own line, quieter. Alternative: the joined line only.
- Q4 Caveat words: "ELF is a low-water model: it carries the river's recent fall forward and has no rain or snowmelt in it,
  so it cannot see a rise coming." Alternative wording welcome.
- Q5 The +1/+3/+5/+10 chips are NOT on this rule (still `MODEL_ORDER` CLEVER → ELF, Q60): +10 can still be ELF where
  CLEVER's run ends before it. Default: leave; alternative: drive the day horizons from `change_source` too (ratio ×
  gauge), which also brings COFFEE in.

- Q6 (review) A joined rise not back while both models speak is OPEN at COFFEE's last day ("still up when COFFEE's
  forecast ends"), even where CLEVER alone comes back later (Englishman: CLEVER "back down by Tue"). Alternative: after
  the handover judge the return on CLEVER's own line (its fall below half of its own peak). Built: open — no model
  forecast the joined value's return.

## Left / follow-ups
- ~~The chart under Change still defaults to the freshest run~~ — fixed in the adversarial review (`changeChartModel`).
- **Section-level caveat**: a section whose donors are partly ELF-only (Harrison River: 08MG013 ELF 81% + 08MG001) shows the
  caveat only on the lead gauge's block; the section headline/range does not say part of it is a low-water model. The
  index row carries `lowWater`, so the donor rows could say it.
- **Wording mismatch (pre-existing, T52)**: the publisher calls 1.35x "steady" (big is ≥ 2x) while the app's donor row says
  "Expects about 35% more" (`STEADY_RATIO` 1.25) — Chilliwack shows both in one screen.
- **Verification we could run ourselves**: no history of runs is kept, so CLEVER vs COFFEE skill on BC stations cannot be
  measured here. Archiving each run's daily series (small) for a season would let Q1 be answered from data.
- `forecast.py`'s module docstring still says "the one with the more recent issue time wins" for overlapping models —
  stale since T52, untouched here.
- Browser note: the shared Playwright browser was on another lane's page (:39461) when this lane took its shots; it was
  navigated away to :39418.
