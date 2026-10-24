# T177 — Combine CLEVER and COFFEE for every forecast day (horizons and Like-your-visit), weighted by measured skill

Status: DONE — b039349e (feed) + 73dd133e (app, Q161), lane-T177. Live after a FEED REPUBLISH (below); weights stay
EQUAL (too few runs to weigh — measured below), the run archive now accumulates the record that will decide them.

Source: owner 2026-10-24 (Q160): "should combine models for change. look into which models is best to figure out how to
combine." Today the +1/+3/+5/+10 horizons and T171's `aheadDays` take the FIRST model that answers a day
(`change.day_model`), while the Change already combines CLEVER and COFFEE (geometric mean of ratios, equal weights —
T144 found no published head-to-head verification).

## Work
1. MEASURE which model is better, from our own data: the feed keeps every model run it fetched (`forecasts.<MODEL>`,
   "925 model runs", runs carried forward) and the observed daily means (`daily`, ECCC 30-day archive). Score each model's
   forecasts against what then happened, per station × lead day (1–5, 10) × season where data allows: bias, error on
   the percentile scale, and hit rate on the low/normal/high band the app shows. Archive runs going forward if the history
   is too short (a small append-only store in the feed), and say how long until the score is trustworthy.
2. DECIDE the combination from the score: equal weights where neither is better; skill weights (e.g. inverse error) where
   one is; per station or per region/season only if the data supports it. Write the numbers and the rule into this
   ticket and decisions.md.
3. ONE function combines a day — used by the Change (T144), the horizons and `aheadDays` (T171) — replacing first-model
   for days both reach; COFFEE stays rescaled to the gauge before combining (T171); ELF stays a last resort with its
   low-water caveat, never beside a flood model.
4. Tests whole-value; a no-second-copy pin (one combine); measured feed diff (which stations' horizons move).
Ships: feed republish (local; R2 owner) + app words if the panel names the combination. Size: M (measurement first).

## As built (lane-T177)

Commits: b039349e (feed: one combination + run archive), 73dd133e (app: Q161 trip-day capture), then the ticket/decisions commit.

### 1. Measured skill (2026-10-24, from what this machine holds)
**There is no run history to speak of.** The feed kept only the newest run per model (a carried run is overwritten when
its issue time moves), so the only past runs on disk are the test fixtures:

| snapshot | runs | scoreable against observed |
|---|---|---|
| `fixtures/gauge_feed/forecast_change` (issued 2026-10-22, fetched 22:28Z) | 5 stations x CLEVER/COFFEE/ELF | lead 1 (23 Oct, a day 77 % observed when measured) |
| same, 08CG001 (issued 2026-10-15) | CLEVER/COFFEE/ELF | none: the station has reported nothing since 14 Oct |
| `fixtures/gauge_feed/forecast_days` (issued 2026-10-23) | 3 stations | none: their lead days are not over |
| `fixtures/gauge_feed/clever_clock/observed_vs_forecast.json` (CLEVER 10-22, 304 stations) | 40 h of CLEVER | none: its observed series ends before the forecast's day 1 |
| the live feed (`data/generated/gauges/feeds`, runs issued 10-23) | 925 runs | none yet: lead 0 only |

So: **5 paired stations, 1 issue day, lead 1 only; leads 2-10: 0 cases.** Scored with the feed's own ranking
(`skill.score`; each model's daily mean on the gauge's scale, ranked by the station's discharge envelope for the day;
error in log2, 1.0 = a factor of two; `pmae` on the 0-1 percentile scale; `hit` = same low/normal/high band as the
observed day, core `standing` collapsed at p25/p75):

| lead 1, paired (n = 5: 08EE020, 08HA003, 08HB025, 08HF006, 08MH029) | bias | log2 error | percentile error | band hit |
|---|---|---|---|---|
| CLEVER | +0.17 | 0.19 | 0.036 | 0.8 |
| COFFEE (gauge-scaled) | +0.72 | 0.72 | 0.151 | 0.8 |
| CLEVER+COFFEE joined (equal) | +0.45 | 0.45 | 0.091 | 0.8 |
| ELF (reference, same cases) | +0.22 | 0.25 | 0.071 | 0.8 |
| head-to-head (CLEVER error - COFFEE error) | mean -0.53, SE 0.56 | | | |
| **without 08HF006** (n = 4) | CLEVER 0.075, COFFEE 0.054, joined 0.055, difference +0.02 +/- 0.04 | | | |

All of COFFEE's deficit is one case: 08HF006, where COFFEE's run of 10-22 forecast a storm rise (~23 m3/s on the gauge's
scale) and the gauge read 2.2. With it out, COFFEE is marginally the better of the two. Neither result is evidence.

**Published verification:** none found for either model head to head (searched 2026-10-24: RFC model pages, technical
references, Google; T144's research stands — CLEVER's 2015 freshet figures, COFFEE's worked examples only, ELF's
min-max band hit rates). Sources: https://bcrfc.env.gov.bc.ca/freshet/map_clever.html,
https://bcrfc.env.gov.bc.ca/fallfloods/map_coffee.html, https://bcrfc.env.gov.bc.ca/freshet/CLM_FOR_overview.html,
https://bcrfc.env.gov.bc.ca/freshet/cleverm_ref/RFC_MODELS_CLIMATE_CHANGE.pdf. The WMO's 2025 Guidelines for Streamflow
Forecast Verification (WMO-No. 1364, https://wmo.int/node/31193) frame the same measures (bias, accuracy, reliability).

**The run archive (built, so skill accumulates):** `pipeline/gauges/feed/skill.py`. Every publish appends each run it
has not seen (station, model, issue time) to `<feed dir>.runs/<YYYY-MM>.jsonl` — BESIDE the published directory, never
in it (the notifier's precedent). A line is the run's daily means for today..+10, the gauge reading and the gauge scale.
Closed months gzipped, 14 months kept. **Measured** on the scratch republish: 825 runs = 270,632 B (328 B a run),
42,494 B gzipped; ~825 new runs a day -> ~8 MB a month raw, ~1.3 MB gzipped, ~32 MB at steady state.
`python -m pipeline.gauges.feed.skill --feed data/generated/gauges/feeds` prints the table and the verdict per lead.
**When it is trustworthy:** the rule asks for 200 paired cases over 20 issue days per lead. CLEVER and COFFEE overlap
at ~100-120 coastal stations, but COFFEE runs only when rain is on or forecast and one storm moves every station at once,
so issue days are the binding count: about **4-6 weeks of the fall storm season** for leads 1-4 (COFFEE stops at 5);
a per-season answer (freshet vs fall) needs **a full year**. Per region needs more again; not attempted.

### 2. The rule chosen
- **EQUAL weights** (`change.FLOOD_WEIGHTS` unchanged) — the measurement cannot tell the models apart.
- `skill.verdict`: below 200 paired cases or 20 issue days -> equal; mean paired difference within 2 SE -> equal
  (neither better); else inverse-error weights (1 / each model's paired mean log2 error, the better one at 1). The feed
  never re-weights itself: a forecast must not change because a file grew overnight; a person moves `FLOOD_WEIGHTS`
  with the table. Written into decisions.md ("CLAUDE 2026-10-24 — T177").
- Per region / season: not supported by the data; the archive is what will say.

### 3. One combination (feed, b039349e)
- `change.combine_day(values)` — THE join of one forecast day: the flood models that answer, weighted geometric mean
  (`FLOOD_WEIGHTS`), one model exactly its own value; else ELF alone with `lowWater`; never ELF beside a flood model; an
  unnamed model never answers. `model_name` names a joined day `CLEVER+COFFEE`.
- Asked by: the Change (`change_source` picks the station's runs, then joins each day's ratios with `combine_day` —
  Change values byte-identical, every T144 test unchanged), the horizons (`publish.forecast_percentiles`: each run's step
  on the gauge's scale, joined, ranked once), every ranked day (`change.forecast_days`), and the skill score. T171's
  first-model `day_model` is deleted.
- COFFEE still rescaled to the gauge before joining (`gauge_scale`, T171); without a reading it cannot answer and CLEVER
  speaks alone. ELF stays the last resort with `lowWater`.
- App: reads `aheadDays.model` / `ahead[h].model` only to tell an ELF day, so `CLEVER+COFFEE` needs no app change
  (docstring in `@app/data` `panel.ts` updated).

### Measured feed diff (scratch republish, never into `<repo>/data`)
Scratch republish 2026-10-24T02:58Z of a copy of the live feed (`<scratchpad>/t177/feed177`, 433 stations, 825 runs
carried, no series re-fetched), then HEAD's code and this lane's code replayed over the SAME station files (the
replay of the new code equals the published index at all 433 stations):
- **Moved: 100 stations — exactly the ones where CLEVER and COFFEE both run**, on the days both reach: horizons +1 and +3
  (200 station-horizons), `aheadDays` days 1-4 (400 station-days). +5, +10, days 5-10, every CLEVER-only, COFFEE-only and
  ELF-only station: unchanged. The Change summary: unchanged at every station.
- Size of the move on the percentile scale: horizons median 0.037, mean 0.078, max 0.48; 63 of 200 moved >= 0.10,
  16 >= 0.25; 45 changed only their model name (values clamped at p5/p95). Days: median 0.042, max 0.48; 130 of 400
  >= 0.10, 34 >= 0.25. **69 strip days at 43 stations change band** (low/normal/high).
- Biggest: 08CE001 +3 CLEVER 0.747 -> joined 0.271 (the two disagree widely); 08HD007 +3 0.153 -> 0.583; 08HF006 +1
  0.95 -> 0.603; 08HB025 +1 0.05 -> 0.378.
- `aheadDays` models now: CLEVER 2363, ELF 819, CLEVER+COFFEE 400, COFFEE 80 station-days.
- Index: 347,332 -> 350,118 B raw (+0.8 %, the longer model names); gzip 47,927 -> 48,337 B.

### Tests and mutation pins (feed)
`pipeline/tests/test_forecast_combine.py` (new, 32): `combine_day` whole cases (geometric join, ELF left out, policy
order, one model exact — 18.0 on p90 stays 0.95, a zero join), the names; **no second copy** (`combine_day` holds the only
`math.exp`, the Change / days / horizons / score call it, `day_model` exists nowhere, publish names no model); one built day
read three ways (`forecast_days` 0.4653 = sqrt(8 x 14) ranked, horizons the same, Change sqrt(1.4) = 1.1832); COFFEE
without a reading; 08HF006 recorded days whole (each joined day between its two models); the publisher writing
`CLEVER+COFFEE` and archiving each run once beside the feed; archive records whole, 14 months, gzip and read-back; the
score whole on a built archive; a day only one model reached kept out of the head-to-head; `verdict` cases.
`test_forecast_days.py`, `test_forecast_change.py` updated (Chilliwack now joined on 24-27 Oct; the no-second-copy pin).
Mutations, each run, each failed: first model only (16 failures), arithmetic mean (13), ELF beside the flood models (24),
CLEVER weight 2 (14), COFFEE ranked unscaled (8), lowWater never set (9), horizons on a first-model rule (4), archive
inside the feed dir (1), archive never dedupes (1), joined day named by its lead model (7), score pairing unequal cases
(1, after adding the test), verdict ignoring "too few" (2, after adding the cases).

## Q161 — a spot records its TRIP day (owner 2026-10-24, folded into this lane; T170's "Owner answer" holds the record)
"it should save the date that you set as the time of trip in the add a spot". Built (73dd133e):
- **Capture** (`@app/data` `captureSpot`): a trip on an earlier local day than today never reads the live feed. Its lead
  gauge's flow is that day's daily mean (`fillDayFlow`, the one read `flowOnDay`: feed daily series incl. HYDAT years,
  else one GeoMet request when online; discharge only), its reading keeps the station and no value, and its panel is the
  section's panel as now with each member's daily mean that day ranked with core `percentileInBand`, answered by
  `answerFrom` (`panelOnDay`; `panelDay: {state: "day", date}`). A member that cannot be read yet (offline, the day not
  over) leaves the panel `pending` with its members frozen (station ids and catchments only — no section, AGENTS 5); a
  refresh fills it. A trip today: unchanged.
- **Migration**: a spot saved before this for an earlier day (a save-day panel, no `panelDay`) is due a refresh
  (`needsDayPanel`; "The gauges for the day of this visit have not been read yet. They can be filled in from the day's
  record."); the refresh rebuilds its panel for the trip day from the frozen rows (`panelOfAnswer`: the member whose
  frozen share was raised over its plain one keeps leading). Its recorded reading is never rewritten, only not shown; a
  spot whose reading was taken on the trip day is never touched.
- **Said** (`savedGauges`, SpotScreen): the trip day's panel with "Each gauge's daily mean on 20 Oct 2026, the day of the
  visit."; a panel still to read: "The gauges for 20 Oct 2026 have not been read from their record yet. Refresh fills
  them in."; no panel: "This water's gauge reading for the day of the visit is under Flow that day." T170's "The estimate
  above was read when this spot was saved (…), not on the day of the visit." is gone (`DAY_FLOW_WORDS.notTheDay` deleted).
  The draft shows "Flow that day" when it read one. The Spots list row shows the trip day's flow, never the save day's.
- Plumbing: `CaptureInput.history` (HYDAT + online) from App -> Shell (`hydat`) -> SpotCapture -> `useCaptureSpot`.
- Tests (whole values): `capture.tripday.test.ts` (new, 6): 3 days ago freezes that day's percentile (0.375) and panel
  (`answerFrom` of the day's 0.375/0.75, not today's 0.05); today unchanged (live reading, live panel, no day fields);
  offline -> pending -> a refresh online fills flow and panel; the migration refills a save-day panel and keeps the
  recorded reading and flow; a trip-day reading untouched; `panelOfAnswer` round trip with its leader. Updated:
  `dayFlow.test.ts`, `words.test.ts` (list row), `dayFlow.render.test.tsx`, `DayFlowBlock.test.tsx` (pending / day /
  flow lines whole, light and dark, the save-day 11.2 absent, the draft's day line). **No second copy**
  (`tools/one-history-read.test.ts`): `flowOnDay(` only in dayFlow.ts; `fillDayFlow(` by capture, refresh, App only;
  `capturePanelOnDay(`/`fillDayPanel(` by capture and refresh only; `panelDay:` written in dayFlow.ts only; `gaugeNow(`
  once in capture.ts, on the today branch.
- Mutations, each failed: capture reading live for an earlier day (2), a panel frozen with a member unread (1), a legacy
  save-day panel not refilled (3), a trip-day reading's panel refilled (1), save-day gauges shown (9), capture asking no
  day flow (2), the list row showing the save-day reading (1), the leader dropped from a frozen answer (1).
- **Shot** (`../T177-shots/`, this lane's own Expo :39711 and own headless Chromium; data :39217 GET only, the live local
  feed): adding a spot "At my location" on the Vedder River for Tue 20 Oct 09:00 (today Fri 23 Oct) —
  `add-earlier-day-when-light.png`, `add-earlier-day-panel-light/dark.png` (4 gauges' daily means: "Very low for the time
  of year", 1st-17th percentile), `add-earlier-day-flow-light/dark.png` ("Each gauge's daily mean on 20 Oct 2026, the day
  of the visit." and "13.4 m3/s, at or below the 10th percentile for the date — … 20 Oct 2026"). Open-Meteo's archive
  does not answer from this machine (curl times out), so the shot browser refused it and the spot shows no weather.

## Checks
- Feed/forecast pytest, unfiltered (18 files incl. the new one): 231 passed.
- `REGS_STORE_REQUIRED=1 pnpm -C app check` via `scripts/heavy.sh appcheck 30` (read-only `.venv` symlink, removed
  after): every gate green (tickets, boundaries, platform, style, colours, spinner, fixture,
  gauge-policy `--check`, calendar, fish, answers, pack, deps, typecheck); vitest 8183 passed / 24 skipped, 0 failed.

## Adversarial review
- **Overfitting on a few runs**: the rule refuses to weigh on 5 cases; the 1-SE interval spans zero; dropping one storm
  flips the sign. Weights stay equal; the verdict needs many issue days, not many stations (one storm moves all at once).
- **COFFEE's peak scaling**: COFFEE is scaled by observed / its own today. Where COFFEE forecasts a storm its day-0 base is
  a peak-ratio-inflated number too, so the scale is right only if the peak ratio is the same on the storm day as today —
  it is a station constant in COFFEE (Luo 2018 §3.3), so the ratio cancels. The one big miss (08HF006) was a forecast
  rise that did not come, not a scaling artefact (its day 0 sat at the gauge after scaling, by construction).
- **Geometric mean with a near-zero model**: a zero joins as 1e-9 and collapses the join towards zero (pinned). It is the
  honest geometric reading; it cannot occur from COFFEE (a 0 base cannot be scaled) and has not occurred on the feed.
- **Stations with one model**: CLEVER-only, COFFEE-only and ELF-only stations are byte-identical to before (measured,
  and pinned: a single model's value is returned exactly, so 18.0 on p90 still ranks 0.95 — the first draft's log round
  trip made it 0.9 and a test caught it).
- **Handover cliff**: a joined day 4 followed by CLEVER alone on day 5 can step (08MH029 days 4-5) — the same handover
  T144 already words for the Change ("COFFEE runs to …"); the strip does not say it. Left (owner Q3).
- **Level**: the horizons join level too where both models forecast a stage (COFFEE needs a level reading to be scaled);
  stages near or below zero would be floored in a join. No station on the feed joins a level today (COFFEE's level runs
  need an observed level and none answered). Left as is; noted.
- **Q161 extra requests**: a backdated capture asks the feed once per panel member (the station files) and GeoMet once per
  member the feed does not carry (Vedder: 4 files, 3 GeoMet requests for members never in the feed). The lead station
  is read twice (day flow + panel member). Acceptable; a cache is a follow-up.
- **Q161 "last report not known"**: a member with no flow that day is listed "Not reporting (last report not known)" —
  words written for today's panel. Left (owner Q5).
- Capture hangs while Open-Meteo hangs (pre-existing: the weather port has no timeout) — seen in the shot. Left, noted.

## Rebuild / republish
**Feed republish (the parent republishes the live local feed; R2 owner):**
`.venv/bin/python -m pipeline.gauges.feed.publish --out data/generated/gauges/feeds --clim data/generated/gauges/feeds/clim.json`.
It starts the run archive at `data/generated/gauges/feeds.runs/` (beside the feed, never uploaded — make sure the R2
sync uploads only the feed directory). No bundle, deliver or release. App: ships with the next app build.

## Owner questions (default built)
1. Weights: EQUAL until the archive says otherwise (rule above). Alternative now: CLEVER 2 : COFFEE 1 (one line,
   `FLOOD_WEIGHTS`) on the strength of today's one-storm sample — not recommended.
2. Check the skill table in ~6 weeks (`python -m pipeline.gauges.feed.skill --feed data/generated/gauges/feeds`) and move
   the weights by its verdict? Default: yes, as a ticket then.
3. The strip/horizon step where COFFEE stops (joined day 4, CLEVER alone day 5) is not marked. Default: leave.
4. Archive retention 14 months (~32 MB, beside the feed, not uploaded). Default: yes.
5. (Q161) A panel member with no flow that day reads "Not reporting (last report not known)". Default: leave; alternative
   "no record for that day".
6. (Q161) At capture the day's flow says "filled in from the day's record" (T170's words). Default: leave; alternative
   drop "filled in" when it was read as the spot was added.
