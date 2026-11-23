# T1043 — Capilano at Canyon still shows no Level in the app (0.8.6), though the Water Office page has it

Status: DONE 88648625f (lane L28, branch `fix/T1043-capilano-level`; app-only, ships in the next app build).

Source: owner 2026-11-23 (verbatim): "i still dont know why 0.8.6 capilano at canyon gauge doesnt have level like website
does here (https://wateroffice.ec.gc.ca/report/real_time_e.html?stn=08GA031) add as ticket".

Follows T830 (DONE 3eb8a4e28: a live level with no band is offered, unranked) and T817 (DONE: the water's Flow / Level
choices).

## Cause (lane L28, 2026-11-23, production read with GETs only)

**In plain words:** the data has been there since T830, but the Flow / Level switch on a water's Conditions page takes
its choices from the gauge's long record only. Capilano at Canyon has only two years of level record, so that list says
"flow only" and the switch never appeared. T830 added the live level to a different list that this page stopped
reading after T817.

Evidence, each checked, not assumed:
1. **EC's source carries the level.** `dd.weather.gc.ca/today/hydrometric/csv/BC/hourly/BC_08GA031_hourly_hydrometric.csv`
   (HTTP 200): every row has level and flow (13:35 PT: 0.592 m, 1.80 m³/s). The Water Office "Data Collection History"
   table: level is recorded continuously only since 2023 ("2023 - 2026 Flow & Level Continuous Recorder").
2. **Our production feed carries it.** `data.canifishthis.ca/feeds/live/index.json` entry 08GA031:
   `reports: ["discharge", "level"]`, `parameter: "discharge"`, a flow rank (0.05), no level rank.
   `feeds/live/08GA031.json`: `now.level` 0.592 and the level on every `recent` and `daily` row.
3. **Not a datum drop (T550).** `levelOnRating` passed: `reports` includes level (the worker leaves level out of
   `reports` only when the live level is off the rating).
4. **No level band, by rule.** `feeds/live/clim.json` `stations.08GA031` has `discharge` only; HYDAT holds 2024–2025 level,
   under `MIN_RECORD_YEARS` = 3 (T830's finding).
5. **Not the edge cache (T513).** The feed is current (observed 2026-11-23 14:35 PT, fetched 22:05 UTC).
6. **The share card** (`feeds/cards/08GA031.json`) carries one quantity by design (`parameter: "discharge"`); it is not
   the app's card.
7. **The app.** Production web (canifishthis.ca, the same code as the phone app), Capilano River → Conditions: the gauge
   card says "1.8 m³/s · 0.59 m stage" and draws NO Flow / Level switch (`T1043-shots/before-prod-web-capilano-conditions.png`).
   Code: the water's view (`WaterViews.tsx`) passes T817's `offer` = `conditionsOpening(measured)`, where `measured` is
   each speaking gauge's BANDED quantities (`useSpeakerMeasures` → `gaugeParameters`, the bundle's `gauge_clim`).
   `cardQuantity` (`ui/answers.ts`), given `offer`, built the switch from `asked` + `offer` and ignored `available`, the
   list T830 extended with `reports` (`cardParameters`). T817 (6d5179875, 11-16 22:11) and T830 (11-17) were built in
   parallel; T830's tests called `cardQuantity` without `offer`, the path the water view never takes, so they stayed green.

## Fix (built, lane L28, 88648625f)

`cardQuantity` with the water's choices also keeps what the lead gauge itself offers (`available`: banded + `reports`).
The lead is the same gauge under either pick, so T817's rule (the switch is the same before and after a pick) holds.
Under Level, Capilano shows T830's agreed form: 0.59 m, no dot, "Too new to say if this is high or low.", the 72 h line
with no band. A T550 other-datum level is not in `reports`, so it stays out, as before.

App-only. No feeds worker change, no data release, no staging feeds deploy needed.

Tests (mutation-proved: drop `|| available.includes(q)` → both fail): `ui/cardQuantity.test.ts` (the switch offers Level
under Flow and under Level with the water's choices; an other-datum level stays out), `ui-native/WaterConditions.levelOnly.test.tsx`
(Capilano's shape through the real `WaterConditions`: "Flow* Level" → Level "0.59" + the no-record line, no dot → Flow
again; other-datum: no switch).

## Pictures (`T1043-shots/`, web 390 px)
- `wateroffice-08GA031.png`: the Water Office page, level and flow on one graph (2026-11-23).
- `before-prod-web-capilano-conditions.png`: production canifishthis.ca, Capilano River → Conditions: "1.8 m³/s ·
  0.59 m stage", no Flow / Level switch.
- `after-web-capilano-flow.png`: this branch, local web over PRODUCTION data (read only): the switch shows Flow | Level.
- `after-web-capilano-level.png`, `after-web-capilano-level-top.png`: Level picked: "0.59 m", "Too new to say if this is
  high or low.", the 72 h level line with no band. (Local dev fonts did not load, so the type face differs from the app.)

## Seen, not fixed here
- Under Level the chart's axis ticks repeat ("0.59 0.59 0.6 0.6"): ticks are written with `figure` (T830 kept them so)
  and Capilano's level moves only ~0.01 m in 72 h. A follow-up if the owner wants it.
- Under Level the "Estimate for this spot" block above the card still speaks of flow (it is ranked on flow; nothing
  ranks Capilano's level). Unchanged by this fix.

## Not verified
- Not run on an Android or iOS build (web only; the code path is shared `ui` + `ui-native`).
