# T1043 — Capilano at Canyon still shows no Level in the app (0.8.6), though the Water Office page has it

Status: PARTIAL — 88648625f (Level offered), eb4591227 (axis labels); left (Claude): item 2 below, the estimate's words under Level, waits on a wording pick (lane L28, branch `fix/T1043-capilano-level`; app-only).

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

## Follow-ups in this ticket (coordinator 2026-11-23: fix both here, no new number)

1. **Axis labels repeated under Level** ("0.59 0.59 0.6 0.6"). Cause: `niceTicks` stepped 0.005 m for a line moving
   0.01 m, finer than the label writes (`hydrograph.ts` `fmt`: two decimals under 10). Fixed, eb4591227: a linear axis spans
   at least four of the label's finest steps (`minSpan`: 0.01 under 10, 0.1 under 100, 1 under 1,000, 100 above),
   widened about the middle and never below 0. One rule for every station and both quantities; an axis that already reads
   apart is left alone. Capilano now reads 0.58 · 0.59 · 0.6 · 0.61 (`T1043-shots/after-web-capilano-level-axis.png`).
   Tests: `hydrograph.test.ts` "(T1043) no two gridlines carry the same label" (Capilano's real 72 h level; a lake at 340 m
   moving 3 cm; 1.8 m³/s; ~1,000 m³/s; a line crossing 10); mutation: drop the `minSpan` call -> 2 fail.
   The public gauge page (`tools/seo/gaugeClient.ts`) draws its own axis with `figure` labels and was not changed.
2. **"Estimate for this spot" speaks of flow while Level is picked.** Not built: it needs a wording pick.
   Following the pick is ruled out: owner 2026-11-10 "Flow leads, Level auxiliary", applied in T971 (2a2aa0e75): the
   estimate is the lead's, in Flow, whatever the card shows (`ui/panel/conditions.ts` `estimateQuantity`). No existing
   words say the estimate is flow-based; the nearest is core `standingRankedIn` ("… (ranked on flow)"), written for a
   gauge reading. Options are in the lane report; shot: `T1043-shots/after-web-capilano-level-top.png`.

## Not verified
- Not run on an Android or iOS build (web only; the code path is shared `ui` + `ui-native`).
