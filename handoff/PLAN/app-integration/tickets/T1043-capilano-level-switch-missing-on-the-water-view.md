# T1043 — Capilano at Canyon still shows no Level in the app (0.8.6), though the Water Office page has it

Status: DONE 88648625f, eb4591227, f7cdf00bb, 228006397, 66978f86d, 19a8485bb, 2602d207b, bdcd0f27d (lane L28, branch `fix/T1043-capilano-level`, PR #77; app-only, ships in the next app build).

| id | title | status | commits | what's left |
|---|---|---|---|---|
| T1043 | Capilano at Canyon still shows no Level in the app (0.8.6): the water view's Flow / Level switch reads the long record only | DONE | 88648625f, eb4591227, f7cdf00bb, 228006397, 66978f86d, 19a8485bb, 2602d207b, bdcd0f27d | owner 2026-11-23: "i still dont know why 0.8.6 capilano at canyon gauge doesnt have level like website does here (https://wateroffice.ec.gc.ca/report/real_time_e.html?stn=08GA031) add as ticket"; lane L28 |

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
2. **"Estimate for this spot" speaks of flow while Level is picked.** Following the pick was ruled out (owner 2026-11-10
   "Flow leads, Level auxiliary", T971 2a2aa0e75: the estimate is the lead's, in Flow).

## Owner, 2026-11-23 (verbatim, through the coordinator)
"add ranked on flow at top. also the scale of the chart should use the year data if not enough histroical data exists."

## Built — the 2026-11-23 answers
- **"(ranked on flow)" at the top of the estimate box** (f7cdf00bb). Under the "ESTIMATE FOR THIS SPOT" heading, while the
  card shows the other quantity: core's existing words, now one function (`rankedOnWords`, which `standingRankedIn` reads
  too); `donorView` `rankedNote` (null when the card and the estimate agree), drawn in `DonorPanel`. A level-only water
  under Flow would say "(ranked on water level)" by the same rule. Test: `ConditionsPanel.test.tsx` "(T971) …" now also
  pins the note (none under Flow, "(ranked on flow)" under Level); mutations: note always null -> fails; note always
  shown -> fails.
- **A 72 h chart with no normal range is scaled to its year** (228006397, 66978f86d). With no band on screen the axis spans
  that quantity's year (`yearScale`: this year's daily values and the earlier years) with the usual padding. The year is
  read from the feed's own record (`draftSeriesOf`, the join the Year view's values come from): the release's Year series
  is null for exactly these stations (no envelope, `seriesOf`), which is why the first cut (228006397, asking the
  release) drew nothing new; 66978f86d fixed it. It is asked only once the 72 h series is known to have no band, so a banded
  chart never asks and keeps today's scale. No feed to read, no value in it, or a failed read (reported by `useAsync`,
  T368): the drawn values and `minSpan` as the last step. The hydrograph says which rule set the range
  (`Hydrograph.scale`: band / year / reading / log), so each path is visible to a test. Capilano's level now draws as a
  near-flat line low on a 0-2 m axis. The Year view itself is unchanged ("no envelope in this quantity").
  Tests: `hydrograph.test.ts` "(T1043) a chart with no band is scaled to its year" (5) and `chart.render.test.tsx`
  "(T1043) a bandless 72 h chart is scaled to its year" (4: the feed's year read; no feed -> reading; a failed read
  reported, reading; a banded chart never asks). Mutations: drop the year widening; let a band be overridden; pass no
  `scaleTo`; ask regardless of the band -> each fails a test.
- The public gauge page (`tools/seo`) is not touched here: it gets this through the shared chart once PR #73 moves it onto
  the app's chart.
- Shots (local web over production data, 390 px): `T1043-shots/after2-web-capilano-level-top.png` ("(ranked on flow)"
  under the heading), `after2-web-capilano-level-72h.png` (the 72 h level on its year's scale), `after2-web-capilano-flow-banded.png`
  (Flow, a banded chart: unchanged against `after-web-capilano-flow.png`).

## Independent review of #77 (2026-11-23) — fixed
1. **Gun Creek (08ME030): the note and the switch disagreed** (a level band only, a live flow 7.8 m³/s: "Level only: no
   flow is measured here." over "Flow Level*"). Decision: the water's opening sees each speaker's live `reports`, joined
   as the card joins them (`useSpeakerMeasures` -> `cardParameters`), because a live unranked flow is still flow (T830
   offered it; "Flow leads"). Gun Creek now opens on Flow, unranked ("Too new to say if this is high or low."), the
   estimate box says "(ranked on water level)", and no level-only note (19a8485bb). The lead note "No gauge here reports
   flow today, so the gauge shown is chosen on water level." was then false over that flow: not drawn when the lead reports
   a live flow (bdcd0f27d). Tests: Gun Creek's real shape through `WaterConditions`, with and without the feed index.
2. **Year scale race** (2602d207b): the year answer carries its key, so a switch never draws station B (or Level) on
   A's (or Flow's) year; the chart is held (pane not revealed) until this key's year has settled or failed. Test: a switch
   between two bandless stations records every drawn frame: none at the reading scale, none on the other's year.
3. **08HD006's off-datum live level** (19a8485bb): `stationReading` drops a figure the publisher left out of `reports`, so
   2.06 m is never today's reading (no stage under Flow; under Level the card says it reports flow only today). Test:
   08HD006's real shape. The chart under Level still draws the off-datum line against the band: **T1047** (owner pick).
4. **Robust year range** (2602d207b): `yearScale` is the 5th to 95th percentile of the year's values (Capilano's level
   year 0.52-2.96 m no longer sets the axis); the 72 h values and today's reading stay inside it.
Mutations, each fails one test: take the year whatever its key; drop the hold; raw min/max; the opening without `reports`;
the lead note without its check; keep an unstood figure.
Shots: `T1043-shots/review-capilano-level-72h-p5-p95.png`, `review-gun-creek-opening.png`,
`review-08HD006-salmon-sayward-level.png` (local web over production data, 390 px).

## Not verified
- Not run on an Android or iOS build (web only; the code path is shared `ui` + `ui-native`).
