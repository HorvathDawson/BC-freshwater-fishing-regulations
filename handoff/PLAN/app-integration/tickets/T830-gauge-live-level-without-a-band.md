# T830 — A gauge that reports a live level but has no level history shows no Level (Capilano at Canyon, 15 more)

Status: OPEN (app chat; worker + data layer + app; one owner pick in part 2).

Source: owner 2026-11-17 ~00:50, in the app chat (verbatim): "also for gauges should look into why capilano river at
canyon gauge doesnt have level but i can find level for it online like here
https://metrovancouver.org/services/water/seymour-and-capilano-river-levels with 2023 - 2026 Flow & Level Continuous
Recorder here https://wateroffice.ec.gc.ca/report/real_time_e.html?stn=08GA031" and "other gauges might have same issue
as that".

## Cause (app chat, 2026-11-17 ~01:00; read from code + the live staging feed, not run in the app)
EC's realtime CSV for 08GA031 carries level and flow on every row. Our level BAND comes from HYDAT history
(`pipeline/gauges/feed/climatology.py`, `record.py` MIN_RECORD_YEARS = 3). Capilano's HYDAT level record is
2024-2025 only (2 years), so `clim.json` has a discharge band and no level band. Then:
- the index entry has no level at all (`publish.ts` ~L1050); the station file does carry the raw level
  (`now: {discharge 25.7, level 1.187}`; all 94 recent rows);
- the card offers no Level toggle (choices come from `gauge_clim`: `sql/queries.ts:196`, `ConditionsPanel.tsx:126-138`);
  under Flow it already prints "25.7 m³/s · 1.187 m" (`ui/panel/conditions.ts:220`);
- **bug**: with the map on Level, `otherOnly` (`data/panel.ts:387-392`) finds no level percentile and the gauge row says
  "Reports flow only today, not water level" (`core/conditions.ts:474`). That is false.

## Sweep (427 roster stations, EC fetched 2026-11-17, all HTTP 200)
398 have a level band and rank. 16 report a live level and show none: 8 with no HYDAT level at all, 4 with a record
too short (none cut by a datum shift), and others. Biggest by sections led: Nazko 08KF001 (985), Beatton 07FC008 (327),
Witches Brook 08LG009 (131), Capilano 08GA031 (72), B.X. Creek 08NM123 (41), Vernon Creek 08NM160 (20).
A further 5 drop their live level on purpose (T550: the live level is off the rating = another datum; 1,495 sections).
Full list: app chat scratch `gauge-level/stations_no_level.csv` (copy into this ticket's folder when built).

## Work
1. (bug, no pick needed) Python says which quantities a station reported live (worker index, `publish.py` and
   `publish.ts` twins), so no row says "flow only" for a station that reports level.
2. (owner pick) What a level with no band shows. Recommended: the raw live level in metres, unranked (no high/low word,
   words from Python), the Level toggle offered when the station reports level live, the 30-day line drawn without a
   band. Rejected: lowering MIN_RECORD_YEARS to 2 (not a normal; it also gates which gauges a panel admits); a band
   from the 30-day record (not a climatology); mapping the flow band onto level via the rating (adds nothing, datum risk).

2026-11-17 08:37, owner (main chat, verbatim): "Yes still show gauge data." So pick 2 = the recommended: the raw live
level in metres, unranked, Level offered when the station reports level live, the 30-day line without a band.

## Built — 2026-11-17 (app chat lane, branch `app-fix/t830-level-no-band`; not merged, not deployed)

**Cause (verified).** As the 01:00 note says: the index's per-quantity entries are RANKS, so a station with a live level
and no level band (Capilano at Canyon: HYDAT level 2024-2025 only, under `MIN_RECORD_YEARS` = 3) had no level anywhere
in the index; `otherOnly` read "no level rank" as "no level" and the app said "flow only". On the 2026-11-17 staging
feed 17 stations report a level live with no level rank (the sweep's 16 + one more), list in `T830-files/`. A second
fault showed once Level was offered: the donor panel read its rows against the day's MIDNIGHT, so every reading that
cannot be ranked made after 06:00 was past the 6 h age line and was named "Not reporting" (the gauge switch beside it,
asked at the clock, said reporting).

**Fix.**
1. Publisher, both twins: the index entry carries `reports`, the quantities the reading carries, ranked or not
   (`publish.live_quantities` / `liveQuantities`). A level off the record's rating (T550, the 5 other-datum stations)
   is left out on purpose, so those keep exactly today's behaviour. Written after `parameter` in both; the worker's
   change-aware cache keeps it (whole-entry compare in `gauges.robust`).
2. App: `otherOnly` answers nothing for a quantity in `reports` (no "flow only" row, pin ring, or panel refusal; under
   Level Capilano's panel says the existing "None of the gauges … can be ranked against its record"). The card's
   quantities are the bundle's banded ones plus `reports` (`cardParameters`); under a quantity offered from `reports`
   alone the card shows the raw figure in metres, NO dot, NO standing word, and Python's line instead
   (`record.NO_RECORD_WORDS`, emitted by `gauge_policy`): **"Not ranked: no long record of its level to compare
   with."** — a proposal for the owner's eye. The 72 h chart draws the line with no band (its key already says "no
   normal range (short record)"); Year says the existing "This station has no envelope in this quantity…". The map
   under Level leaves Capilano's dot off (no rank), where it drew a grey "reports flow only" ring. A feed from before
   `reports` behaves exactly as before.
3. The donor panel is asked at `ConditionsVM.ref` (today: the clock; a picked day: its B.C. midnight; a saved spot:
   its day, unchanged).

**Files.** `pipeline/gauges/feed/{publish,record}.py`, `pipeline/tools/emit/gauge_policy.py` (+ generated
`core/gauge-policy.generated.ts`), `app/workers/feeds/src/gauges/publish.ts`, `app/tools/gauge-feed-contract.ts`,
`core/conditions.ts`, `data/{feed/http,panel,types}.ts`, `ui/{answers,chart,hooks/gauge,panel/conditions,panel/donor}.ts`,
`ui-native/{ConditionsPanel,DonorPanel}.tsx`, test-bed scenario `gauge-level`.

**Tests (each mutation-checked: the guarded line removed, the test fails).** Python `test_feed_reports.py` (6);
worker `gauges.reports.test.ts` (vectors through the repo's Python + a recording run with 07FB002's level band taken
away), gauge parity/contract/robust/age green; data `livePanel` (Capilano under Level: no-record, not flow-only; an old
feed and an other-datum level still flow-only), `feed/format` (now() carries `reports`); ui `cardQuantity`
(`cardParameters`, `noRecordLine`), `panel.silent` (asked at the clock); ui-native `ConditionsPanel` (Level offered,
1.19 m, Python's line, no dot, no flow standing; old feed: no Level). Runs: pytest on the 26 gauge/publish/policy files
(298 passed), `python -m pipeline.tools.emit --check` 0, `pnpm -C app run check:fast` 0, vitest ui/data/core +
Conditions/Donor/water ui-native + feed-contract/one-* tools (3489 passed after two whole-value expectations took the
new `reports` field), worker gauge tests 49 passed, `typecheck:worker` 0.

**Pictures** (app chat scratch `t830-shots/`): web, 390 px, light + dark, staging data, Capilano River → Conditions.
Before = the branch base; after = this branch with the live staging index given `reports` in the browser (computed from
each station file's `now`, the 5 other-datum levels left out: what the publisher will write), since the deployed worker
does not write it yet. `web-before-flow-*` / `web-after-flow-*` (Flow: unchanged, the Flow/Level choice now shows),
`web-after-level-*` (Level: "1 m", the no-record line, the 72 h line without a band), `web-before-maplevel-*` /
`web-after-maplevel-*` (Conditions map on Level: the grey "flow only" ring at the gauge is gone).
Android (test bed, emulator, build d29767238 clean, patched MapLibre; heavy lane 2, 10 min): new scenario
`gauge-level` on the staging TEST channel. Before = the test index as it stood (no `reports`): Flow ranked as before, no
Level choice (`android-before-flow.png`). After = the test channel's `test/feeds/live/index.json` replaced for the run
by the live index + `reports` (computed as above), then put back byte-for-byte (backup taken first): Flow unchanged
(`android-after-flow.png`), Level shows "1 m", "Not ranked: no long record of its level to compare with.", the 72 h line
with no band (`android-after-level.png`); 1/1 PASS. `install` PASS. `gauge-offline` FAILED both steps, I believe
not from this change: the Conditions view drew (estimate, route map), but the scenario looks for "m³/s" on the first
screen and the gauge card is below the fold (`android-gauge-offline-step1.png`). Its check needs a scroll; not fixed here.

**Left.**
- Deploy the feeds worker (and the Python publisher's next run) so the live index carries `reports`; until then the
  app behaves as before. Ship with an app build containing this branch.
- Owner's eye: the line's words; whether Year should draw this year's levels with no band instead of the "no envelope"
  sentence; the figure is rounded to 2 decimals (0.998 m shows as "1 m"; existing `figure`).
- Fix `gauge-offline`'s first-screen check (scroll to the card first).
- The README row stays OPEN until the worker is deployed.

## Owner, 2026-11-17 ~09:57 (app chat), after the Android/web pictures, picks verbatim
- The line under an unranked level: picked "Plainer" = "Too new to say if this is high or low." (instead of "Not ranked:
  no long record of its level to compare with.").
- Level readings to 2 decimals ("1.00 m", "0.85 m") instead of whole metres: "Depends how much space this adds. But yes".
  So 2 decimals, shown with pictures of the space it takes (reading, chart axis, tap read-out).

## Built — the 09:57 picks (2026-11-17 ~11:35, commit 15c9a3c34)

- The line is `record.NO_RECORD_LINE` = "Too new to say if this is high or low." (one line for either quantity).
- Levels to two decimals: `record.LEVEL_DECIMALS = 2` (emitted by `gauge_policy`); core `figureIn(v, quantity)` (half up,
  as `figure`) wherever a level is written: the card's figure and the stage beside a flow, the chart read-out and its
  median, a gauge pin's label, the alert editor (prefill, context line, summary, notification), a spot's row and lead-up
  strip and trend, the public gauge card. Flow unchanged. Chart axis ticks stay `figure` (round values: "1.5", "10").
- Space: the big figure grows 1-3 characters ("1" → "1.00", "10.3" → "10.30"); fits at 390 px, light and dark, web and
  Android. The read-out line was ALREADY cut by its one-line ellipsis at 390 px before this (measured 269 px of text in a
  202 px slot; the median goes first), now 276 px: the value itself still shows for levels under 100 m. Map dots carry
  no figures (T636), so nothing there moves.
- Tests (mutation-checked): core `standing` (`figureIn`), `conditions` (stage, read-out), ui `gaugePoints`, `chart.render`
  (a level series' read-out), data spot words, ui-native `ConditionsPanel` (0.998 m → "1.00"), `Notifications` (alert
  prefill "3.20"), worker `cards` ("1.00 m"), Python `test_feed_reports` (the picks are the policy the app reads). Six
  existing expectations moved to the new form. Full app vitest 6145 passed; worker 520; pytest 299; emit --check 0;
  check:fast 0.
- Pictures (`t830-shots/`): `web-2dp-{before,after}-{capilano,harrison}-level-{light,dark}.png`,
  `android-2dp-{before,after}-{capilano,harrison}-level.png` (Android after: build 15c9a3c34, scenario `gauge-level` with
  `GAUGE_LEVEL_DP=2`, both PASS; Capilano "1.00 m · Too new to say…", Harrison Lake 08MG012 "10.30 m").
