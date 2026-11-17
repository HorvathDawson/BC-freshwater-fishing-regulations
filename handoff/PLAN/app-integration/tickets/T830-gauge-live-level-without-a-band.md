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
