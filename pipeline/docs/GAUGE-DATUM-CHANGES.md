# Gauge datum changes: what was moved, why, and how to undo it

A water-level gauge measures height above its own zero (its "datum"). When Environment Canada moves a gauge or resets its
zero, the live readings jump by a fixed amount, while the 30+ years of history (HYDAT) stay on the old zero. Live and
history no longer compare, so the app had stopped ranking these levels: under **Level** it drew the live line alone, with
no normal range, and said "Too new to say if this is high or low." (T550, T1047).

Accepting a change shifts the station's **history** by the stated amount, so the history sits on today's zero. The live
readings are not touched. Only level is moved; flow (m³/s) is not affected by a gauge's zero and is unchanged.

The list of changes: `data/curated/gauges/datum_change_stations.json` (one row per change). Only a row with status
`accepted` moves anything. Code: `pipeline/gauges/feed/datum.py` (the one reader) and `pipeline/gauges/feed/climatology.py`
(the one consumer: it builds `clim.json`, which feeds both the bundle's normal range and the live feed's ranking).

## Accepted 2026-11-24 (owner)

Asked in plain words whether to shift the history of 08HD006 (5.0 m lower since 2025-06-05) and 08EF001 (2.37 m higher
since 2025-01-30) so their normal range comes back, the owner answered, verbatim:

> "Yes but make sure it is a clean well documented update that happens which is easy to see"

The other three proposed rows (07FD001, 08KH019, 08MH153) were not asked about and stay `proposed`: they move nothing.

### 08HD006, Salmon River near Sayward: history moved 5.0 m down

- **Environment Canada's note** (HYDAT `STN_REMARKS`, verbatim): "The station was relocated on June 5, 2025, 1.5km
  downstream to the Sayward Road bridge- the original station location from establishment in 1956 to 1981. The datum
  from that period is preserved, with original benchmark elevations used (converted from feet to meters). This datum was
  not preserved when the station was relocated in 1981 and again in 2015 to the most recent location. The datum change
  results in a ~5m drop in stage on the 2025 relocation date."
- **Date**: 2025-06-05 (every level before this day is shifted).
- **Offset**: −5.0 m (the history moves DOWN 5.0 m). The note says "~5m", so the size is approximate; with −5.0 m all 300
  live days of 2026 checked sit on the record's flow-to-level curve (0 of 300 without it).
- **Before**: under Level, "3.18 m · Too new to say if this is high or low.", the live line alone, key "no normal range
  (short record)". Today's normal range was 6.74–8.16 m (10th–90th), 5 m above every live reading.
- **After**: the normal range is back at 1.74–3.16 m (10th–90th) and the reading is ranked: "Far higher than normal ·
  higher than almost every day on record for the date (ranked on flow)" (the dot follows flow, which leads; the level's
  own rank is also top-tenth).
- Pictures (web, 390 px, light; the same live reading of 2026-11-24 07:10 PT, only the history differs):
  `handoff/PLAN/app-integration/T1047-shots/accepted-08HD006-before.png`, `…/accepted-08HD006-after.png`.

### 08EF001, Skeena River at Usk: history moved 2.37 m up

- **Environment Canada's note** (verbatim): "Station moved approximately 1km upstream of original location on
  2025-01-30, causing a 2.370m jump in stage due to change in elevation."
- **Date**: 2025-01-30.
- **Offset**: +2.37 m (the history moves UP 2.37 m). With it all 300 live days of 2026 checked sit on the record's curve
  (110 of 300 without it).
- **Before**: "5.05 m · Too new to say if this is high or low.", the live line alone, "no normal range (short record)".
  Today's normal range was 2.22–3.43 m, about 2 m under the reading.
- **After**: normal range 4.59–5.80 m, the reading inside it: "Normal · higher than 6 days in 10 for the date (ranked on
  flow)" (level's own rank: 46th percentile).
- Pictures: `handoff/PLAN/app-integration/T1047-shots/accepted-08EF001-before.png`, `…/accepted-08EF001-after.png`.

A moved gauge is also a new cross-section of the river: the shift lines the zeros up, but the band's shape is still the old
site's. That is the remaining caveat for both.

## What exactly changes in the data (proved 2026-11-24, HYDAT 20261019)

`clim.json` built before and after the acceptance (1,951 stations): only these two stations' entries differ, and in each
only the **level** band, the earlier-years line (`recentYears`, 2024) and the flow-to-level curve (`ratings`). Flow bands,
record lengths (`stats`) and every other station are byte-identical. The live feed (both publishers, the Python one and
the feeds worker, run on one recording, 433 files the same JSON) then puts `level` back in the station's `reports` and
ranks it; nothing else in the feed moves.

## Where it shows up

- The climatology build prints one line per accepted change, e.g.
  `08HD006 level record moved 5.0 m down to the gauge's new zero (Environment Canada note, 2025-06-05)`.
- The data release's checks (`python -m pipeline release --staging`, stage 4) list every gauge level record the release
  moves compared with the live release, one line per station, in the output, in
  `data/generated/release/checks/<r-id>/gauge_records.txt` and on the release's ledger record (`checks.gauge_records`).
  Undoing an acceptance is listed the same way ("… back on HYDAT's own zero …").

## How it goes live

No app build is needed. The next data release does it: `pipeline build` reruns its `clim` stage (its key holds the
accepted rows), which rewrites `clim.json` and the bundle's normal ranges, and the release upload puts both on R2; the
feeds worker reads the new `clim.json` on its next run and ranks the level. The bundle and `clim.json` must go up
together: a `clim.json` on R2 ahead of the bundle would rank the level against the OLD band in the app.

## How to undo it (one command per station)

```
CURATED_WRITE_BY="<your name>" .venv/bin/python -m pipeline.gauges.feed.datum --set 08HD006 proposed
```

This puts the row back to `proposed` (its decision record is removed; git keeps the history, and every write is backed up
and logged in `data/generated/curated_writes/`). The next data release then puts that station's history back on HYDAT's
own zero, and its checks say so. To accept a row:
`… --set <station> accepted --by owner --on YYYY-MM-DD --asked "<the question>" --words "<the answer, verbatim>"`.
