# Gauge datum changes: what was moved, why, and how to undo it

A water-level gauge measures height above its own zero (its datum). When Environment Canada moves a gauge or resets its
zero, the live readings jump by a fixed amount while the HYDAT history stays on the old zero, so the two no longer
compare and the app cannot rank the level (it draws the live line alone: "Too new to say if this is high or low.").

Accepting a change shifts the station's **history** by the stated amount onto today's zero. Live readings are never
touched, and only level moves: flow (m³/s) does not depend on a gauge's zero.

- The list: `data/curated/gauges/datum_change_stations.json`, one row per change. Only a row with status `accepted`
  moves anything; `proposed` and `rejected` rows move nothing.
- The code: `pipeline/gauges/feed/datum.py` (the one reader) and `pipeline/gauges/feed/climatology.py` (the one
  consumer: it builds `clim.json`, which feeds the bundle's normal range and the live feed's ranking). With no accepted
  row, `clim.json` is byte-identical (`test_datum_changes`).
- An accepted change moves only that station's level band, its earlier-years line (`recentYears`) and its
  flow-to-level curve (`ratings`); flow bands, record lengths and every other station stay as they were.
- A moved gauge is also a new cross-section of the river: the shift lines the zeros up, but the band's shape is still
  the old site's.

## Accepted (owner, 2026-11-24)

Asked whether to shift the history of 08HD006 and 08EF001 so their normal range comes back, the owner answered:
"Yes but make sure it is a clean well documented update that happens which is easy to see". The rows' `decision`
records the question and the words. 07FD001, 08KH019 and 08MH153 stay `proposed`.

| Station | Moved | From | Environment Canada's note (HYDAT `STN_REMARKS`) | After the shift |
|---|---|---|---|---|
| 08HD006, Salmon River near Sayward | history 5.0 m down | 2025-06-05 | relocated 1.5 km downstream to the Sayward Road bridge, on the 1956-1981 datum: "a ~5m drop in stage" | all 300 live days of 2026 sit on the record's flow-to-level curve (0 of 300 without it); the reading ranks again |
| 08EF001, Skeena River at Usk | history 2.37 m up | 2025-01-30 | moved about 1 km upstream, "a 2.370m jump in stage" | all 300 live days sit on the curve (110 of 300 without it); the reading is inside the normal range |

Pictures (web, 390 px, light; one live reading, only the history differs):
`handoff/PLAN/app-integration/T1047-shots/accepted-08HD006-before.png`, `…-after.png`,
`…/accepted-08EF001-before.png`, `…-after.png`.

## Where it shows up

- The climatology build prints one line per accepted change, e.g.
  `08HD006 level record moved 5.0 m down to the gauge's new zero (Environment Canada note, 2025-06-05)`.
- The data release's checks (`python -m pipeline release --staging`) list every gauge level record the release moves
  against the live release: in the output, in `data/generated/release/checks/<r-id>/gauge_records.txt` and on the
  release's ledger record (`checks.gauge_records`). An undo is listed the same way.

## How it goes live

No app build is needed. The next data release reruns the `clim` stage (its key holds the accepted rows), rewrites
`clim.json` and the bundle's `gauge_clim`, and the upload puts both on R2; the feeds worker ranks the level on its next
run. Safe order: merge, the staging data release, then the production upload.

One early path exists, on STAGING only: the staging feeds worker starts `feeds-hydat.yml` on `main` by itself when a
newer HYDAT is listed (`app/workers/feeds/src/index.ts`, the `hydat` maintenance event). Then staging's `clim.json`
moves before the staging data release, and for these gauges the live line sits metres off the app's band until that
release. Production's `clim.json` changes only through `deploy/prod/upload.sh`, together with its release.

## How to undo it (one command per station)

```
CURATED_WRITE_BY="<your name>" .venv/bin/python -m pipeline.gauges.feed.datum --set 08HD006 proposed
```

The row goes back to `proposed` (its decision record removed; every write is backed up and logged in
`data/generated/curated_writes/`), and the next data release puts that station's history back on HYDAT's own zero. To
accept a row: `… --set <station> accepted --by owner --on YYYY-MM-DD --asked "<the question>" --words "<the answer,
verbatim>"`.
