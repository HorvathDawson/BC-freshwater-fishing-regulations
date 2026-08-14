"""hydro — recurring hydrometric gauge data (ECCC Wateroffice + BC River Forecast Centre).

Graduated from the standalone ``live-data/hydro/`` POC (mirroring how
``pipeline/recurring/anglerinfo`` graduated from ``live-data/anglerinfo``).

Modules:
    hydro_poc     — fetch engine + CLI (``bootstrap`` / ``update``) into a hydro.db
    fetch_hydat   — HYDAT bulk metadata sync + day-of-year percentile climatology
    match_fwa     — gauge → FWA waterbody matcher (atlas-time; writes gauge_fwa_match)
    gauge_matches — static reach_id/waterbody_key → station_id match table (pipeline-time)
    export_hydro  — shapes a hydro.db → R2-ready JSON tiers (stations / gauges.geojson /
                    recent / history / climatology); no gauge→reach resolution here
    jobs          — the single unified cron entrypoint (``run``)
    serve         — local viewer + cron simulator (dev/reference only)

ONE unified cron (``jobs run``), stateless — NO persisted DB in R2. State lives in
the JSON artifacts; an ephemeral /tmp sqlite is scratch only. It gates internally:
    every run — stations.json + gauges.geojson + recent/ (map + chart)
    daily     — merge history/ (idempotent by date)
    HYDAT     — rebuild climatology/ + refresh coords (only when the release changes)
The gauge→reach match is a STATIC artifact (gauge_matches.json) built once by the
full pipeline; the frontend joins reach_id → station_id at lookup time.
"""
