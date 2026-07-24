"""hydro — recurring hydrometric gauge data (ECCC Wateroffice + BC River Forecast Centre).

Graduated from the standalone ``live-data/hydro/`` POC (mirroring how
``pipeline/recurring/anglerinfo`` graduated from ``live-data/anglerinfo``).

Modules:
    hydro_poc     — fetch engine + CLI (``bootstrap`` / ``update``) into hydro.db
    fetch_hydat   — HYDAT bulk metadata sync + day-of-year percentile climatology
    match_fwa     — gauge → FWA waterbody matcher (atlas-time; writes gauge_fwa_match)
    export_hydro  — shapes hydro.db → R2-ready JSON tiers (stations / recent / history / climatology)
    serve         — local viewer + cron simulator (dev/reference only)

Two production cadences (see README.md):
    frequent (~15 min) — ``hydro_poc update --bc`` → ``export_hydro --scope realtime``
    nightly            — history + climatology re-serialize; rebuilds the small seed DB
"""
