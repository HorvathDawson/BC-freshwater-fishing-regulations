"""recurring — jobs re-run on a schedule against a live site, not a one-shot static build.

Each job type lives in its own subpackage:
    in_season/  — scrape + resolve in-season regulation changes (6-hourly cron)
    stocking/   — resolve FIDQ stocking data to reach IDs (nightly cron)
    anglerinfo/ — fetch + match angler-info data (slow, manual cadence)
    hydro/      — hydrometric gauge data (realtime + nightly crons)

Shared: provenance.py (uniform provenance block on every cron artifact).
"""
