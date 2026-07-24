"""stocking — decoupled FIDQ stocking: fetch-shaping (cron) vs matching (pipeline).

    resolver.export_records — CRON: reads anglerinfo.db's fidq_stocking_records and
        writes per-waterbody cron/stocking/records/<waterbody_id>.json + stocking_index.json.
        No reach resolution, no geopandas.
    resolver.build_matches  — PIPELINE (full build): reads match_final + poly_reaches.json
        and writes the static cron/stocking/stocking_matches.json
        (reach_id/waterbody_key → [waterbody_id]).

The frontend joins reach_id → waterbody_id via stocking_matches.json, then fetches
records/<waterbody_id>.json. The cron carries NO matching logic.

Re-run on a schedule by .github/workflows/update-stocking.yml via
scripts/update-stocking.sh (records only); the match table ships with a full build.
"""
