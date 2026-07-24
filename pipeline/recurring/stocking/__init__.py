"""stocking — resolve FIDQ stocking data to reach IDs → cron/stocking/stocking.json.

    resolver — reads the anglerinfo.db fetch+match artifact + poly_reaches.json,
               maps stocking waterbody_keys to reach IDs.

Re-run on a schedule by .github/workflows/update-stocking.yml via
scripts/update-stocking.sh.
"""
