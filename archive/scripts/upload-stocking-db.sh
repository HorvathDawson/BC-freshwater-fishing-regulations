#!/usr/bin/env bash
# upload-stocking-db.sh — Heavy tier of the two-tier stocking refresh.
#
# This is the MANUAL half. The full anglerinfo fetch+match chain
# (pipeline.recurring.anglerinfo.build_db) needs geopandas, the ~10 GB FWA
# GeoPackage (data/bc_fisheries_data.gpkg, built by data/fetch_data.py) and
# live gov WFS access — far too heavy for GitHub Actions, so it runs LOCALLY
# on a slow cadence (monthly-ish, or whenever new waterbodies need matching).
#
# After rebuilding the db locally, this script publishes it to R2 as the shared
# match_final state the weekly light job (update-stocking.sh) pulls read-only.
# It is the ONLY writer of anglerinfo.db to R2.
#
# Typical run:
#   python -m pipeline.recurring.anglerinfo.build_db --export   # rebuild db (heavy, local)
#   DEPLOY_ENV=production ./scripts/upload-stocking-db.sh        # publish db to R2
#
# Environment:
#   DEPLOY_ENV   staging | production (default: staging)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DB="$ROOT/output/pipeline/anglerinfo/anglerinfo.db"
DB_R2_KEY="cron/stocking/anglerinfo.db"

DEPLOY_ENV="${DEPLOY_ENV:-staging}"
case "$DEPLOY_ENV" in
  staging)    BUCKET="bc-fishing-regulations-staging" ;;
  production) BUCKET="bc-fishing-regulations" ;;
  *) echo "ERROR: Unknown DEPLOY_ENV=$DEPLOY_ENV (use staging or production)" >&2; exit 1 ;;
esac

if [[ ! -f "$DB" ]]; then
  echo "ERROR: $DB not found — rebuild it first:" >&2
  echo "  python -m pipeline.recurring.anglerinfo.build_db --export" >&2
  exit 1
fi

# Fail loud if the db is missing match_final (a partial/aborted build) — the
# light job resolves stocking off this table, so publishing a db without it
# would silently empty stocking.json.
if ! sqlite3 "$DB" "SELECT 1 FROM sqlite_master WHERE type='table' AND name='match_final';" | grep -q 1; then
  echo "ERROR: $DB has no match_final table — the match chain did not complete." >&2
  echo "  Re-run: python -m pipeline.recurring.anglerinfo.build_db" >&2
  exit 1
fi

SIZE="$(du -h "$DB" | cut -f1)"
echo "── Publishing anglerinfo.db ($SIZE) → r2:$BUCKET/$DB_R2_KEY ($DEPLOY_ENV) ──"
rclone copyto "$DB" "r2:$BUCKET/$DB_R2_KEY" --s3-no-check-bucket --checksum --progress
echo "✅ anglerinfo.db published. The weekly light job will pick it up on its next run."
