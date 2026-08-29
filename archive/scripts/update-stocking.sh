#!/usr/bin/env bash
# update-stocking.sh — Light tier of the two-tier stocking refresh.
#
# This is the CHEAP, geopandas-free half (wired to .github/workflows/
# update-stocking.yml, weekly):
#   1. Pull anglerinfo.db from R2 read-only (for the FIDQ stocking release rows).
#   2. Refresh the FIDQ stocking release rows locally via fetch_stocking update
#      (no geopandas; new releases for already-matched waterbodies land here).
#   3. Export per-waterbody records → cron/stocking/records/<waterbody_id>.json
#      + stocking_index.json. NO reach resolution here — the reach→waterbody
#      match table (stocking_matches.json) ships with the full pipeline build.
# It never writes anglerinfo.db back to R2 — the heavy job owns that.
#
# The HEAVY, EXPENSIVE half (full fetch+match chain, geopandas, WFS downloads)
# is pipeline.recurring.anglerinfo.build_db, wired to .github/workflows/
# refresh-stocking-db.yml (monthly + manual). It's the only writer of
# anglerinfo.db to R2, and where genuinely NEW waterbodies enter the system.
#
# Usage:
#   ./scripts/update-stocking.sh              # refresh + resolve (local)
#   ./scripts/update-stocking.sh --seed        # also re-seed local R2
#   ./scripts/update-stocking.sh --upload      # refresh + resolve + upload to R2 (CI)
#
# Environment:
#   DEPLOY_ENV   staging | production (default: staging)
#                Controls which R2 bucket + worker origin to use.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$ROOT/output/pipeline/deploy"

# ── Environment config ──────────────────────────────────────────────

DEPLOY_ENV="${DEPLOY_ENV:-staging}"

case "$DEPLOY_ENV" in
  staging)
    R2_BUCKET="bc-fishing-regulations-staging"
    R2_ORIGIN="${R2_ORIGIN:-https://data-staging.canifishthis.ca}"
    ;;
  production)
    R2_BUCKET="bc-fishing-regulations"
    R2_ORIGIN="${R2_ORIGIN:-https://data.canifishthis.ca}"
    ;;
  *)
    echo "ERROR: Unknown DEPLOY_ENV=$DEPLOY_ENV (use staging or production)" >&2
    exit 1
    ;;
esac

echo "Environment: $DEPLOY_ENV (bucket: $R2_BUCKET)"

mkdir -p "$DEPLOY_DIR"

# ── Step 0: Fetch poly_reaches.json from R2 if not present locally ──
# In CI there's no pipeline output — pull the wbk->reach_id map from R2
# (written by pipeline/enrichment/builder.py's own build step).

# R2 I/O goes through the shared S3/boto3 helper (same transport as every cron).
_fetch_r2_file() {
  local r2_key="$1" dest="$2"
  python -m pipeline.recurring.r2_storage get "$r2_key" "$dest"
}

# ── Step 1: Refresh stocking records (light tier) ───────────────────
# Two-tier design (see refresh-stocking-db.yml for the heavy tier):
#   • Heavy monthly job runs the full fetch+match chain (geopandas) and is the
#     ONLY writer of anglerinfo.db to R2 — it's the source of match_final, i.e.
#     which FIDQ waterbodies map to an FWA waterbody_key. New waterbodies enter
#     the system here.
#   • This light job pulls that db read-only (for the release rows), then
#     refreshes just the FIDQ stocking release rows locally via fetch_stocking —
#     no geopandas, no db write-back to R2 (avoids racing the heavy job). New
#     releases for already-matched waterbodies enter the system here.
ANGLERINFO_DB="$ROOT/output/pipeline/anglerinfo/anglerinfo.db"
DB_R2_KEY="cron/stocking/anglerinfo.db"

if [[ ! -f "$ANGLERINFO_DB" ]]; then
  echo "── Fetching anglerinfo.db from R2 ($DB_R2_KEY) ──"
  mkdir -p "$(dirname "$ANGLERINFO_DB")"
  _fetch_r2_file "$DB_R2_KEY" "$ANGLERINFO_DB"
fi

echo "── Refreshing FIDQ stocking records (fetch_stocking update) ──"
python -m pipeline.recurring.anglerinfo.fetch_stocking update

# ── Step 2: Export per-waterbody records ────────────────────────────
echo "── Exporting per-waterbody stocking records ──"
mkdir -p "$DEPLOY_DIR/cron/stocking"
python -m pipeline.recurring.stocking.resolver records \
  --out "$DEPLOY_DIR/cron/stocking"

echo "✅ records/ + stocking_index.json → $DEPLOY_DIR/cron/stocking/"

# ── Step 3: Upload / seed (optional) ────────────────────────────────

if [[ "${1:-}" == "--upload" ]]; then
  echo "── Uploading cron/stocking/ tree to R2 ($R2_BUCKET) ──"
  python - "$DEPLOY_DIR/cron/stocking" <<'PY'
import sys
from pathlib import Path
from pipeline.recurring.r2_storage import put_tree, storage_from_env

# ~2300 per-waterbody record files: independent PUTs, latency-bound on CI, so
# upload them in parallel via the shared helper (no version.json here → nothing
# to fence). Serial uploads were the bulk of this job's runtime.
root = Path(sys.argv[1])
n = put_tree(storage_from_env(), root, "cron/stocking", version_last=False)
print(f"uploaded {n} file(s) under cron/stocking/ (parallel)")
PY
  echo "✅ Uploaded to R2"

elif [[ "${1:-}" == "--seed" ]]; then
  echo "── Re-seeding local R2 ──"
  node "$SCRIPT_DIR/seed.mjs" --force
  echo "✅ Local R2 refreshed"
fi
