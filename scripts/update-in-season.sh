#!/usr/bin/env bash
# update-in-season.sh — Scrape + resolve in-season regulation changes.
#
# Shared between local dev and GitHub Actions so the pipeline never diverges.
#
# Usage:
#   ./scripts/update-in-season.sh              # scrape + resolve (local)
#   ./scripts/update-in-season.sh --seed        # also re-seed local R2
#   ./scripts/update-in-season.sh --upload      # resolve + upload to R2 (CI)
#
# Environment:
#   DEPLOY_ENV   staging | production (default: staging)
#                Controls which R2 bucket + worker origin to use.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$ROOT/output/pipeline/deploy"
MATCHING_DIR="$ROOT/output/pipeline/matching"

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

mkdir -p "$DEPLOY_DIR" "$MATCHING_DIR"

# ── Step 0: Fetch data files from R2 if not present locally ─────────
# In CI there's no pipeline output — pull tier0 + match_table from R2 via the
# shared S3/boto3 helper (direct R2 API, no bot-protection on the public origin).

# R2 I/O goes through the shared S3/boto3 helper (same transport as every cron).
_fetch_r2_file() {
  local r2_key="$1" dest="$2"
  python -m pipeline.recurring.r2_storage get "$r2_key" "$dest"
}

if [[ ! -f "$DEPLOY_DIR/tier0.json" ]]; then
  echo "── Fetching tier0.json from R2 ──"
  _fetch_r2_file "tier0.json" "$DEPLOY_DIR/tier0.json"
fi

if [[ ! -f "$DEPLOY_DIR/match_table.json" ]]; then
  echo "── Fetching match_table.json from R2 ──"
  _fetch_r2_file "match_table.json" "$DEPLOY_DIR/match_table.json"
fi

# ── Step 1: Scrape ──────────────────────────────────────────────────

echo "── Scraping in-season changes ──"
python -m pipeline.recurring.in_season.scraper \
  --match-table "$DEPLOY_DIR/match_table.json" \
  --quiet

# ── Step 2: Resolve ─────────────────────────────────────────────────

echo "── Resolving to reach IDs ──"
mkdir -p "$DEPLOY_DIR/cron/in-season"
python -m pipeline.recurring.in_season.resolver \
  --tier0 "$DEPLOY_DIR/tier0.json" \
  --match-table "$DEPLOY_DIR/match_table.json" \
  --out "$DEPLOY_DIR/cron/in-season/in_season.json" \
  --quiet
# Legacy dual-write (one release) so the webapp can cut over to cron/ paths
# without an outage — see plan Part F3. The workflow's jq summary reads either.
cp "$DEPLOY_DIR/cron/in-season/in_season.json" "$DEPLOY_DIR/in_season.json"

echo "✅ in_season.json → $DEPLOY_DIR/cron/in-season/in_season.json (+ legacy root)"

# ── Step 3: Upload / seed (optional) ────────────────────────────────

if [[ "${1:-}" == "--upload" ]]; then
  echo "── Uploading to R2 ($R2_BUCKET) ──"
  # New canonical key + legacy key (dual-write during cutover).
  for key in "cron/in-season/in_season.json" "in_season.json"; do
    python -m pipeline.recurring.r2_storage put \
      "$DEPLOY_DIR/cron/in-season/in_season.json" "$key"
  done
  echo "✅ Uploaded to R2"

elif [[ "${1:-}" == "--seed" ]]; then
  echo "── Re-seeding local R2 ──"
  node "$SCRIPT_DIR/seed.mjs" --force
  echo "✅ Local R2 refreshed"
fi
