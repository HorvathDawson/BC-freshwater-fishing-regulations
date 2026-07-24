#!/usr/bin/env bash
# update-stocking.sh — Resolve FIDQ stocking data to reach IDs.
#
# Mirrors scripts/update-in-season.sh's shape, with one deliberate difference:
# there is no "scrape" step here. pipeline/recurring/anglerinfo/'s own
# fetch+match chain (fetch_stocking.py -> match.py -> match_fwa_*.py ->
# match_final.py -> export.py) is a separate, slower, manual cadence — this
# script assumes anglerinfo.db is already fetched/matched locally and just
# resolves its stocking rows to reach IDs.
#
# NOT yet wired into a GitHub Actions workflow (unlike update-in-season.sh's
# .github/workflows/update-in-season.yml) — stocking-info display isn't built
# in the frontend yet, so there's nothing consuming stocking.json on a
# schedule. Run manually, or call from CI once that lands.
#
# Usage:
#   ./scripts/update-stocking.sh              # resolve (local)
#   ./scripts/update-stocking.sh --seed        # also re-seed local R2
#   ./scripts/update-stocking.sh --upload      # resolve + upload to R2 (CI)
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

if [[ ! -f "$DEPLOY_DIR/poly_reaches.json" ]]; then
  echo "── Fetching poly_reaches.json from R2 ──"
  _fetch_r2_file "poly_reaches.json" "$DEPLOY_DIR/poly_reaches.json"
fi

# ── Step 1: Resolve ─────────────────────────────────────────────────

echo "── Resolving stocking data to reach IDs ──"
mkdir -p "$DEPLOY_DIR/cron/stocking"
python -m pipeline.recurring.stocking.resolver \
  --poly-reaches "$DEPLOY_DIR/poly_reaches.json" \
  --out "$DEPLOY_DIR/cron/stocking/stocking.json"
# Legacy dual-write (one release) so the webapp can cut over to cron/ paths
# without an outage — see plan Part F3.
cp "$DEPLOY_DIR/cron/stocking/stocking.json" "$DEPLOY_DIR/stocking.json"

echo "✅ stocking.json → $DEPLOY_DIR/cron/stocking/stocking.json (+ legacy root)"

# ── Step 2: Upload / seed (optional) ────────────────────────────────

if [[ "${1:-}" == "--upload" ]]; then
  echo "── Uploading to R2 ($R2_BUCKET) ──"
  # New canonical key + legacy key (dual-write during cutover).
  for key in "cron/stocking/stocking.json" "stocking.json"; do
    python -m pipeline.recurring.r2_storage put \
      "$DEPLOY_DIR/cron/stocking/stocking.json" "$key"
  done
  echo "✅ Uploaded to R2"

elif [[ "${1:-}" == "--seed" ]]; then
  echo "── Re-seeding local R2 ──"
  node "$SCRIPT_DIR/seed.mjs" --force
  echo "✅ Local R2 refreshed"
fi
