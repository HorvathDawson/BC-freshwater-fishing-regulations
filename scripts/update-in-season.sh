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
# In CI there's no pipeline output — pull tier0 + match_table from R2.
# Use wrangler (direct R2 API) when available to avoid Cloudflare bot
# protection blocking GHA runner IPs on the public worker URL.

_fetch_r2_file() {
  local r2_key="$1" dest="$2"
  if command -v wrangler &>/dev/null && [[ -n "${CLOUDFLARE_ACCOUNT_ID:-}" ]]; then
    npx wrangler r2 object get "$R2_BUCKET/$r2_key" --file "$dest" --remote
  else
    curl -sfSL "$R2_ORIGIN/$r2_key" -o "$dest"
  fi
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
python -m pipeline.matching.in_season_scraper \
  --match-table "$DEPLOY_DIR/match_table.json" \
  --quiet

# ── Step 2: Resolve ─────────────────────────────────────────────────

echo "── Resolving to reach IDs ──"
python -m pipeline.matching.in_season_resolver \
  --tier0 "$DEPLOY_DIR/tier0.json" \
  --match-table "$DEPLOY_DIR/match_table.json" \
  --quiet

echo "✅ in_season.json → $DEPLOY_DIR/in_season.json"

# ── Step 3: Upload / seed (optional) ────────────────────────────────

if [[ "${1:-}" == "--upload" ]]; then
  echo "── Uploading to R2 ($R2_BUCKET) ──"
  npx wrangler r2 object put "$R2_BUCKET/in_season.json" \
    --file "$DEPLOY_DIR/in_season.json" \
    --content-type "application/json" \
    --remote
  echo "✅ Uploaded to R2"

elif [[ "${1:-}" == "--seed" ]]; then
  echo "── Re-seeding local R2 ──"
  node "$SCRIPT_DIR/seed.mjs" --force
  echo "✅ Local R2 refreshed"
fi
