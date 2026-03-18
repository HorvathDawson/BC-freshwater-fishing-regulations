#!/usr/bin/env bash
# update-in-season.sh — Scrape + resolve in-season regulation changes.
#
# Shared between local dev and GitHub Actions so the pipeline never diverges.
#
# Usage:
#   ./scripts/update-in-season.sh              # scrape + resolve (local)
#   ./scripts/update-in-season.sh --seed        # also re-seed local R2
#   ./scripts/update-in-season.sh --upload      # fetch data from R2, resolve, upload (CI)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$ROOT/output/pipeline/deploy"
MATCHING_DIR="$ROOT/output/pipeline/matching"

R2_ORIGIN="${R2_ORIGIN:-https://bc-fishing-r2.horvath-dawson.workers.dev}"

mkdir -p "$DEPLOY_DIR" "$MATCHING_DIR"

# ── Step 0: Fetch data files from R2 if not present locally ─────────
# In CI there's no pipeline output — pull tier0 + match_table from prod R2.

if [[ ! -f "$DEPLOY_DIR/tier0.json" ]]; then
  echo "── Fetching tier0.json from R2 ──"
  curl -sfSL "$R2_ORIGIN/tier0.json" -o "$DEPLOY_DIR/tier0.json"
fi

if [[ ! -f "$DEPLOY_DIR/match_table.json" ]]; then
  echo "── Fetching match_table.json from R2 ──"
  curl -sfSL "$R2_ORIGIN/match_table.json" -o "$DEPLOY_DIR/match_table.json"
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
  echo "── Uploading to R2 ──"
  npx wrangler r2 object put "bc-fishing-regulations/in_season.json" \
    --file "$DEPLOY_DIR/in_season.json" \
    --content-type "application/json"
  echo "✅ Uploaded to R2"

elif [[ "${1:-}" == "--seed" ]]; then
  echo "── Re-seeding local R2 ──"
  node "$SCRIPT_DIR/seed.mjs" --force
  echo "✅ Local R2 refreshed"
fi
