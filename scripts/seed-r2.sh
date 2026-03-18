#!/usr/bin/env bash
# seed-r2.sh — Upload pipeline output to a remote R2 bucket.
#
# Usage:
#   ./scripts/seed-r2.sh                    # upload to staging bucket (default)
#   DEPLOY_ENV=production ./scripts/seed-r2.sh  # upload to production bucket

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
DEPLOY_DIR="$ROOT/output/pipeline/deploy"

DEPLOY_ENV="${DEPLOY_ENV:-staging}"

case "$DEPLOY_ENV" in
  staging)    BUCKET="bc-fishing-regulations-staging" ;;
  production) BUCKET="bc-fishing-regulations" ;;
  *) echo "ERROR: Unknown DEPLOY_ENV=$DEPLOY_ENV" >&2; exit 1 ;;
esac

if [[ ! -d "$DEPLOY_DIR" ]]; then
  echo "ERROR: Deploy dir not found: $DEPLOY_DIR" >&2
  echo "  Run the pipeline first: python -m pipeline --step all" >&2
  exit 1
fi

echo "Uploading to bucket: $BUCKET"
echo "Source: $DEPLOY_DIR"
echo

cd "$DEPLOY_DIR"

# Count files (skip _tile_temp work dir)
FILES=$(find . -type f -not -path './_tile_temp/*' | sort)
TOTAL=$(echo "$FILES" | wc -l)
COUNT=0

for f in $FILES; do
  COUNT=$((COUNT + 1))
  KEY="${f#./}"

  # Set content-type
  CT="application/octet-stream"
  [[ "$KEY" == *.json ]] && CT="application/json"
  [[ "$KEY" == *.pmtiles ]] && CT="application/octet-stream"
  [[ "$KEY" == *.png ]] && CT="image/png"

  printf "[%d/%d] %s\n" "$COUNT" "$TOTAL" "$KEY"
  wrangler r2 object put "$BUCKET/$KEY" --file "$KEY" --content-type "$CT"
done

echo
echo "✅ Uploaded $COUNT files to $BUCKET"
