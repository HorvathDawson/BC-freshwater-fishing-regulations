#!/bin/bash
# PRODUCTION — run AT THE SWITCH, on the owner's word (T865; owner 2026-11-18 ~18:25: "when we flip the switch i will get
# you to clean that stuff up too"), the same moment the old bc-fishing-cron worker and main's update-hydro /
# update-in-season / update-stocking workflows are turned off (PRODUCTION.md step 11). DROP THE V1 LAYOUT (the old Vite
# site's data: cron/**, shards/**, mobile/**, root tier0.json, *.pmtiles, ...; the list is V1_FILES / V1_DIRS in
# pipeline/deliver/release/prod_prune.py, read from `main`) from bc-fishing-regulations. Never automatic: once it is gone
# a site rollback to the Vite build loads with no data.
#   CONFIRM_PROD=canifishthis.ca deploy/prod/drop-v1.sh [--dry-run]      (the dry run reads production too: same guard)
#   deploy/prod/drop-v1.sh --bucket <a non-production bucket> [--dry-run]   (a rehearsal; SITE_URL picks the site checked)
# FAILS CLOSED: nothing is deleted unless https://canifishthis.ca/ serves the new web export (its index.html loads
# /_expo/static/js/web/index-<hash>.js and lacks the old `<!-- build: webapp UI fixes` comment); otherwise it says why
# and exits 1. Prints every key once deleted and the bytes freed.
set -euo pipefail
cd "$(dirname "$0")/../.."
source deploy/lib/bucket.sh
PROD_BUCKET=bc-fishing-regulations
BUCKET=$PROD_BUCKET; DRY=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run) DRY=(--dry-run) ;;
    --bucket) BUCKET=$(bucket_name "${2:?--bucket needs a name}") || exit 2; shift ;;
    *) echo "drop-v1 (prod): unknown argument $1" >&2; exit 2 ;;
  esac; shift
done
if [[ $BUCKET == "$PROD_BUCKET" && "${CONFIRM_PROD:-}" != canifishthis.ca ]]; then
  echo "drop-v1 (prod): set CONFIRM_PROD=canifishthis.ca to drop the v1 layout from $PROD_BUCKET (or --bucket <other> to rehearse)" >&2; exit 2
fi
if [[ $BUCKET == "$PROD_BUCKET" ]]; then SITE=https://canifishthis.ca/; else SITE=${SITE_URL:-https://canifishthis.ca/}; fi
.venv/bin/python -m pipeline.deliver.release.prod_prune v1 --remote "r2:$BUCKET" --site "$SITE" ${DRY[@]+"${DRY[@]}"}
