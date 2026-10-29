#!/bin/bash
# STAGING ONLY (T225 Phase A, owner 2026-10-28): put the feeds Worker's curated inputs and the app's in-season links into
# bc-fishing-regulations-staging, writing only what differs (rclone --checksum). The Mac twin of
# .github/workflows/feeds-sync-inputs.yml (same keys); upload.sh calls it after a release upload.
#   private/feeds/dfo/entries/region-<s>.json  <- data/curated/regulations/entries/dfo_salmon/region-<s>.json
#   private/feeds/gauges/roster.json           <- data/source/bc_hydrometric_stations.json, slim [{station, realtime}]
#   feeds/inseason/matches.json   (PUBLIC)     <- python -m pipeline.regs.inseason.publish (data/curated/inseason/matches.json)
# Needs the rclone remote `r2:` and the repo's .venv (or python3 with pydantic + pyyaml).
#   deploy/staging/sync-inputs.sh [--dry-run]
set -euo pipefail
cd "$(dirname "$0")/../.."
BUCKET=bc-fishing-regulations-staging            # hardcoded: this script must never name a production bucket
D="r2:$BUCKET"
DRY=(); [ "${1:-}" = "--dry-run" ] && DRY=(--dry-run)
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
mkdir -p "$T/entries" "$T/inseason"
cp data/curated/regulations/entries/dfo_salmon/region-*.json "$T/entries/"
jq -c '[.[] | {station, realtime}]' data/source/bc_hydrometric_stations.json > "$T/roster.json"
"$PY" -m pipeline.regs.inseason.publish --out "$T/inseason" >/dev/null
JSON=(--s3-no-check-bucket --checksum --header-upload "Content-Type: application/json" ${DRY[@]+"${DRY[@]}"})
rclone copy "$T/entries" "$D/private/feeds/dfo/entries" "${JSON[@]}" -v 2>&1 | grep -E "Copied|Skipped|ERROR" || true
rclone copyto "$T/roster.json" "$D/private/feeds/gauges/roster.json" "${JSON[@]}" -v 2>&1 | grep -E "Copied|Skipped|ERROR" || true
rclone copyto "$T/inseason/matches.json" "$D/feeds/inseason/matches.json" "${JSON[@]}" \
  --header-upload "Cache-Control: public, max-age=300, stale-while-revalidate=3600" -v 2>&1 | grep -E "Copied|Skipped|ERROR" || true
echo "inputs in sync: $D (entries $(ls "$T/entries" | wc -l | tr -d ' '), roster, in-season matches $(jq '.matches|length' "$T/inseason/matches.json"))"
