#!/bin/bash
# STAGING ONLY: clear bc-fishing-regulations-staging and upload a release + feeds + sql.js wasm, in publish.py's order
# (tiles, basemap, data files, synopsis, then the manifest LAST), under the `release/` prefix the app reads
# (app/apps/mobile/src/config.ts). Needs the rclone remote `r2:`.  Usage: deploy/staging/upload.sh [r-<id>]
# T225 Phase A: the feeds Worker runs on a schedule and OWNS feeds/** and private/** — the clear spares them; the local
# feed copies only SEED a key that is missing (never overwrite the worker's newer output); clim.json (the worker's input)
# goes up when it differs; then sync-inputs.sh (DFO entries, roster, in-season matches).
set -euo pipefail
cd "$(dirname "$0")/../.."
BUCKET=bc-fishing-regulations-staging            # hardcoded: this script must never name a production bucket
D="r2:$BUCKET"
REL=${1:-$(cat data/release/LATEST)}
R=data/release; M=$R/$REL/manifest.json
q() { python3 -c "import json,sys;m=json.load(open('$M'));print(eval(sys.argv[1]))" "$1"; }
IMM="Cache-Control: public, max-age=31536000, immutable"
cp_() { rclone copyto "$1" "$D/$2" --s3-no-check-bucket "${@:3}"; }
echo "clear $D (except feeds/, private/)"; rclone delete "$D" --s3-no-check-bucket --exclude "feeds/**" --exclude "private/**"
TILES=$(q "m['tiles']['current']['id']")
rclone copy "$R/tiles/$TILES" "$D/release/tiles/$TILES" --s3-no-check-bucket --transfers 16 --header-upload "$IMM" \
  --exclude "*.pbf"
rclone copy "$R/tiles/$TILES" "$D/release/tiles/$TILES" --s3-no-check-bucket --transfers 16 --header-upload "$IMM" \
  --include "*.pbf" --header-upload "Content-Type: application/x-protobuf"
BM=$(q "m['basemap']['url']"); cp_ "$R/$BM" "release/$BM" --header-upload "$IMM"
rclone copy "$R/$REL" "$D/release/$REL" --s3-no-check-bucket --transfers 16 --include "*.gz" --header-upload "$IMM" \
  --header-upload "Content-Encoding: gzip" --header-upload "Content-Type: application/octet-stream"
SYN=$(q "m['synopsis']['url']"); cp_ "$R/$SYN" "release/$SYN" --header-upload "$IMM" --header-upload "Content-Type: application/pdf"
rclone copy data/generated/gauges/feeds "$D/feeds/live" --s3-no-check-bucket --transfers 32 --include "*.json" \
  --exclude clim.json --exclude temp_history.json --ignore-existing \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
rclone copyto data/generated/gauges/feeds/clim.json "$D/feeds/live/clim.json" --s3-no-check-bucket --checksum \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
rclone copy data/generated/regs/dfo_salmon/feed "$D/feeds/dfo" --s3-no-check-bucket --include "*.json" --ignore-existing \
  --exclude "feed.notified.json" \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
deploy/staging/sync-inputs.sh
for w in app/node_modules/sql.js/dist/sql-wasm-browser.wasm app/node_modules/sql.js/dist/sql-wasm.wasm; do
  cp_ "$w" "$(basename "$w")" --header-upload "Content-Type: application/wasm" --header-upload "Cache-Control: public, max-age=86400"
done
cp_ "$M" release/manifest.json --header-upload "Cache-Control: no-cache" --header-upload "Content-Type: application/json"
echo "uploaded $REL to $D"; rclone size "$D"
