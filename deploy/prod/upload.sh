#!/bin/bash
# PRODUCTION — THE OWNER RUNS THIS (T404; agents never touch prod). PROMOTE the release STAGING serves to the production
# bucket bc-fishing-regulations (data.canifishthis.ca): the same release id, the same stamped manifest, the same tiles.
#   CONFIRM_PROD=canifishthis.ca deploy/prod/upload.sh [r-<id>] [--dry-run]
#   deploy/prod/upload.sh [r-<id>] --bucket <a non-production bucket> [--dry-run]   (a rehearsal: no confirmation needed)
# Refuses unless staging's live release/manifest.json is byte-identical to data/release/<id>/manifest.json (the release
# the owner checked on staging, stamped by deploy/staging/upload.sh). Order as staging's (publish.py's): run timing,
# tiles, map assets, basemap, data files, synopsis, feed seeds, inputs, sql.js — then the pointer LAST. Unlike staging:
#   - run timing is COPIED from staging (same tiles = same output; feeds-runtiming.yml stays staging's publisher until
#     the owner enables its production option);
#   - nothing is cleared: the previous release stays whole, and its pointer is kept as release/manifest.previous.json,
#     so deploy/prod/rollback.sh is one copy back (old releases are pruned by hand, PRODUCTION.md);
#   - no test/ channel, no smoke run here: run `node app/tools/smoke-staging.mjs --production` after.
set -euo pipefail
cd "$(dirname "$0")/../.."
PROD_BUCKET=bc-fishing-regulations
STAGING=r2:bc-fishing-regulations-staging
BUCKET=$PROD_BUCKET; DRY=(); REL=
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run) DRY=(--dry-run) ;;
    --bucket) BUCKET=${2:?--bucket needs a name}; shift ;;
    r-*) REL=$1 ;;
    *) echo "upload (prod): unknown argument $1" >&2; exit 2 ;;
  esac; shift
done
if [[ $BUCKET == "$PROD_BUCKET" && "${CONFIRM_PROD:-}" != canifishthis.ca ]]; then
  echo "upload (prod): set CONFIRM_PROD=canifishthis.ca to write $PROD_BUCKET (or --bucket <other> to rehearse)" >&2; exit 2
fi
[[ "r2:$BUCKET" != "$STAGING" || ${#DRY[@]} -gt 0 ]] || { echo "upload (prod): --bucket staging is only for a --dry-run" >&2; exit 2; }
D="r2:$BUCKET"
REL=${REL:-$(cat data/release/LATEST)}
R=data/release; M=$R/$REL/manifest.json
[[ -f $M ]] || { echo "upload (prod): no $M" >&2; exit 1; }
say() { echo "== $*${DRY[*]:+ (dry run)}"; }
q() { python3 -c "import json,sys;m=json.load(open('$M'));print(eval(sys.argv[1]))" "$1"; }

# 1. THE SAME RELEASE STAGING SERVES (and was checked on): byte-identical manifests, or stop
LIVE=$(mktemp); trap 'rm -f "$LIVE"' EXIT
rclone cat "$STAGING/release/manifest.json" --s3-no-check-bucket > "$LIVE"
cmp -s "$M" "$LIVE" || { echo "upload (prod): staging's live manifest is not $M ($(q "m['data_release']")) — upload it to staging" \
  "(deploy/staging/upload.sh $REL), check it there, then promote" >&2; exit 1; }
MIN_APP_VERSION= .venv/bin/python -m pipeline.deliver.release.app_gate "$M"
say "promote $REL to $D (staging serves the same manifest)"
IMM="Cache-Control: public, max-age=31536000, immutable"
cp_() { rclone copyto "$1" "$D/$2" --s3-no-check-bucket ${DRY[@]+"${DRY[@]}"} "${@:3}"; }
cpr() { rclone copy "$1" "$2" --s3-no-check-bucket ${DRY[@]+"${DRY[@]}"} "${@:3}"; }
CLIM_PY=python3; [ -x .venv/bin/python ] && CLIM_PY=.venv/bin/python
TILES=$(q "m['tiles']['current']['id']"); ASSETS=$(q "m['tiles']['assets']['url']")
BM=$(q "m['basemap']['url']"); SYN=$(q "m['synopsis']['url']")

# 2. run timing: staging's, for THIS release's tiles
RT_TV=$(rclone cat "$STAGING/runtiming/index.json" --s3-no-check-bucket | python3 -c "import json,sys;print(json.load(sys.stdin)['tile_version'])")
[[ "$RT_TV" == "$TILES" ]] || { echo "upload (prod): staging's run timing is for $RT_TV, the release's tiles are $TILES" >&2; exit 1; }
say "run timing (server-side copy from staging)"
cpr "$STAGING/runtiming" "$D/runtiming" --transfers 16
cpr "$STAGING/private/runtiming/base" "$D/private/runtiming/base" --include "${TILES%%.*}.json.gz"

# 3. the release's files, with staging's headers (deploy/staging/upload.sh)
say "tiles $TILES, map assets, basemap, data files, synopsis"
cpr "$R/tiles/$TILES" "$D/release/tiles/$TILES" --transfers 16 --header-upload "$IMM" --exclude "*.pbf"
cpr "$R/tiles/$TILES" "$D/release/tiles/$TILES" --transfers 16 --header-upload "$IMM" --include "*.pbf" \
  --header-upload "Content-Type: application/x-protobuf"
cpr "$R/$ASSETS" "$D/map-assets/v1" --transfers 16 --header-upload "$IMM" --exclude "assets.json" --exclude "*.pbf"
cpr "$R/$ASSETS" "$D/map-assets/v1" --transfers 16 --header-upload "$IMM" --include "*.pbf" \
  --header-upload "Content-Type: application/x-protobuf"
cp_ "$R/$BM" "release/$BM" --header-upload "$IMM"
cpr "$R/$REL" "$D/release/$REL" --transfers 16 --include "*.gz" --header-upload "$IMM" \
  --header-upload "Content-Encoding: gzip" --header-upload "Content-Type: application/octet-stream"
cp_ "$R/$SYN" "release/$SYN" --header-upload "$IMM" --header-upload "Content-Type: application/pdf"

# 4. feed SEEDS (the prod feeds worker owns feeds/** + private/**: a seed never overwrites its newer output), clim, HYDAT
say "feed seeds, clim.json, HYDAT day means, DFO seeds"
cpr data/generated/gauges/feeds "$D/feeds/live" --transfers 32 --include "*.json" --exclude clim.json \
  --exclude temp_history.json --ignore-existing \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
# (T554 item 4) never an older HYDAT's envelope back over a newer one (the HYDAT workflow may have written it while the
# Mac had not run scripts/update-hydat.sh): pipeline.gauges.feed.clim_guard says put (0) or skip (3, said)
CG=0; "$CLIM_PY" -m pipeline.gauges.feed.clim_guard --local data/generated/gauges/feeds/clim.json --remote "$D/feeds/live/clim.json" || CG=$?
[[ $CG == 0 || $CG == 3 ]] || { echo "upload: clim.json could not be compared with R2's (above); stopped" >&2; exit 1; }
[[ $CG == 0 ]] && cp_ data/generated/gauges/feeds/clim.json feeds/live/clim.json --checksum \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
[[ -d data/generated/gauges/hydat_daily ]] || { echo "upload (prod): no data/generated/gauges/hydat_daily (scripts/update-hydat.sh)" >&2; exit 1; }
cpr data/generated/gauges/hydat_daily "$D/feeds/hydat" --transfers 32 --include "*.json" \
  --header-upload "Cache-Control: public, max-age=86400" --header-upload "Content-Type: application/json"
cpr data/generated/regs/dfo_salmon/feed "$D/feeds/dfo" --include "*.json" --ignore-existing --exclude "feed.notified.json" \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
CONFIRM_PROD=${CONFIRM_PROD:-} deploy/prod/sync-inputs.sh --bucket "$BUCKET" ${DRY[@]+"${DRY[@]}"}
for w in app/node_modules/sql.js/dist/sql-wasm-browser.wasm app/node_modules/sql.js/dist/sql-wasm.wasm; do
  cp_ "$w" "$(basename "$w")" --header-upload "Content-Type: application/wasm" --header-upload "Cache-Control: public, max-age=86400"
done

# 5. THE POINTER, LAST — the one it replaces is kept for deploy/prod/rollback.sh
# an object: 0; none: 1 (R2 answers a missing key as an empty "directory", rc 0); rclone failed: 2
obj() { local o; o=$(rclone lsjson --stat "$1" --s3-no-check-bucket) || return 2; [[ $o == *'"IsDir": false'* ]] || return 1; }
set +e; obj "$D/release/manifest.json"; rc=$?; set -e
case $rc in
  0) say "keep the current pointer as release/manifest.previous.json"
     rclone copyto "$D/release/manifest.json" "$D/release/manifest.previous.json" --s3-no-check-bucket ${DRY[@]+"${DRY[@]}"} ;;
  1) echo "== no current pointer in $D (the first promote): nothing to keep for a rollback" ;;
  *) echo "upload (prod): could not read $D/release/manifest.json (rclone exit $rc) — the pointer was NOT written" >&2; exit 1 ;;
esac
say "pointer: release/manifest.json -> $REL"
cp_ "$M" release/manifest.json --header-upload "Cache-Control: no-cache" --header-upload "Content-Type: application/json"
# (T72) what production serves: the local prune never removes it (pipeline/deliver/release/prune.py)
[[ ${#DRY[@]} -gt 0 || $BUCKET != "$PROD_BUCKET" ]] || echo "$REL" > "$R/PUBLISHED"
echo "promoted $REL to $D${DRY[*]:+ (dry run: nothing was written)}. Next: node app/tools/smoke-staging.mjs --production"
