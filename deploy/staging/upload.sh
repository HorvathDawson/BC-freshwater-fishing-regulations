#!/bin/bash
# STAGING ONLY: clear bc-fishing-regulations-staging and upload a release + feeds + sql.js wasm, in publish.py's order
# (tiles, basemap, data files, synopsis, then the manifest LAST), under the `release/` prefix the app reads
# (app/apps/mobile/src/config.ts). Needs the rclone remote `r2:`.  Usage: deploy/staging/upload.sh [r-<id>]
# T225 Phase A: the feeds Worker runs on a schedule and OWNS feeds/** and private/** — the clear spares them; the local
# feed copies only SEED a key that is missing (never overwrite the worker's newer output); clim.json (the worker's input)
# goes up when it differs; then sync-inputs.sh (the list in pipeline.deliver.sync_inputs: DFO entries + wording, roster, in-season matches).
set -euo pipefail
cd "$(dirname "$0")/../.."
BUCKET=bc-fishing-regulations-staging            # hardcoded: this script must never name a production bucket
D="r2:$BUCKET"
REL=${1:-$(cat data/release/LATEST)}
R=data/release; M=$R/$REL/manifest.json
q() { python3 -c "import json,sys;m=json.load(open('$M'));print(eval(sys.argv[1]))" "$1"; }
# (guard, 2026-10-30) refuse a release whose paint/salmon format needs a newer app than the oldest the manifest lets apply
# it (MIN_APP_VERSION, or the manifest's stamp). The table and the rule: pipeline/deliver/release/app_gate.py (one copy:
# `pipeline release` runs the same check before it promotes).
MIN_APP_VERSION=${MIN_APP_VERSION-} .venv/bin/python -m pipeline.deliver.release.app_gate "$M"
IMM="Cache-Control: public, max-age=31536000, immutable"
cp_() { rclone copyto "$1" "$D/$2" --s3-no-check-bucket "${@:3}"; }
TILES=$(q "m['tiles']['current']['id']")
# (T412) RUN TIMING HAS ONE PUBLISHER, feeds-runtiming.yml. This release ships the BINDING BASE of its map (`pipeline
# deliver` stages it in data/generated/runtiming/feed/release/); it goes to private/, then the workflow is started for
# these tiles and runs WHILE the rest uploads; the pointer (release/manifest.json) is written last, after it (below).
RT=data/generated/runtiming/feed/release
RT_TV=$(python3 -c "import json;print(json.load(open('$RT/base.json'))['tile_version'])") \
  || { echo "upload: no run-timing base at $RT — run pipeline.deliver all"; exit 1; }
[[ "$RT_TV" == "$TILES" ]] || { echo "upload: the run-timing base is for $RT_TV, the release's tiles are $TILES"; exit 1; }
RT_BASE=$(python3 -c "import json;print(json.load(open('$RT/base.json'))['base'])")
rclone copyto "$RT/$RT_BASE" "$D/private/runtiming/base/${TILES%%.*}.json.gz" --s3-no-check-bucket --checksum
# the workflow runs the code on GitHub: refuse a local commit it would not see
GH_REPO=HorvathDawson/BC-freshwater-fishing-regulations; GH_REF=redesign/stream-sections
git fetch -q origin "$GH_REF"
git merge-base --is-ancestor HEAD "origin/$GH_REF" || { echo "upload: push $GH_REF first (the run-timing workflow runs GitHub's copy)"; exit 1; }
# (T199/T412) runtiming/ is the run-timing feed's (feeds-runtiming.yml, its only publisher): never cleared here
# (T251) test/ is the staging update-test channel (deploy/staging/test-updates.mjs): never cleared here either
echo "clear $D (except feeds/, private/, runtiming/, test/, app/android/)"
rclone delete "$D" --s3-no-check-bucket --exclude "feeds/**" --exclude "private/**" --exclude "runtiming/**" --exclude "test/**" --exclude "app/android/**"
RT_T0=$(date -u -v-60S +%Y-%m-%dT%H:%M:%SZ)     # a minute back: the Mac and GitHub clocks differ a little
gh workflow run feeds-runtiming.yml -R "$GH_REPO" --ref "$GH_REF" -f upload=true -f tiles="$TILES" >/dev/null
RT_RUN=""
for _ in $(seq 1 30); do
  RT_RUN=$(gh run list -R "$GH_REPO" --branch "$GH_REF" --event workflow_dispatch --limit 10 --json databaseId,createdAt,workflowName \
    --jq "[.[] | select(.workflowName | startswith(\"feeds — run timing\")) | select(.createdAt >= \"$RT_T0\")][0].databaseId // empty")
  [[ -n "$RT_RUN" ]] && break; sleep 4
done
[[ -n "$RT_RUN" ]] || { echo "upload: the run-timing workflow did not start (gh workflow run)"; exit 1; }
echo "run timing: workflow run $RT_RUN started for $TILES"
rclone copy "$R/tiles/$TILES" "$D/release/tiles/$TILES" --s3-no-check-bucket --transfers 16 --header-upload "$IMM" \
  --exclude "*.pbf"
rclone copy "$R/tiles/$TILES" "$D/release/tiles/$TILES" --s3-no-check-bucket --transfers 16 --header-upload "$IMM" \
  --include "*.pbf" --header-upload "Content-Type: application/x-protobuf"
# (T270) the map's fonts + sprites at the fixed path the app falls back to (data host /map-assets/v1/), from the release's own folder
rclone copy "$R/$(q "m['tiles']['assets']['url']")" "$D/map-assets/v1" --s3-no-check-bucket --transfers 16 --header-upload "$IMM" \
  --exclude "assets.json" --exclude "*.pbf"
rclone copy "$R/$(q "m['tiles']['assets']['url']")" "$D/map-assets/v1" --s3-no-check-bucket --transfers 16 --header-upload "$IMM" \
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
# (T274) HYDAT daily means per station (pipeline.gauges.feed.hydat_daily; static between HYDAT releases): what dayFlow.ts reads
if [[ -d data/generated/gauges/hydat_daily ]]; then
  rclone copy data/generated/gauges/hydat_daily "$D/feeds/hydat" --s3-no-check-bucket --transfers 32 --include "*.json" \
    --header-upload "Cache-Control: public, max-age=86400" --header-upload "Content-Type: application/json"
fi
rclone copy data/generated/regs/dfo_salmon/feed "$D/feeds/dfo" --s3-no-check-bucket --include "*.json" --ignore-existing \
  --exclude "feed.notified.json" \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
deploy/staging/sync-inputs.sh
for w in app/node_modules/sql.js/dist/sql-wasm-browser.wasm app/node_modules/sql.js/dist/sql-wasm.wasm; do
  cp_ "$w" "$(basename "$w")" --header-upload "Content-Type: application/wasm" --header-upload "Cache-Control: public, max-age=86400"
done
# (T412) ATOMIC SWITCH: the release pointer flips only once run timing for THIS release's tiles is on R2 (the workflow
# started above publishes it; same tiles as the live feed and nothing new: it publishes nothing, the index already
# names them). A failed run stops here without writing the pointer; fix it and rerun this script.
echo "run timing: waiting for workflow run $RT_RUN"
gh run watch "$RT_RUN" -R "$GH_REPO" --exit-status --interval 20 >/dev/null \
  || { echo "upload: the run-timing workflow failed: https://github.com/$GH_REPO/actions/runs/$RT_RUN — the release pointer was NOT written"; exit 1; }
RT_LIVE=$(rclone cat "$D/runtiming/index.json" --s3-no-check-bucket | python3 -c "import json,sys;print(json.load(sys.stdin)['tile_version'])")
[[ "$RT_LIVE" == "$TILES" ]] || { echo "upload: runtiming/index.json names $RT_LIVE, the release's tiles are $TILES — pointer NOT written"; exit 1; }
# (T251) the publish day (kept once set) and, with MIN_APP_VERSION=x.y.z, the oldest app that may apply it: stamped
# into the manifest here, never by the build; neither moves a file, the tiles or the release id (nothing re-uploaded)
.venv/bin/python -m pipeline.deliver.release.stamp "$M" ${MIN_APP_VERSION+--min-app-version "$MIN_APP_VERSION"}
cp_ "$M" release/manifest.json --header-upload "Cache-Control: no-cache" --header-upload "Content-Type: application/json"
echo "uploaded $REL to $D"; rclone size "$D"
# (T241) the smoke test against the live staging site + this bucket; SMOKE=0 skips it
if [[ "${SMOKE:-1}" != 0 ]]; then node app/tools/smoke-staging.mjs --shots "${SMOKE_SHOTS:-app/dist/smoke-staging}"; fi
