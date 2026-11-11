#!/bin/bash
# STAGING ONLY: upload a release + feeds + sql.js wasm to bc-fishing-regulations-staging in publish.py's order (tiles,
# basemap, data files, synopsis, then the manifest LAST), then clear what the release does not name (T433), under the
# `release/` prefix the app reads (app/apps/mobile/src/config.ts). Needs the rclone remote `r2:`.  Usage: deploy/staging/upload.sh [r-<id>]
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
# (T483 fast-fail; caught 2026-11-09: r-d14bdb180f55 went up with minAppVersion 0.7.0 while the live staging site was
# 0.6.9, so the smoke test failed only AFTER the upload) the live site must be at least the oldest app this release lets
# apply it — checked before anything is sent; it names the site redeploy. SITE_CHECK_SKIP=<reason> skips it (printed).
MIN_APP_VERSION=${MIN_APP_VERSION-} .venv/bin/python -m pipeline.deliver.release.site_gate "$M"
# (T533) CI on HEAD (app-ci, pipeline-ci) is not red, nor its newest finished run when HEAD has none: before anything is
# sent. CI_CHECK_SKIP=<reason> skips it (printed; `pipeline release --no-ci-check` passes its reason).
.venv/bin/python scripts/ci_status.py
IMM="Cache-Control: public, max-age=31536000, immutable"
cp_() { rclone copyto "$1" "$D/$2" --s3-no-check-bucket "${@:3}"; }
CLIM_PY=python3; [ -x .venv/bin/python ] && CLIM_PY=.venv/bin/python
TILES=$(q "m['tiles']['current']['id']")
# (T412) RUN TIMING HAS ONE PUBLISHER, feeds-runtiming.yml. This release ships the BINDING BASE of its map (`pipeline
# deliver` stages it in data/generated/runtiming/feed/release/); it goes to private/, then the workflow is started for
# these tiles and runs WHILE the rest uploads; the pointer (release/manifest.json) is written last, after it (below).
RT=data/generated/runtiming/feed/release
RT_TV=$(python3 -c "import json;print(json.load(open('$RT/base.json'))['tile_version'])") \
  || { echo "upload: no run-timing base at $RT — run pipeline.deliver all"; exit 1; }
[[ "$RT_TV" == "$TILES" ]] || { echo "upload: the run-timing base is for $RT_TV, the release's tiles are $TILES"; exit 1; }
RT_BASE=$(python3 -c "import json;print(json.load(open('$RT/base.json'))['base'])")
# (T485) NOTHING TO REDO: when staging's run timing already names these tiles and its binding base is byte-identical to
# this release's, the workflow would publish nothing — skip it (it fetches PSF + open.canada.ca, and an outage there
# blocked r-1b4f54fb6ed9 on 2026-11-07). Said out loud; any difference runs the workflow as before.
RT_KEY="private/runtiming/base/${TILES%%.*}.json.gz"
RT_HAVE=$(rclone cat "$D/runtiming/index.json" --s3-no-check-bucket 2>/dev/null \
  | python3 -c "import json,sys;print(json.load(sys.stdin)['tile_version'])" 2>/dev/null || true)
RT_SAME=""
if [[ "$RT_HAVE" == "$TILES" ]] && cmp -s "$RT/$RT_BASE" <(rclone cat "$D/$RT_KEY" --s3-no-check-bucket 2>/dev/null); then
  RT_SAME=1
fi
# the workflow runs the code on GitHub: refuse a local commit it would not see
GH_REPO=HorvathDawson/BC-freshwater-fishing-regulations; GH_REF=redesign/stream-sections
git fetch -q origin "$GH_REF"
RT_RUN=""
if [[ -n "$RT_SAME" ]]; then
  echo "run timing: staging already has it for $TILES with the same binding base — workflow not started (T485)"
else
rclone copyto "$RT/$RT_BASE" "$D/$RT_KEY" --s3-no-check-bucket --checksum
git merge-base --is-ancestor HEAD "origin/$GH_REF" || { echo "upload: push $GH_REF first (the run-timing workflow runs GitHub's copy)"; exit 1; }
RT_T0=$(date -u -v-60S +%Y-%m-%dT%H:%M:%SZ)     # a minute back: the Mac and GitHub clocks differ a little
gh workflow run feeds-runtiming.yml -R "$GH_REPO" --ref "$GH_REF" -f upload=true -f tiles="$TILES" >/dev/null
for _ in $(seq 1 30); do
  RT_RUN=$(gh run list -R "$GH_REPO" --branch "$GH_REF" --event workflow_dispatch --limit 10 --json databaseId,createdAt,workflowName \
    --jq "[.[] | select(.workflowName | startswith(\"feeds — run timing\")) | select(.createdAt >= \"$RT_T0\")][0].databaseId // empty")
  [[ -n "$RT_RUN" ]] && break; sleep 4
done
[[ -n "$RT_RUN" ]] || { echo "upload: the run-timing workflow did not start (gh workflow run)"; exit 1; }
echo "run timing: workflow run $RT_RUN started for $TILES"
fi
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
# (T554 item 4) never an older HYDAT's envelope back over a newer one (the HYDAT workflow may have written it while the
# Mac had not run scripts/update-hydat.sh): pipeline.gauges.feed.clim_guard says put (0) or skip (3, said)
CG=0; "$CLIM_PY" -m pipeline.gauges.feed.clim_guard --local data/generated/gauges/feeds/clim.json --remote "$D/feeds/live/clim.json" || CG=$?
[[ $CG == 0 || $CG == 3 ]] || { echo "upload: clim.json could not be compared with R2's (above); stopped" >&2; exit 1; }
[[ $CG == 0 ]] && rclone copyto data/generated/gauges/feeds/clim.json "$D/feeds/live/clim.json" --s3-no-check-bucket --checksum \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
# (T274) HYDAT daily means per station (pipeline.gauges.feed.hydat_daily; static between HYDAT releases): what dayFlow.ts reads
if [[ -d data/generated/gauges/hydat_daily ]]; then
  rclone copy data/generated/gauges/hydat_daily "$D/feeds/hydat" --s3-no-check-bucket --transfers 32 --include "*.json" \
    --header-upload "Cache-Control: public, max-age=86400" --header-upload "Content-Type: application/json"
fi
rclone copy data/generated/regs/dfo_salmon/feed "$D/feeds/dfo" --s3-no-check-bucket --include "*.json" --ignore-existing \
  --exclude "feed.notified.json" \
  --header-upload "Cache-Control: public, max-age=300" --header-upload "Content-Type: application/json"
for w in app/node_modules/sql.js/dist/sql-wasm-browser.wasm app/node_modules/sql.js/dist/sql-wasm.wasm; do
  cp_ "$w" "$(basename "$w")" --header-upload "Content-Type: application/wasm" --header-upload "Cache-Control: public, max-age=86400"
done
# (T412) ATOMIC SWITCH: the release pointer flips only once run timing for THIS release's tiles is on R2 (the workflow
# started above publishes it; same tiles as the live feed and nothing new: it publishes nothing, the index already
# names them). A failed run stops here without writing the pointer; fix it and rerun this script.
if [[ -n "$RT_RUN" ]]; then
  echo "run timing: waiting for workflow run $RT_RUN"
  gh run watch "$RT_RUN" -R "$GH_REPO" --exit-status --interval 20 >/dev/null \
    || { echo "upload: the run-timing workflow failed: https://github.com/$GH_REPO/actions/runs/$RT_RUN — the release pointer was NOT written"; exit 1; }
fi
RT_LIVE=$(rclone cat "$D/runtiming/index.json" --s3-no-check-bucket | python3 -c "import json,sys;print(json.load(sys.stdin)['tile_version'])")
[[ "$RT_LIVE" == "$TILES" ]] || { echo "upload: runtiming/index.json names $RT_LIVE, the release's tiles are $TILES — pointer NOT written"; exit 1; }
# (T537) THE POINTER-COUPLED GROUP goes up LAST, in order: run timing (above, the workflow done) → the worker's inputs
# and the in-season links (sync-inputs.sh; it only VERIFIES the matches name this release — `pipeline release` resolved
# them before its promote) → the pointer. A failure anywhere before leaves the old pointer, inputs and links live together.
deploy/staging/sync-inputs.sh
# (T251) the publish day (kept once set) and, with MIN_APP_VERSION=x.y.z, the oldest app that may apply it: stamped
# into the manifest here, never by the build; neither moves a file, the tiles or the release id (nothing re-uploaded)
.venv/bin/python -m pipeline.deliver.release.stamp "$M" ${MIN_APP_VERSION+--min-app-version "$MIN_APP_VERSION"}
cp_ "$M" release/manifest.json --header-upload "Cache-Control: no-cache" --header-upload "Content-Type: application/json"
# (T433) THE CLEAR COMES AFTER THE POINTER: everything this release does not name goes now, so a failure anywhere above
# leaves the old manifest + release live. Spared: (T199/T412) runtiming/ (feeds-runtiming.yml, its only publisher),
# (T251) test/ (the update-test channel), feeds/ + private/ (the worker's), app/android/, and this release's own keys.
echo "clear $D of what $REL does not name (except feeds/, private/, runtiming/, test/, app/android/)"
rclone delete "$D" --s3-no-check-bucket --exclude "feeds/**" --exclude "private/**" --exclude "runtiming/**" --exclude "test/**" --exclude "app/android/**" \
  --exclude "release/manifest.json" --exclude "release/tiles/$TILES/**" --exclude "release/$REL/**" --exclude "release/$BM" \
  --exclude "release/$SYN" --exclude "map-assets/v1/**" --exclude "/*.wasm"
# (T442) test/ is spared, but its manifest names the release just cleared: pointed at this one, or a test-channel
# build's fresh install 404s on every file ("The map data could not be loaded"; walk-063)
# (T537) a failed reset STOPS (it was a WARNING: a fresh test-channel install then 404s — no silent fallbacks)
node deploy/staging/test-updates.mjs reset >/dev/null || { echo "upload: test/ was NOT reset — run: node deploy/staging/test-updates.mjs reset" >&2; exit 1; }
echo "uploaded $REL to $D"; rclone size "$D"
# (T241) the smoke test against the live staging site + this bucket; SMOKE=0 skips it
if [[ "${SMOKE:-1}" != 0 ]]; then node app/tools/smoke-staging.mjs --shots "${SMOKE_SHOTS:-app/dist/smoke-staging}"; fi
