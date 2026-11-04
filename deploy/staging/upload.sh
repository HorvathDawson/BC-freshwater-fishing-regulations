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
# (guard, 2026-10-30) an unstamped paint/5 release reached staging and broke 0.2.0, which reads paint/4 at most: refuse a
# release whose paint format needs a newer app than the oldest the manifest lets apply it. PAINT_NEEDS: format -> first
# app that reads it (add a row whenever paint.py's FORMAT moves); SALMON_NEEDS the same for salmon.py's FORMAT. MIN_APP_VERSION (or an existing stamp) must reach it.
MIN_APP_VERSION=${MIN_APP_VERSION-} python3 - "$M" <<'PY'
import json, os, sys
PAINT_NEEDS = {5: "0.3.0"}
# (T332/T349) salmon/3 = salmon quotas in the game-fish quota tree; (T356) salmon/4 = header closure, shared limits and sizes
# resolved in the file. No shipped app reads either yet (0.4.0 reads salmon/2); 0.5.0 reads salmon/4 only
SALMON_NEEDS = {3: "0.5.0", 4: "0.5.0"}
m = json.load(open(sys.argv[1]))
have = os.environ.get("MIN_APP_VERSION") or m.get("minAppVersion")
ver = lambda s: tuple(int(x) for x in s.split("-")[0].split("+")[0].split("."))
for key, needs in (("paint", PAINT_NEEDS), ("salmon", SALMON_NEEDS)):
    f = m["files"].get(key)
    if not f:
        continue
    fmt = int(f["format"].split("/")[1])
    need = max((v for k, v in needs.items() if fmt >= k), default=None, key=ver)
    if need and (not have or ver(have) < ver(need)):
        sys.exit(f"upload: refused — {key}/{fmt} needs app >= {need} but the oldest app allowed is {have or 'any'}; "
                 f"rerun with MIN_APP_VERSION={need} (or newer)")
PY
IMM="Cache-Control: public, max-age=31536000, immutable"
cp_() { rclone copyto "$1" "$D/$2" --s3-no-check-bucket "${@:3}"; }
# (T199) runtiming/ is the run-timing feed's (feeds-runtiming.yml / pipeline.runtiming.feed.publish): never cleared here
# (T251) test/ is the staging update-test channel (deploy/staging/test-updates.mjs): never cleared here either
echo "clear $D (except feeds/, private/, runtiming/, test/, app/android/)"
rclone delete "$D" --s3-no-check-bucket --exclude "feeds/**" --exclude "private/**" --exclude "runtiming/**" --exclude "test/**" --exclude "app/android/**"
TILES=$(q "m['tiles']['current']['id']")
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
# (T251) the publish day (kept once set) and, with MIN_APP_VERSION=x.y.z, the oldest app that may apply it: stamped
# into the manifest here, never by the build; neither moves a file, the tiles or the release id (nothing re-uploaded)
.venv/bin/python -m pipeline.deliver.release.stamp "$M" ${MIN_APP_VERSION+--min-app-version "$MIN_APP_VERSION"}
cp_ "$M" release/manifest.json --header-upload "Cache-Control: no-cache" --header-upload "Content-Type: application/json"
echo "uploaded $REL to $D"; rclone size "$D"
# (T241) the smoke test against the live staging site + this bucket; SMOKE=0 skips it
if [[ "${SMOKE:-1}" != 0 ]]; then node app/tools/smoke-staging.mjs --shots "${SMOKE_SHOTS:-app/dist/smoke-staging}"; fi
