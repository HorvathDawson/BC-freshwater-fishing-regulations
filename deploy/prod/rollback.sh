#!/bin/bash
# PRODUCTION — THE OWNER RUNS THIS (T404). ROLL THE DATA BACK one release: release/manifest.previous.json (kept by
# deploy/prod/upload.sh) becomes release/manifest.json again. Every file it names is still in the bucket (a promote
# clears nothing). Apps pick it up on their next check; a phone that already installed the newer release keeps it until
# then (the manifest decides, T231). Then set data/release/LATEST back one line on the Mac so the next build agrees.
#   CONFIRM_PROD=canifishthis.ca deploy/prod/rollback.sh [--dry-run]
#   deploy/prod/rollback.sh --bucket <a non-production bucket> [--dry-run]      (a rehearsal: no confirmation needed)
set -euo pipefail
cd "$(dirname "$0")/../.."
PROD_BUCKET=bc-fishing-regulations
BUCKET=$PROD_BUCKET; DRY=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run) DRY=(--dry-run) ;;
    --bucket) BUCKET=${2:?--bucket needs a name}; shift ;;
    *) echo "rollback (prod): unknown argument $1" >&2; exit 2 ;;
  esac; shift
done
if [[ $BUCKET == "$PROD_BUCKET" && "${CONFIRM_PROD:-}" != canifishthis.ca ]]; then
  echo "rollback (prod): set CONFIRM_PROD=canifishthis.ca to roll $PROD_BUCKET back (or --bucket <other> to rehearse)" >&2; exit 2
fi
D="r2:$BUCKET"
# an object: 0; none: 1 (R2 answers a missing key as an empty "directory", rc 0); rclone failed: 2
obj() { local o; o=$(rclone lsjson --stat "$1" --s3-no-check-bucket) || return 2; [[ $o == *'"IsDir": false'* ]] || return 1; }
set +e; obj "$D/release/manifest.previous.json"; rc=$?; set -e
case $rc in
  0) ;;
  1) echo "rollback (prod): $D has no release/manifest.previous.json (no promote by deploy/prod/upload.sh yet): nothing to roll back to" >&2; exit 1 ;;
  *) echo "rollback (prod): could not read $D/release/manifest.previous.json (rclone exit $rc)" >&2; exit 1 ;;
esac
rel() { rclone cat "$D/release/$1" --s3-no-check-bucket | python3 -c "import json,sys;print(json.load(sys.stdin)['data_release'])"; }
NOW=$(rel manifest.json); PREV=$(rel manifest.previous.json)
[[ $NOW != "$PREV" ]] || { echo "rollback (prod): the previous pointer is the live one ($NOW): nothing to roll back" >&2; exit 1; }
# every file the previous manifest names must still be there, or the rollback would point apps at 404s
rclone cat "$D/release/manifest.previous.json" --s3-no-check-bucket | python3 -c "
import json,sys; m=json.load(sys.stdin)
for f in m['files'].values(): print(f['url'])" | while read -r u; do
  obj "$D/release/$u" || { echo "rollback (prod): release/$u is gone" >&2; exit 1; }
done
echo "== $D: $NOW -> $PREV${DRY[*]:+ (dry run)}"
rclone copyto "$D/release/manifest.previous.json" "$D/release/manifest.json" --s3-no-check-bucket ${DRY[@]+"${DRY[@]}"} \
  --header-upload "Cache-Control: no-cache" --header-upload "Content-Type: application/json"
[[ ${#DRY[@]} -gt 0 || $BUCKET != "$PROD_BUCKET" ]] || echo "$PREV" > data/release/PUBLISHED   # (T72) the prune keeps it
echo "rolled back to $PREV${DRY[*]:+ (dry run: nothing was written)}. On the Mac: echo $PREV > data/release/LATEST"
