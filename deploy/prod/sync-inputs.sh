#!/bin/bash
# PRODUCTION — THE OWNER RUNS THIS (T404; agents never touch prod). The twin of deploy/staging/sync-inputs.sh: the feeds
# Worker's curated inputs + the app's in-season links into the PRODUCTION bucket, writing only what differs. The list is
# `python -m pipeline.deliver.sync_inputs` (one list for both, T331). deploy/prod/upload.sh calls it.
#   CONFIRM_PROD=canifishthis.ca deploy/prod/sync-inputs.sh [--dry-run]
#   FEEDS_URL=<the worker reading that bucket> deploy/prod/sync-inputs.sh --bucket <a non-production bucket> [--dry-run]
#                                                                              (a rehearsal: no confirmation needed)
# FORMATS (T597 item 2, the twin of staging's check; deploy/lib/input-formats.sh is the one copy): an input naming a
# format the DEPLOYED production worker does not list on <FEEDS_URL>/status.json `inputs` is refused (`FAILED <key>: …`),
# never put; the others still go up and the script exits 1. Deploy the worker that reads the format first
# (deploy/prod/feeds.sh), then sync again. A --dry-run asks the worker too (a GET) and says what it would refuse.
#   FEEDS_WORKER_NOT_LIVE=1   the FIRST seed, before deploy/prod/feeds.sh has ever run: no worker reads the bucket, so
#                             there is nothing to ask and nothing to break. Honoured only while the address gives no
#                             answer or the placeholder answers; once the real worker is live its list decides.
set -euo pipefail
cd "$(dirname "$0")/../.."
PROD_BUCKET=bc-fishing-regulations
PROD_FEEDS_URL=https://feeds.canifishthis.ca     # deploy/prod/feeds.sh URL (the custom domain; workers.dev is off)
BUCKET=$PROD_BUCKET; DRY=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run) DRY=(--dry-run) ;;
    --bucket) BUCKET=${2:?--bucket needs a name}; shift ;;
    *) echo "sync-inputs (prod): unknown argument $1" >&2; exit 2 ;;
  esac; shift
done
if [[ $BUCKET == "$PROD_BUCKET" && "${CONFIRM_PROD:-}" != canifishthis.ca ]]; then
  echo "sync-inputs (prod): set CONFIRM_PROD=canifishthis.ca to write $PROD_BUCKET (or --bucket <other> to rehearse)" >&2; exit 2
fi
# the worker that reads the bucket being written: production's for the production bucket (never overridden there); a
# rehearsal names its own, and one that names none has every formatted input refused (it cannot be asked)
if [[ $BUCKET == "$PROD_BUCKET" ]]; then FEEDS_URL=$PROD_FEEDS_URL; else FEEDS_URL=${FEEDS_URL:-}; fi
D="r2:$BUCKET"
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
"$PY" -m pipeline.deliver.sync_inputs --out "$T/out" > "$T/manifest.tsv"
rc=0
DEPLOY_HINT="CONFIRM_PROD=bc-fishing-feeds deploy/prod/feeds.sh"
NO_WORKER_YET=${FEEDS_WORKER_NOT_LIVE:-}
: > "$T/failed"
refuse() { echo "FAILED $1" | tee -a "$T/failed" >&2; rc=1; }
# shellcheck source=deploy/lib/input-formats.sh
source deploy/lib/input-formats.sh
input_formats_filter "$T/manifest.tsv" "$T/out" "$T/put.tsv"
JSON=(--s3-no-check-bucket --checksum --header-upload "Content-Type: application/json" ${DRY[@]+"${DRY[@]}"})
n=0
while IFS=$'\t' read -r key cache; do
  CC=(); [ "$cache" != "-" ] && CC=(--header-upload "Cache-Control: $cache")
  rclone copyto "$T/out/$key" "$D/$key" "${JSON[@]}" ${CC[@]+"${CC[@]}"} -v > "$T/log" 2>&1 \
    || { cat "$T/log" >&2; echo "sync-inputs (prod): $key failed — nothing after it was sent" >&2; exit 1; }
  grep -E "Copied|Skipped|ERROR|NOTICE" "$T/log" || true
  n=$((n + 1))
done < "$T/put.tsv"
if [[ $rc != 0 ]]; then
  echo "sync-inputs (prod): $n object(s) put${DRY[*]:+ (dry run)}; these inputs were NOT:" >&2; cat "$T/failed" >&2
  exit 1
fi
echo "inputs in sync${DRY[*]:+ (dry run)}: $D ($n objects, in-season matches $(jq '.notices|length' "$T/out/feeds/inseason/matches.json"))"
