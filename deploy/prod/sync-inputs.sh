#!/bin/bash
# PRODUCTION — THE OWNER RUNS THIS (T404; agents never touch prod). The twin of deploy/staging/sync-inputs.sh: the feeds
# Worker's curated inputs + the app's in-season links into the PRODUCTION bucket, writing only what differs. The list is
# `python -m pipeline.deliver.sync_inputs` (one list for both, T331). deploy/prod/upload.sh calls it.
#   CONFIRM_PROD=canifishthis.ca deploy/prod/sync-inputs.sh [--dry-run]
#   deploy/prod/sync-inputs.sh --bucket <a non-production bucket> [--dry-run]     (a rehearsal: no confirmation needed)
set -euo pipefail
cd "$(dirname "$0")/../.."
PROD_BUCKET=bc-fishing-regulations
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
D="r2:$BUCKET"
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
"$PY" -m pipeline.deliver.sync_inputs --out "$T/out" > "$T/manifest.tsv"
JSON=(--s3-no-check-bucket --checksum --header-upload "Content-Type: application/json" ${DRY[@]+"${DRY[@]}"})
n=0
while IFS=$'\t' read -r key cache; do
  CC=(); [ "$cache" != "-" ] && CC=(--header-upload "Cache-Control: $cache")
  rclone copyto "$T/out/$key" "$D/$key" "${JSON[@]}" ${CC[@]+"${CC[@]}"} -v > "$T/log" 2>&1 \
    || { cat "$T/log" >&2; echo "sync-inputs (prod): $key failed — nothing after it was sent" >&2; exit 1; }
  grep -E "Copied|Skipped|ERROR|NOTICE" "$T/log" || true
  n=$((n + 1))
done < "$T/manifest.tsv"
echo "inputs in sync${DRY[*]:+ (dry run)}: $D ($n objects, in-season matches $(jq '.matches|length' "$T/out/feeds/inseason/matches.json"))"
