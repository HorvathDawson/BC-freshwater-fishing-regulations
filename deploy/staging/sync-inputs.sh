#!/bin/bash
# STAGING ONLY (T225 Phase A, owner 2026-10-28): put the feeds Worker's curated inputs and the app's in-season links into
# bc-fishing-regulations-staging, writing only what differs (rclone --checksum). THE ONE PUBLISH PATH (T520, owner
# 2026-11-08: from the Mac only; the GitHub workflow is gone): upload.sh calls it after a release upload, and the review
# app runs it after saving any input it stages (curation-review/backend/inputs_sync.py). Production: deploy/prod/
# sync-inputs.sh, the owner's CONFIRM_PROD step. Never touches git.
#   1. RESOLVE: the in-season matches re-resolved on LATEST (the atlas, heavy.sh) when they name another release
#   2. REFUSALS: `sync_inputs` stages through `pipeline.regs.inseason.publish`, which refuses (exit 1, nothing put) an
#      unresolved match, empty coverage or a range off its release
#   3. PUT what differs
# THE LIST IS NOT HERE (T331): `python -m pipeline.deliver.sync_inputs` stages every input at <dir>/<R2 key> and prints
# `<key>\t<cache-control|->`; both uploaders put exactly that, so they cannot drift (dfo entries, wording, roster,
# in-season matches today — see that module's docstring).
# Needs the rclone remote `r2:` and the repo's .venv (or python3 with pydantic + pyyaml).
#   deploy/staging/sync-inputs.sh [--dry-run]
set -euo pipefail
cd "$(dirname "$0")/../.."
BUCKET=bc-fishing-regulations-staging            # hardcoded: this script must never name a production bucket
D="r2:$BUCKET"
DRY=(); [ "${1:-}" = "--dry-run" ] && DRY=(--dry-run)
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
T=$(mktemp -d); trap 'rm -rf "$T"' EXIT
LATEST=$(cat data/release/LATEST)
if [[ "$(jq -r .release data/curated/inseason/notice_waters.json)" != "$LATEST" ]]; then
  scripts/heavy.sh inseason-resolve 30 -- "$PY" -m pipeline.regs.inseason.resolve --release "data/release/$LATEST" \
    || { echo "sync-inputs: the in-season matches do not all resolve on $LATEST — re-match them (review app)" >&2; exit 1; }
  echo "sync-inputs: in-season matches re-resolved on $LATEST — commit data/curated/inseason/notice_waters.json" >&2
fi
"$PY" -m pipeline.deliver.sync_inputs --out "$T/out" > "$T/manifest.tsv"
JSON=(--s3-no-check-bucket --checksum --header-upload "Content-Type: application/json" ${DRY[@]+"${DRY[@]}"})
n=0
while IFS=$'\t' read -r key cache; do
  CC=(); [ "$cache" != "-" ] && CC=(--header-upload "Cache-Control: $cache")
  # (T433) a failed copy STOPS the script (upload.sh then writes no pointer); the log is filtered only after the check
  rclone copyto "$T/out/$key" "$D/$key" "${JSON[@]}" ${CC[@]+"${CC[@]}"} -v > "$T/log" 2>&1 \
    || { cat "$T/log" >&2; echo "sync-inputs: $key failed — nothing after it was sent" >&2; exit 1; }
  grep -E "Copied|Skipped|ERROR" "$T/log" || true
  n=$((n + 1))
done < "$T/manifest.tsv"
echo "inputs in sync: $D ($n objects, in-season matches $(jq '.notices|length' "$T/out/feeds/inseason/matches.json"))"
