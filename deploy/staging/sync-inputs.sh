#!/bin/bash
# STAGING ONLY (T225 Phase A, owner 2026-10-28): put the feeds Worker's curated inputs and the app's in-season links into
# bc-fishing-regulations-staging, writing only what differs (rclone --checksum). THE ONE PUBLISH PATH (T520, owner
# 2026-11-08: from the Mac only; the GitHub workflow is gone): upload.sh calls it after a release upload, and the review
# app runs it after saving any input it stages (curation-review/backend/inputs_sync.py). Production: deploy/prod/
# sync-inputs.sh, the owner's CONFIRM_PROD step. Never touches git.
#   1. ONE AT A TIME: a lock shared by every caller (by hand, upload.sh, the review app), waited for
#   2. VERIFY (T537): matches naming another release than LATEST are REFUSED, never re-resolved here (it ran mid-upload,
#      after the promote, and left the curated file dirty): `pipeline release --staging` resolves them on the candidate
#      before its promote; by hand, the command is printed; from the review app (SYNC_INPUTS_FROM_REVIEW=1) re-resolve
#      there, then commit (the file is written only through its writes.commit)
#   3. REFUSALS, PER INPUT: `sync_inputs` stages each input on its own (the in-season links through
#      `pipeline.regs.inseason.publish`, which refuses an unresolved / stale / off-release match); a refused input is
#      named (`FAILED <name>: …`) and the others still go up; the script then exits 1
#   4. FORMATS (T597 item 2): an input naming a format the deployed worker does not list on /status.json `inputs` is
#      refused (`FAILED <key>: …`), never put; the others still go up
#   5. PUT what differs
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
LOCK=${SYNC_INPUTS_LOCK:-data/generated/.sync-inputs.lock}
mkdir -p "$(dirname "$LOCK")"
for i in $(seq 1 600); do
  if mkdir "$LOCK" 2>/dev/null; then echo $$ > "$LOCK/pid"; break; fi
  holder=$(cat "$LOCK/pid" 2>/dev/null || true)
  if [[ -n "$holder" ]] && ! kill -0 "$holder" 2>/dev/null; then rm -rf "${LOCK:?}"; continue; fi   # a dead holder
  [[ $i == 1 ]] && echo "sync-inputs: waiting for another sync (pid ${holder:-?})" >&2
  sleep 1
done
[[ "$(cat "$LOCK/pid" 2>/dev/null)" == "$$" ]] || { echo "sync-inputs: another sync held the lock for 10 min" >&2; exit 1; }
T=$(mktemp -d); trap 'rm -rf "$T" "${LOCK:?}"' EXIT
LATEST=$(cat data/release/LATEST)
if [[ "$(jq -r .release data/curated/inseason/notice_waters.json)" != "$LATEST" ]]; then
  if [[ "${SYNC_INPUTS_FROM_REVIEW:-}" == 1 ]]; then
    echo "FAILED inseason-matches: resolved on $(jq -r .release data/curated/inseason/notice_waters.json), LATEST is $LATEST — re-resolve in the review app, then commit" >&2
    exit 1
  fi
  echo "FAILED inseason-matches: resolved on $(jq -r .release data/curated/inseason/notice_waters.json), LATEST is $LATEST — nothing was put. Re-resolve:" >&2
  echo "  scripts/heavy.sh inseason-resolve 30 -- $PY -m pipeline.regs.inseason.resolve --release data/release/$LATEST" >&2
  echo "  git commit -m 'inseason: matches resolved on $LATEST' -- data/curated/inseason/notice_waters.json" >&2
  exit 1
fi
rc=0
"$PY" -m pipeline.deliver.sync_inputs --out "$T/out" > "$T/manifest.tsv" 2> "$T/stage.err" || rc=$?
cat "$T/stage.err" >&2
# (T597 item 2) an input in a format the DEPLOYED worker does not list is refused, never put: deploy/lib/input-formats.sh
# (the one copy, shared with deploy/prod/sync-inputs.sh). Staging never claims "no worker yet".
FEEDS_URL=${FEEDS_URL:-https://bc-fishing-feeds-staging.horvath-dawson.workers.dev}
DEPLOY_HINT=deploy/staging/feeds.sh
NO_WORKER_YET=
refuse() { echo "FAILED $1" | tee -a "$T/stage.err" >&2; rc=1; }
# shellcheck source=deploy/lib/input-formats.sh
source deploy/lib/input-formats.sh
input_formats_filter "$T/manifest.tsv" "$T/out" "$T/put.tsv"
JSON=(--s3-no-check-bucket --checksum --header-upload "Content-Type: application/json" ${DRY[@]+"${DRY[@]}"})
n=0
while IFS=$'\t' read -r key cache; do
  CC=(); [ "$cache" != "-" ] && CC=(--header-upload "Cache-Control: $cache")
  # (T433) a failed copy STOPS the script (upload.sh then writes no pointer); the log is filtered only after the check
  rclone copyto "$T/out/$key" "$D/$key" "${JSON[@]}" ${CC[@]+"${CC[@]}"} -v > "$T/log" 2>&1 \
    || { cat "$T/log" >&2; echo "sync-inputs: $key failed — nothing after it was sent" >&2; exit 1; }
  grep -E "Copied|Skipped|ERROR" "$T/log" || true
  n=$((n + 1))
done < "$T/put.tsv"
ins=$([ -f "$T/out/feeds/inseason/matches.json" ] && jq '.notices|length' "$T/out/feeds/inseason/matches.json" || echo "NOT STAGED")
if [[ $rc != 0 ]]; then
  echo "sync-inputs: $n object(s) put; these inputs were NOT:" >&2; grep '^FAILED ' "$T/stage.err" >&2 || true
  exit 1
fi
echo "inputs in sync: $D ($n objects, in-season matches $ins)"
