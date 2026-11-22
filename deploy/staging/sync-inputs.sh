#!/bin/bash
# STAGING ONLY (T225 Phase A, owner 2026-10-28): put the feeds Worker's curated inputs and the app's in-season links into
# bc-fishing-regulations-staging, writing only what differs (rclone --checksum). THE ONE PUBLISH PATH (T520, owner
# 2026-11-08: from the Mac only; the GitHub workflow is gone): upload.sh calls it after a release upload, and the review
# app runs it after saving any input it stages (curation-review/backend/inputs_sync.py). Production: deploy/prod/
# sync-inputs.sh, the owner's CONFIRM_PROD step. Never touches git.
#   1. ONE AT A TIME: a lock shared by every caller (by hand, upload.sh, the review app), waited for
#   2. VERIFY (T537): matches resolved on another release than LATEST are REFUSED, never re-resolved here (it ran
#      mid-upload, after the promote): `pipeline release --staging` resolves them on the candidate before its promote;
#      by hand, the command is printed; from the review app (SYNC_INPUTS_FROM_REVIEW=1) re-resolve there. (T679 #11)
#      The resolution is GENERATED (data/generated/inseason/resolved.json, never committed); the curated
#      notice_waters.json holds only the owner's matches
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
# (T994 #18) every argument is read, an unknown one refused (as production's twin): `--dry-run` was read from $1 only,
# so in second place, or misspelt, it did a REAL put
DRY=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --dry-run) DRY=(--dry-run) ;;
    *) echo "sync-inputs: unknown argument $1 (deploy/staging/sync-inputs.sh [--dry-run])" >&2; exit 2 ;;
  esac; shift
done
command -v jq >/dev/null || { echo "sync-inputs: jq is not installed (brew install jq): nothing was put" >&2; exit 1; }
PY=python3; [ -x .venv/bin/python ] && PY=.venv/bin/python
LOCK=${SYNC_INPUTS_LOCK:-data/generated/.sync-inputs.lock}
mkdir -p "$(dirname "$LOCK")"
# ONE AT A TIME. (T994 #18) a dead holder's lock is taken over as scripts/heavy.sh does: under a mutex, re-checked, then
# `mv` (atomic) and delete — a bare `rm -rf` let a second waiter delete the lock the first had just taken. The
# 2>/dev/null here are the lock's own probes: mkdir losing the race, a pid file not yet written, kill -0 on a dead pid.
held=0
for i in $(seq 1 600); do
  if mkdir "$LOCK" 2>/dev/null; then echo $$ > "$LOCK/pid"; held=1; break; fi
  holder=$(cat "$LOCK/pid" 2>/dev/null) || holder=""
  if [[ -n "$holder" ]] && ! kill -0 "$holder" 2>/dev/null; then
    if mkdir "$LOCK.takeover" 2>/dev/null; then
      if [[ "$(cat "$LOCK/pid" 2>/dev/null)" == "$holder" ]] && mv "$LOCK" "$LOCK.stale.$$" 2>/dev/null; then
        rm -rf "$LOCK.stale.$$"; echo "sync-inputs: took over the lock of a sync that is gone (pid $holder)" >&2
      fi
      rmdir "$LOCK.takeover"; continue
    elif (( $(date +%s) - $(stat -f %m "$LOCK.takeover" 2>/dev/null || date +%s) > 60 )); then
      rmdir "$LOCK.takeover" 2>/dev/null; continue    # a waiter died holding the takeover mutex (held for milliseconds)
    fi
  fi
  [[ $i == 1 ]] && echo "sync-inputs: waiting for another sync (pid ${holder:-?})" >&2
  sleep 1
done
[[ $held == 1 ]] || { echo "sync-inputs: another sync held the lock for 10 min" >&2; exit 1; }
T=$(mktemp -d); trap 'rm -rf "$T"; [[ "$(cat "$LOCK/pid" 2>/dev/null)" == "$$" ]] && rm -rf "${LOCK:?}"' EXIT
LATEST=$(cat data/release/LATEST)
RESOLVED=data/generated/inseason/resolved.json     # (T679 #11) `pipeline.regs.inseason.matches.resolved_path()`
# a missing resolution is "nothing"; one jq cannot read STOPS (it read as "no file" too)
if [[ -f $RESOLVED ]]; then ON=$(jq -r .release "$RESOLVED") || { echo "sync-inputs: $RESOLVED cannot be read (jq, above)" >&2; exit 1; }
else ON="nothing (no $RESOLVED)"; fi
if [[ "$ON" != "$LATEST" ]]; then
  if [[ "${SYNC_INPUTS_FROM_REVIEW:-}" == 1 ]]; then
    echo "FAILED inseason-matches: resolved on $ON, LATEST is $LATEST — re-resolve in the review app" >&2
    exit 1
  fi
  echo "FAILED inseason-matches: resolved on $ON, LATEST is $LATEST — nothing was put. Re-resolve:" >&2
  echo "  scripts/heavy.sh inseason-resolve 30 -- $PY -m pipeline.regs.inseason.resolve --release data/release/$LATEST" >&2
  exit 1
fi
rc=0
"$PY" -m pipeline.deliver.sync_inputs --out "$T/out" > "$T/manifest.tsv" 2> "$T/stage.err" || rc=$?
cat "$T/stage.err" >&2
# (T597 item 2) an input in a format the DEPLOYED worker does not list is refused, never put: deploy/lib/input-formats.sh
# (the one copy, shared with deploy/prod/sync-inputs.sh). Staging never claims "no worker yet". (T788 #1) staging's
# worker is named HERE, never taken from the caller's shell (a stray FEEDS_URL asked another worker's list)
FEEDS_URL=https://bc-fishing-feeds-staging.horvath-dawson.workers.dev
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
  grep -E "Copied|Skipped|ERROR" "$T/log" || true    # (T994 #19, kept) only the log's filter: no line is rc 1, not a failure
  n=$((n + 1))
done < "$T/put.tsv"
if [[ $rc != 0 ]]; then
  # (kept) the staging step may fail with no FAILED line of its own: its whole stderr was printed above
  echo "sync-inputs: $n object(s) put; these inputs were NOT:" >&2; grep '^FAILED ' "$T/stage.err" >&2 || true
  exit 1
fi
# counted after the refusals: a matches file that could not be read was refused above, never counted here
if [[ -f "$T/out/feeds/inseason/matches.json" ]]; then ins=$(jq '.notices|length' "$T/out/feeds/inseason/matches.json")
else ins="NOT STAGED"; fi
echo "inputs in sync: $D ($n objects, in-season matches $ins)"
