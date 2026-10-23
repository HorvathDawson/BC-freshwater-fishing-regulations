#!/usr/bin/env bash
#
# update-hydat.sh — the owner's ONE command when the feed says a newer HYDAT is out
# (the index's `maintenance` notice of kind "hydat", the notification, or the publisher's console warning).
#
#   scripts/update-hydat.sh                         the served feed in data/generated/gauges/feeds (the only one:
#                                                   `pipeline build` packs the bundle from THAT clim.json)
#   scripts/update-hydat.sh --upload                the same, then uploads the new release to R2 (needs the
#                                                   R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET env)
#
# THE ORDER IS THE POINT (handoff/PLAN/app-integration/decisions.md, "upstream updates the owner runs: HYDAT first"):
#   1. fetch HYDAT        data/source/hydat.sqlite3 + hydat.release (~266 MB; a no-op when already the newest).
#                         fetch_data.py prints a failed listing or download and still exits 0, so the script then
#                         asks the listing itself and STOPS unless data/source now holds the newest release.
#   2. the envelope       clim.json, from that HYDAT, into the served feed directory
#   3. republish the feed with the new envelope: every percentile re-measured; the notice clears here (current == latest)
#   4. the bundle         `pipeline build` reruns the deliver stage (its key holds the hydat stamp and clim's hash):
#                         gauge_clim / gauge_stats / section_panel, and a new release whose manifest dates HYDAT.
#                         Heavy, so through scripts/heavy.sh: one heavy job at a time, with the memory guard.
#   5. the upload         the new release to R2 through the release publisher (`pipeline.tools.publish`: tiles,
#                         data files, manifest LAST). Without --upload it prints the plan (spends nothing, sends
#                         nothing) and the exact command; with --upload it sends, and stops first if an R2_* variable
#                         is unset. Until the release is uploaded no installed app reads the new HYDAT.
# Tiles are not keyed on HYDAT and are not rebuilt for it (the plan re-lists the current tile folder; same bytes).
# The PRODUCTION feed is published by its own cron (T86); its clim.json must be the one built in step 2.
#
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
FEED=data/generated/gauges/feeds
UPLOAD=0
for a in "$@"; do
  case "$a" in
    --upload) UPLOAD=1 ;;
    *) echo "usage: scripts/update-hydat.sh [--upload]" >&2; exit 2 ;;
  esac
done
# the upload's needs, checked BEFORE the half-hour of work it follows, and again right before it sends
r2_ready() {
  for v in R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET; do
    if [ -z "${!v:-}" ]; then
      echo "   --upload: $v is not set; nothing uploaded. Set R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET." >&2
      exit 1
    fi
  done
  $PY -c "import boto3" 2>/dev/null || { echo "   --upload: boto3 is missing ($PY -m pip install boto3)." >&2; exit 1; }
}
if [ "$UPLOAD" = 1 ]; then r2_ready; fi

echo "-- 1/5 HYDAT --------------------------------------------"
(cd data && PYTHONPATH=.. ../$PY fetch_data.py --layers hydat)
latest=$($PY -c 'import sys; sys.path.insert(0, "data"); from fetch_data import hydat_latest_release as f; print(f() or "")' || true)
have=$(cat data/source/hydat.release 2>/dev/null || true)
if [ -z "$latest" ]; then
  echo "   could not read the HYDAT listing; nothing rebuilt. Run this again." >&2; exit 1
fi
if [ "$have" != "$latest" ]; then
  echo "   HYDAT $latest is listed but data/source holds ${have:-nothing}: the fetch failed (see above); nothing rebuilt." >&2
  exit 1
fi
echo "   now on HYDAT $have"

echo "-- 2/5 envelope -----------------------------------------"
$PY -m pipeline.gauges.feed.climatology --hydat data/source/hydat.sqlite3 --out "$FEED/clim.json"

echo "-- 3/5 feed (republished with the new envelope) ----------"
$PY -m pipeline.gauges.feed.publish --out "$FEED" --clim "$FEED/clim.json"

echo "-- 4/5 bundle + release (heavy) -------------------------"
scripts/heavy.sh build 40 -- $PY -m pipeline build

echo "-- 5/5 upload to R2 -------------------------------------"
# the release `pipeline build` just wrote: the newest data/release/r-* (the publisher's own default, named here)
# (`|| true`: with no match, ls fails and pipefail + set -e would end the script here with no word)
REL=$(ls -1td data/release/r-*/ 2>/dev/null | head -n 1 || true)
REL=${REL%/}
if [ -z "$REL" ]; then
  echo "   no release under data/release; nothing to upload." >&2; exit 1
fi
$PY -m pipeline.tools.publish --release "$REL"
if [ "$UPLOAD" = 1 ]; then
  r2_ready
  $PY -m pipeline.tools.publish --release "$REL" --yes
  sent="uploaded to R2 ($REL)"
else
  sent="NOT uploaded: the plan is above. To send it (R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET set):"
fi

echo ""
echo "-- done -------------------------------------------------"
echo "  HYDAT $(cat data/source/hydat.release): fetched, envelope rebuilt ($FEED/clim.json),"
echo "  feed republished ($FEED/index.json: the hydat notice is gone when current == latest),"
echo "  bundle and release rebuilt by \`pipeline build\`; release $sent"
if [ "$UPLOAD" != 1 ]; then
  echo "       $PY -m pipeline.tools.publish --release $REL --yes"
fi
