#!/usr/bin/env bash
#
# update-hydat.sh — the owner's ONE command when the feed says a newer HYDAT is out
# (the index's `maintenance` notice of kind "hydat", the notification, or the publisher's console warning).
#
#   scripts/update-hydat.sh                         the served feed in data/generated/gauges/feeds (the only one:
#                                                   `pipeline build` packs the bundle from THAT clim.json)
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
# Tiles are not keyed on HYDAT and are not rebuilt for it. Uploading the new release is not done here.
#
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
FEED=data/generated/gauges/feeds

echo "-- 1/4 HYDAT --------------------------------------------"
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

echo "-- 2/4 envelope -----------------------------------------"
$PY -m pipeline.gauges.feed.climatology --hydat data/source/hydat.sqlite3 --out "$FEED/clim.json"

echo "-- 3/4 feed (republished with the new envelope) ----------"
$PY -m pipeline.gauges.feed.publish --out "$FEED" --clim "$FEED/clim.json"

echo "-- 4/4 bundle + release (heavy) -------------------------"
scripts/heavy.sh build 40 -- $PY -m pipeline build

echo ""
echo "-- done -------------------------------------------------"
echo "  HYDAT $(cat data/source/hydat.release): fetched, envelope rebuilt ($FEED/clim.json),"
echo "  feed republished ($FEED/index.json: the hydat notice is gone when current == latest),"
echo "  bundle and release rebuilt by \`pipeline build\`. Ship the new release as usual."
