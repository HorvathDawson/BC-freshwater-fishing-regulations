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
#                         fetch_data's hydat layer downloads through scripts/hydat-fetch.sh (the checked one, T433)
#                         and fails on a failed listing or fetch; the script still asks the listing itself and
#                         STOPS unless data/source now holds the newest release.
#   2. the envelope       clim.json, from that HYDAT, into the served feed directory
#      (T557: no step republishes the feed here. The feeds worker is the ONE gauge publisher: it re-ranks every station
#      on its next run once this clim.json is on R2 (the "feeds — HYDAT → climatology → R2" workflow uploads it), and the
#      hydat notice clears there. pipeline/gauges/feed/publish.py is the worker's spec — the parity test's oracle and
#      the local dev feed (scripts/dev-build.sh) — never a feed anyone is served.)
#   3. the bundle         `pipeline build` reruns its clim stage (keyed on the HYDAT stamp: the same envelope again)
#                         and the deliver stage (its key holds the hydat stamp and clim's hash):
#                         gauge_clim / gauge_stats / section_panel, and a new release whose manifest dates HYDAT.
#                         Heavy, so through scripts/heavy.sh: one heavy job at a time, with the memory guard.
#   4. the upload         the new release to R2 through the release publisher (`pipeline.deliver.release.publish`: tiles,
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

echo "-- 3/4 bundle + release (heavy) -------------------------"
scripts/heavy.sh build 40 -- $PY -m pipeline build

echo "-- 4/4 upload to R2 -------------------------------------"
# the release `pipeline build` just wrote: THE release on disk (`release.manifest.latest`, the publisher's own
# default and the one answer every reader asks — T133), named here
REL=$($PY -c 'from pipeline.deliver.release.manifest import latest; print(latest("data/release") or "")')
if [ -z "$REL" ]; then
  echo "   no release under data/release; nothing to upload." >&2; exit 1
fi
$PY -m pipeline.deliver.release.publish --release "$REL"
if [ "$UPLOAD" = 1 ]; then
  r2_ready
  $PY -m pipeline.deliver.release.publish --release "$REL" --yes
  sent="uploaded to R2 ($REL)"
else
  sent="NOT uploaded: the plan is above. To send it (R2_ACCOUNT_ID R2_ACCESS_KEY_ID R2_SECRET_ACCESS_KEY R2_BUCKET set):"
fi

echo ""
echo "-- done -------------------------------------------------"
echo "  HYDAT $(cat data/source/hydat.release): fetched, envelope rebuilt ($FEED/clim.json),"
echo "  the feeds worker re-ranks on its next run once clim.json is on R2 (its hydat notice clears there),"
echo "  bundle and release rebuilt by \`pipeline build\`; release $sent"
if [ "$UPLOAD" != 1 ]; then
  echo "       $PY -m pipeline.deliver.release.publish --release $REL --yes"
fi
