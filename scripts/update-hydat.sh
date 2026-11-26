#!/usr/bin/env bash
#
# update-hydat.sh — the owner's ONE command when the feed says a newer HYDAT is out
# (the index's `maintenance` notice of kind "hydat", the notification, or the publisher's console warning).
#
#   scripts/update-hydat.sh                         the served feed in data/generated/gauges/feeds (the only one:
#                                                   `pipeline build` packs the bundle from THAT clim.json)
#
# THE ORDER IS THE POINT (this script's own; AGENTS.md rule 64 makes the HYDAT refresh the owner's):
#   1. fetch HYDAT        data/source/hydat.sqlite3 + hydat.release (~266 MB; a no-op when already the newest).
#                         fetch_data's hydat layer downloads through scripts/hydat-fetch.sh (the checked one, T433)
#                         and fails on a failed listing or fetch; the script still asks the listing itself and
#                         STOPS unless data/source now holds the newest release.
#   2. the envelope       clim.json, from that HYDAT, into the served feed directory
#      (T557: no step republishes the feed here. The feeds worker is the ONE gauge publisher: it re-ranks every station
#      on its next run once this clim.json is on R2 (the "feeds — HYDAT → climatology → R2" workflow uploads it), and the
#      hydat notice clears there. pipeline/gauges/feed/publish.py is the worker's spec — the parity test's oracle and
#      the local dev feed (scripts/dev-build.sh) — never a feed anyone is served.)
#   3. the bundle         `pipeline build --hold-latest` reruns its clim stage (keyed on the HYDAT stamp: the same
#                         envelope again) and the deliver stage (its key holds the hydat stamp and clim's hash):
#                         gauge_clim / gauge_stats / section_panel, and a new release whose manifest dates HYDAT. The
#                         release WAITS in data/release/CANDIDATE (LATEST untouched) for the release command's checks.
#                         Heavy, so through scripts/heavy.sh: one heavy job at a time, with the memory guard.
#   4. the hand-off       (T994 #1) THIS SCRIPT UPLOADS NOTHING. Its old `--upload` sent the release through
#                         `pipeline.deliver.release.publish --yes` to whatever R2_BUCKET named, past every deploy gate.
#                         A release reaches R2 only by the one release path, printed here:
#                         `python -m pipeline release --staging` (it finds this build done, runs its checks against
#                         the live release, promotes LATEST, uploads STAGING), then the owner's
#                         `CONFIRM_PROD=<r-id> python -m pipeline release --prod <r-id>`. Until then no installed app
#                         reads the new HYDAT.
# Tiles are not keyed on HYDAT and are not rebuilt for it.
# The PRODUCTION feed is published by its own cron (T86); its clim.json must be the one built in step 2.
#
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
FEED=data/generated/gauges/feeds
for a in "$@"; do
  case "$a" in
    --upload) echo "scripts/update-hydat.sh: --upload is gone (T994 #1): a release reaches R2 only through" \
                   "\`python -m pipeline release --staging\`, which this script prints at the end. Nothing run." >&2
              exit 2 ;;
    *) echo "usage: scripts/update-hydat.sh" >&2; exit 2 ;;
  esac
done

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
# a CANDIDATE left by an earlier build (a release that stopped at its checks) is not this run's release
WAS=$(cat data/release/CANDIDATE 2>/dev/null || true)
scripts/heavy.sh build 40 -- $PY -m pipeline build --hold-latest

echo "-- 4/4 hand-off: the release command uploads -----------"
# the release the build just wrote, waiting for its checks (data/release/CANDIDATE, `--hold-latest`)
REL=$(cat data/release/CANDIDATE 2>/dev/null || true)
if [ -z "$REL" ] || [ ! -f "data/release/$REL/manifest.json" ]; then
  echo "   the build named no release in data/release/CANDIDATE; nothing to hand over." >&2; exit 1
fi
if [ "$REL" = "$WAS" ]; then
  echo "   the build made no new release: data/release/CANDIDATE still names $REL, left by an earlier build (not this" \
       "HYDAT's); nothing handed over. Read \`$PY -m pipeline build --status\` (a deliver stage already up to date" \
       "means this HYDAT was already built)." >&2
  exit 1
fi

echo ""
echo "-- done -------------------------------------------------"
echo "  HYDAT $(cat data/source/hydat.release): fetched, envelope rebuilt ($FEED/clim.json),"
echo "  the feeds worker re-ranks on its next run once clim.json is on R2 (its hydat notice clears there),"
echo "  bundle rebuilt by \`pipeline build --hold-latest\`; release $REL waits in data/release/CANDIDATE, NOT uploaded."
echo "  Next (its checks, the promote, the STAGING upload):"
echo "       $PY -m pipeline release --staging"
echo "  then production, yours only, with the release id it staged (it says so; $REL unless it rebuilt):"
echo "       CONFIRM_PROD=<r-id> $PY -m pipeline release --prod <r-id>"
