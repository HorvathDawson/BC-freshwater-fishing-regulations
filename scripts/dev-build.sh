#!/usr/bin/env bash
#
# Everything a developer needs to see the app with real data, in dependency order.
#
#   ./scripts/dev-build.sh            the fast path -- feeds only, ~1 minute
#   ./scripts/dev-build.sh --full     also refresh HYDAT + the envelope (yearly, 266 MB)
#
# WHY THIS EXISTS. The steps have a strict order and three different clocks, and getting the
# order wrong fails QUIETLY: publish before the envelope exists and every percentile is
# null, which looks exactly like a working feed over a dull map. One script, stated order.
#
#   yearly    HYDAT archive   ->  the percentile envelope
#   per build station roster  ->  the bundle gauge tables
#   30 min    ECCC readings   ->  the feed the app actually reads
#             BCRFC models    ->  the forecast riding in the same files (3 requests)
#
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
FEED=data/generated/gauges/feeds
FULL=${1:-}

echo "-- roster -------------------------------------------"
# Idempotent: leaves an existing file alone, so the fast path costs nothing.
(cd data && PYTHONPATH=.. ../$PY fetch_data.py --layers hydrometric_stations)

if [ "$FULL" = "--full" ]; then
  echo "-- HYDAT (yearly, ~266 MB; skipped unless the release moved) --"
  (cd data && PYTHONPATH=.. ../$PY fetch_data.py --layers hydat)
  echo "-- envelope -----------------------------------------"
  $PY -m pipeline.gauges.feed.climatology --out "$FEED/clim.json"
fi

echo "-- feed ---------------------------------------------"
if [ -f "$FEED/clim.json" ]; then
  $PY -m pipeline.gauges.feed.publish --out "$FEED" --clim "$FEED/clim.json"
else
  echo "  (no envelope yet: percentiles will be null. Run with --full once.)"
  $PY -m pipeline.gauges.feed.publish --out "$FEED"
fi

echo "-- app fixture --------------------------------------"
(cd app && node tools/build-fixture.mjs | tail -3)

echo ""
echo "-- ready --------------------------------------------"
echo "  The feed now carries BC River Forecast Centre outlooks (CLEVER / COFFEE / ELF)."
echo "  They are SEASONAL: outside a model's season it publishes nothing, and a station"
echo "  with no forecast today is the normal case, not a failure."
echo "  serve-tiles.mjs serves  /feeds/live/*   real data,    province bundle"
echo "                          /feeds/gauge/*  fixture data, fixture bundle"
echo "  Point apps/mobile FEED + BUNDLE at the SAME pair: a feed is only"
echo "  meaningful with the bundle it was cut against."
