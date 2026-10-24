#!/usr/bin/env bash
#
# Everything a developer needs to see the app with real data, in dependency order.
#
#   ./scripts/dev-build.sh            the feeds, ~1 minute
#
# THE ENVELOPE IS NOT MADE HERE (N9, plan 2.11): clim.json is the `clim` stage of `python -m pipeline build`,
# keyed on the HYDAT stamp, and a newer HYDAT is `scripts/update-hydat.sh` (fetch, envelope, feed, build).
#
# WHY THIS EXISTS. The steps have a strict order and three different clocks, and getting the
# order wrong fails QUIETLY: publish before the envelope exists and every percentile is
# null, which looks exactly like a working feed over a dull map. One script, stated order.
#
#   yearly    HYDAT archive   ->  the percentile envelope      (pipeline build / update-hydat.sh)
#   per build station roster  ->  the bundle gauge tables
#   30 min    ECCC readings   ->  the feed the app actually reads
#             BCRFC models    ->  the forecast riding in the same files (3 requests)
#
set -euo pipefail
cd "$(dirname "$0")/.."
PY=.venv/bin/python
FEED=data/generated/gauges/feeds

echo "-- roster -------------------------------------------"
# Idempotent: leaves an existing file alone, so the fast path costs nothing.
(cd data && PYTHONPATH=.. ../$PY fetch_data.py --layers hydrometric_stations)

echo "-- feed ---------------------------------------------"
if [ -f "$FEED/clim.json" ]; then
  $PY -m pipeline.gauges.feed.publish --out "$FEED" --clim "$FEED/clim.json"
else
  echo "  (no envelope yet: percentiles will be null. \`python -m pipeline build\` makes it: its clim stage.)"
  $PY -m pipeline.gauges.feed.publish --out "$FEED"
fi

echo ""
echo "-- ready --------------------------------------------"
echo "  The feed now carries BC River Forecast Centre outlooks (CLEVER / COFFEE / ELF)."
echo "  They are SEASONAL: outside a model's season it publishes nothing, and a station"
echo "  with no forecast today is the normal case, not a failure."
echo "  serve-tiles.mjs serves  /feeds/live/*   real data, beside the release it was cut against."
