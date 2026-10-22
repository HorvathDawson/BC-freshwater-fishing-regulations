"""Gauges: which water can be told what its flow is, and how honestly.

THE THREE-WAY SPLIT IS BY WHAT EACH HALF NEEDS, not by tidiness. This was one `hydro/`
package of 2,493 lines mixing a matcher that loads 2.8 GB of pickled geometry with a feed
publisher that knows nothing but station ids — which is exactly why "do not re-run matching
on every build" had to be REMEMBERED rather than being obvious from the tree.

    generate/   needs the atlas: graph.pkl, geometries.pkl, a completed build.
                Runs a few times a year, by hand, on a machine that can hold it.
                Writes candidates; a human promotes them.

    consume/    needs only the frozen matches and the flow graph's topology.
                Runs on every build. Cannot reach the matcher, by construction.

    feed/       needs neither. Station ids and HTTP. Runs every 30 minutes forever,
                and must never import from `generate` — a cron job that could
                re-derive a match is a cron job that can silently change one.

`review.py` sits at the top rather than in a subpackage because it belongs to both halves:
an INPUT to generation, and the evidence a consumer needs to explain why a station speaks
for a water. It is authored by a person and never written by a program.
"""

from pipeline.gauges.consume.shed import (
    TRUST_BANDS, GaugeLink, build_gauge_sheds, is_lake_station, lake_gauge_links, trust_for,
)
from pipeline.gauges.matches import StationMatch, read_match, write_match

__all__ = ["TRUST_BANDS", "GaugeLink", "StationMatch", "build_gauge_sheds",
           "is_lake_station", "lake_gauge_links", "read_match", "trust_for",
           "write_match"]

# `match_stations` is DELIBERATELY not re-exported: importing it pulls in shapely's STRtree and the
# expectation of a whole atlas. Reach for `pipeline.gauges.generate.match` explicitly when you mean
# to match something — a few times a year, by hand; nothing outside `generate/` imports it
# (`test_gauge_imports.py`). Placing the frozen match on a graph (`nodes_for`, `summarise`) is a
# consumer's job: `pipeline.gauges.consume.place` (it needs geopandas, so it is not re-exported here).
