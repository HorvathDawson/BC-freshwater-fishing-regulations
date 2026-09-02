"""Gauges as SPLIT DEFINITIONS — turning the frozen match into cuts.

WHY A RIVER IS CUT AT ITS GAUGES. A section takes ONE station: the one that most nearly is
that water. The Fraser mainstem is 20 nodes for 1,375 km with 18 transmitting stations on
it, so one section runs between Hope and Lillooet and claims a single reading for all of it.
The answer is not a better ranking but a shorter reach — a gauge is the boundary between two
measurements, so the river should change section there.

THIS FILE IS A PURE FUNCTION over `pipeline/gauge_match.json` and nothing else. No graph, no
geometry, no second matching pass: everything it needs — the coordinate and the watershed
code — is in the frozen match, which is the single place a station is ever located.

    data/bc_hydrometric_stations.json        fetched, the roster
      │
      │  python -m pipeline.hydro.match --build <a completed build>
      ▼
    pipeline/gauge_match.json                frozen: station -> coord + wsc (or wbk)
      │                        │
      │  pipeline.build        │  pipeline.bundle
      ▼                        ▼
    a `gauge` point anchor     the section that BEGINS at that coordinate and runs
    at the coord, scoped       upstream on that water — what the gauge has just
    by `wsc`                   measured — or `lake:{wbk}`

Two consumers, one fact, and neither of them matches anything.
"""

from __future__ import annotations

# How far the resolver may look from the station's coordinate for a channel to cut. A gauge
# is often on a bank or a bridge while the FWA line is the channel centre, and on a braided
# reach the far strand can be a few hundred metres off. Wide enough for that, narrow enough
# that it cannot reach the next river over — and the WSC scope does the real work.
PROXIMITY_M = 400.0


def split_defs(matches, stations: list[dict], *, live_only: bool = True) -> list[dict]:
    """`SplitDef` dicts, one per station, scoped to the water the match resolved.

    A `gauge` anchor is a `point` anchor with a provenance: identical geometry — the
    published coordinate, projected onto the scoped channel, then swept perpendicular across
    the braid — and a distinct type so a cut a curator wrote and a cut a station generated
    are told apart in `splits.resolved.json` and in the gpkg.

    ``live_only`` keeps the cuts to transmitting stations. A station discontinued in 1974
    still sits somewhere, but cutting the river at it buys a boundary no reading will ever
    appear on. The MATCH still covers every station, because the bundle wants them all.
    """
    roster = {s["station"]: s for s in stations}
    out: list[dict] = []
    for m in sorted(matches, key=lambda m: m.station):
        s = roster.get(m.station)
        if s is None or (live_only and not s.get("realtime")):
            continue
        # A LAKE STATION IS NOT A CUT. It reports a level for a body of water; there is no
        # "above it" and "below it" along a channel to separate. Lake gauges are linked to
        # the lake itself by `pipeline.hydro.shed.lake_gauge_links`, which is the question
        # they can actually answer. The match records them with a `wbk` and no `blk`.
        if m.status != "matched" or m.wbk or not m.wsc:
            continue
        # The coordinate comes from the MATCH, not the roster: one frozen fact, so a
        # roster refreshed between the match and the build cannot move a cut on its own.
        lon, lat = m.lon, m.lat
        if lon is None or lat is None:
            continue
        out.append({
            # THE STATION IS THE ID. It becomes the graph boundary
            # `split:gauge__08MF005` and travels into the registry, so a boundary carries
            # the name of the thing that made it — a client can say "as far as the gauge at
            # Hope" without a lookup. Part of the section-id ABI: renaming the scheme moves
            # every boundary it made.
            "id": f"gauge__{m.station}",
            "station": m.station,
            "label": f"{m.station} · {str(s.get('name', '')).title()}",
            "anchor": {"type": "gauge", "coord": [float(lon), float(lat)],
                       "is_lonlat": True},
            # SCOPED BY WSC, not by blue line. A gauge on a braided reach measures the whole
            # channel, not the strand its coordinate happens to land on; the watershed code
            # is the river AND its side channels, and the point anchor sweeps a
            # perpendicular across them.
            "wsc": m.wsc,
            "proximity_m": PROXIMITY_M,
            "_name": m.name,          # review aid: which water the match chose
        })
    return out
