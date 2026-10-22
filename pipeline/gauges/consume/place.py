"""Placing the frozen gauge match on the graph in hand — a CONSUMER's job (audit F05).

`pipeline.gauges.generate.match` decides which water each station is on, a few times a year, against
an atlas it loads whole. The atlas build and the bundle only READ that decision (`matches.json`,
`pipeline.gauges.matches`) and find the same water again in the graph they hold: `nodes_for`. They
used to import it from the matcher, so "the build never re-runs matching" was a convention; here it
is a fact about the import graph (`test_gauge_imports.py`).
"""

from __future__ import annotations

from pipeline.gauges.matches import StationMatch

#: THE CAP HERE IS THE MATCH'S OWN DISTANCE, NOT A SECOND OPINION ABOUT IT.
#:
#: This used to be a flat 500 m while `match_stations` accepted 5,000 m — two independent
#: answers to one question, and 79 matched stations lived in the gap between them: recorded
#: `distance_m` from 513 m to 4,652 m, median 1,015, silently dropped here after being
#: accepted there. Raising the flat cap to 5 km would only move the disagreement, because
#: this pass is not re-deciding which water a station is on. The match decided that. All this
#: does is find the same water again in a graph that has been re-sectioned since.
#:
#: So the bound is `the distance the match recorded, plus slack for the re-sectioning`. A
#: station cannot be placed further from its coordinate than the match already accepted, and
#: one matched at 30 m cannot drift onto a node 4 km away because the sectionizer moved a
#: boundary. The floor keeps a 0 m match from becoming an impossible 0 m requirement.
PLACE_FLOOR_M = 500.0
PLACE_SLACK_M = 250.0


def _place_cap(m: "StationMatch") -> float:
    """How far this station may land from its coordinate, in metres."""
    return max(PLACE_FLOOR_M, (m.distance_m or 0.0) + PLACE_SLACK_M)


def nodes_for(matches: list[StationMatch], graph, geoms: dict,
              *, report: list[str] | None = None) -> dict[str, str]:
    """`{station: node_id}` IN THE GRAPH YOU HAND IT, projected from the frozen coordinate.

    THE GRAPH IS REQUIRED. It used to be optional, and the fallback returned the `node_id`
    recorded when the match was made — a value this module's own docstring calls stale by
    construction, since `{blk}:{down_m}` moves every time the sectionizer cuts differently.
    An argument whose default silently produces last-build's answer is a trap, so there is
    no default.

    ``report`` collects one line per station that could NOT be placed. Nothing is dropped
    quietly: a station that matched and then failed to project is a station the app will
    call ungauged, and the only way that was ever visible was by differencing two files.

    THE UPSTREAM SECTION, where the gauge sits on a boundary. After the gauge cuts, a
    station is exactly on the join between two sections and both contain its coordinate at
    their shared end. The one it belongs to is the UPSTREAM one: that is the water that has
    just flowed past it and been measured. The section below has already taken on whatever
    joins in between.

    Ranked by distance first, then by how near the projection lands to the section's own
    downstream end — which is zero for the section starting at the gauge, and large for the
    one ending there. Lakes never project; they are named directly by their waterbody key.

    Nothing outside the station's own watershed is considered, so a creek gauge twenty
    metres from a mainstem cannot be placed on it.
    """
    import geopandas as gpd
    from shapely.geometry import Point

    drops = report if report is not None else []
    out: dict[str, str] = {}
    todo: list[StationMatch] = []
    for m in matches:
        if m.status != "matched":
            continue
        # A LAKE STATION IS NAMED, NOT PROJECTED. It sits on a body of water rather than
        # along a channel, so its address is the waterbody key and the node is called after
        # it. Forgetting this emptied `lake_gauge` from 220 rows to 0.
        if m.wbk:
            if f"lake:{m.wbk}" in graph.nodes:
                out[m.station] = f"lake:{m.wbk}"
            else:
                drops.append(f"{m.station} no lake:{m.wbk} node in this graph")
            continue
        if m.lon is None or m.lat is None:
            drops.append(f"{m.station} matched with no coordinate")
        elif not m.wsc:
            drops.append(f"{m.station} matched with neither wsc nor wbk")
        else:
            todo.append(m)
    if not todo:
        return out

    # Candidate nodes, grouped by watershed: only water the station is actually on.
    from pipeline.common.utils.wsc import trim_wsc
    by_wsc: dict[str, list[str]] = {}
    for nid, n in graph.nodes.items():
        if nid.startswith("lake:") or nid not in geoms:
            continue
        w = trim_wsc(getattr(n, "wsc", "") or "")
        if w:
            by_wsc.setdefault(w, []).append(nid)

    pts = gpd.GeoSeries([Point(m.lon, m.lat) for m in todo], crs=4326).to_crs(3005)
    for m, pt in zip(todo, pts):
        cap = _place_cap(m)
        best: tuple[float, float, str] | None = None
        for nid in by_wsc.get(trim_wsc(m.wsc), ()):
            g = geoms[nid]
            if g is None or g.is_empty:
                continue
            d = g.distance(pt)
            if d > cap:
                continue
            # `project` is distance from the geometry's START, and FWA lines run mouth to
            # source — so 0 means "this section begins at the gauge and runs upstream".
            rank = (round(d, 1), round(float(g.project(pt)), 1), nid)
            if best is None or rank < best:
                best = rank
        if best is not None:
            out[m.station] = best[2]
        else:
            n = len(by_wsc.get(trim_wsc(m.wsc), ()))
            why = ("no node carries that watershed code" if not n else
                   f"none of its {n} nodes are within {cap:.0f} m")
            drops.append(f"{m.station} {m.name!r} wsc={m.wsc} "
                         f"matched at {m.distance_m} m but {why}")
    return out










def summarise(matches: list[StationMatch], live: set[str] | None = None) -> str:
    """One line per outcome. Printed by the build so a regression is visible immediately."""
    import collections

    rows = [m for m in matches if live is None or m.station in live]
    by = collections.Counter(m.status for m in rows)
    how = collections.Counter(m.resolved_by for m in rows if m.resolved_by)
    total = len(rows) or 1
    return (f"{by['matched']}/{len(rows)} matched ({by['matched']/total:.1%})"
            f"  · " + ", ".join(f"{n} by {k}" for k, n in sorted(how.items()))
            + f"  · {by['no_match']} no_match, {by['unresolved']} unresolved")
