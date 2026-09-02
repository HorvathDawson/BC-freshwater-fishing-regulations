"""Gauge positions as SPLITS — one cut on the river a station actually measures.

THE PROBLEM, IN ONE RIVER. The Fraser mainstem is 20 graph nodes for 1,375 km, and 7
stations sit on it. Every reach takes the single station that best represents it, so one
node — hundreds of kilometres of river — paints one colour from one gauge, and the reading
at Hope is claimed for water at Lillooet. It is not wrong about which station is closest to
representative; it is wrong about how long a single answer may be.

Cutting the river AT each gauge fixes it at the source. A station then sits on a boundary
rather than in the middle of something, the reach above it and the reach below it are
different sections, and the map changes colour where the measurement changes rather than
where the FWA happened to end a blue line.

ONLY ON THE WATER THE STATION MATCHED. A cut is emitted for the blue line the matcher
resolved and for nothing else: a creek gauge 40 m from the mainstem must not cut the
mainstem, and the matcher's own radius-plus-name test is the thing that already refuses
that. Nothing here re-decides which water a station is on.

WHY IT IS A FROZEN ARTIFACT AND NOT A BUILD STEP. Matching needs the graph, and the graph
needs the splits — so this cannot run inside the build that consumes it. It is the same
shape as `added_streams`: run this against a completed build, commit the JSON, and the NEXT
build reads it. That also makes the cuts reviewable, which a mid-build side effect would not
be: `pipeline/gauge_splits.json` is a diffable list of where the province's rivers get cut
and why.

    python -m pipeline.hydro.splits --build output/v2/full        # writes the artifact
    python -m pipeline.build ...                                  # the next build cuts there
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

# Cuts closer together than this on one blue line are collapsed to the first: two stations
# 40 m apart describe the same water, and a 40 m section is a rendering artifact rather
# than a reach anybody fishes.
MIN_GAP_M = 250.0

# A cut this close to an end of the blue line is dropped: it would leave a stub too short to
# see, and the station is already effectively on that boundary.
MIN_END_M = 150.0


def _measure_at(geom, point) -> float | None:
    """Route measure of the point on this node's own geometry, in metres from its start."""
    try:
        return float(geom.project(point))
    except Exception:                                     # noqa: BLE001
        return None


def gauge_splits(graph, geoms: dict, stations: list[dict],
                 node_for_station: dict[str, str]) -> list[dict]:
    """One cut per station, on the node the matcher resolved. Sorted, deterministic."""
    import geopandas as gpd
    from shapely.geometry import Point

    by_id = {s["station"]: s for s in stations}
    wanted = [(st, node_for_station[st]) for st in sorted(node_for_station)
              if st in by_id and node_for_station[st] in graph.nodes]
    if not wanted:
        return []

    pts = gpd.GeoSeries([Point(by_id[st]["lon"], by_id[st]["lat"]) for st, _ in wanted],
                        crs=4326).to_crs(3005)

    out: list[dict] = []
    for (station, node_id), pt in zip(wanted, pts):
        node = graph.nodes[node_id]
        # A LAKE STATION IS NOT A CUT. It reports a level for a body of water, not a
        # position along a channel — there is no "above it" and "below it" to separate.
        if str(getattr(node, "kind", "")).endswith("lake"):
            continue
        blk = getattr(node, "blk", "")
        geom = geoms.get(node_id)
        if not blk or geom is None or geom.is_empty:
            continue
        local = _measure_at(geom, pt)
        if local is None:
            continue
        # The node's own start measure plus how far along it the station sits: the cut has
        # to be expressed on the BLUE LINE, which is what the sectionizer subdivides.
        base = float(getattr(node, "down_m", 0.0) or 0.0)
        length = float(geom.length)
        if local < MIN_END_M or length - local < MIN_END_M:
            continue                        # a stub nobody could see, on a boundary already
        out.append({
            "split_id": f"gauge__{station}",
            "blk": str(blk),
            "route_measure": round(base + local, 1),
            "label": f"{station} · {by_id[station].get('name', '').title()}",
            "anchor_type": "gauge",
            "station": station,
            "node_id": node_id,
            "offset_m": round(float(geom.distance(pt)), 1),
        })

    # Collapse near-duplicates per blue line, keeping the lowest measure — deterministic,
    # and it is the downstream one, which is the reach a person is more likely to be on.
    out.sort(key=lambda r: (r["blk"], r["route_measure"], r["station"]))
    kept: list[dict] = []
    for row in out:
        if kept and kept[-1]["blk"] == row["blk"] \
                and row["route_measure"] - kept[-1]["route_measure"] < MIN_GAP_M:
            continue
        kept.append(row)
    return kept


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build", type=Path, default=Path("output/v2/full"),
                    help="a completed build directory (graph.pkl, geometries.pkl)")
    ap.add_argument("--stations", type=Path,
                    default=Path("data/bc_hydrometric_stations.json"))
    ap.add_argument("--out", type=Path,
                    default=Path(__file__).resolve().parents[1] / "gauge_splits.json")
    a = ap.parse_args()

    import pickle
    from pipeline.hydro.match import load_aliases, match_stations, nodes_for, summarise
    from pipeline.hydro.shed import load_stations

    graph = pickle.load((a.build / "graph.pkl").open("rb"))
    geoms = pickle.load((a.build / "geometries.pkl").open("rb"))
    stations = load_stations(a.stations)
    matches = match_stations(stations, geoms, graph, aliases=load_aliases())
    print(summarise(matches))

    rows = gauge_splits(graph, geoms, stations, nodes_for(matches))
    a.out.write_text(json.dumps({
        "_about": "Gauge positions as stream cuts. GENERATED by "
                  "`python -m pipeline.hydro.splits`; do not hand-edit. Read by "
                  "pipeline.build so the NEXT build sections rivers at their gauges — see "
                  "the module docstring for why this cannot be a build step.",
        "splits": rows,
    }, indent=2) + "\n", encoding="utf-8")
    blks = len({r["blk"] for r in rows})
    print(f"wrote {a.out}  ({len(rows)} cuts across {blks} blue lines)")


if __name__ == "__main__":
    main()
