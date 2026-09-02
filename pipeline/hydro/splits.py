"""Gauge positions as SPLIT DEFINITIONS — the same kind of object a curator authors.

THE PROBLEM, IN ONE RIVER. The Fraser mainstem is 20 graph nodes for 1,375 km, and 18
transmitting stations sit on it. Every section takes the ONE station that most nearly is
that water, so a single node — hundreds of kilometres of river — paints one colour from one
gauge, and the reading at Hope is claimed for water at Lillooet. That is not a ranking
problem. It is a LENGTH problem: a gauge is the boundary between two different
measurements, so the river has to change section where the measurement changes.

WHAT THIS EMITS, AND WHY IT IS NOT A CUT.

    A station becomes a `point` anchor — its own published coordinate — scoped to the water
    the matcher resolved it to, by WSC. That is byte-for-byte the shape of a hand-authored
    split in `pipeline/splits.json`, so the build resolves it through
    `pipeline.splits.anchors.resolve_split_defs` with everything else. One resolver, one
    code path, one set of rules.

    The earlier version computed a route measure here and handed the build a finished cut.
    It worked and it was wrong-shaped: it reimplemented projection, missed the perpendicular
    sweep that catches braids, could not carry an offset or a concern, and appeared in
    neither `splits.resolved.json` nor the gpkg — so 379 cuts a curator could not review.
    Everything below the identifier is the resolver's job.

WHY WSC AND NOT BLK. A gauge on a braided reach measures the whole channel, not the strand
its coordinate happens to land on. `wsc` scopes to the river AND its side channels, and the
point anchor then sweeps a perpendicular across them — so a braid is cut where it sits along
the valley rather than by how near each strand is to the station. `blk` is the fallback for
a node with no watershed code.

WHY IT IS FROZEN. Matching a station to a WATER needs the graph — the names it compares
against include `name_variants.json`, which is applied during the build — and the build
needs its splits. So this runs against a COMPLETED build and commits the answer, exactly as
`added_streams` does. The consequence is worth stating plainly: after a station moves or a
new one starts reporting, the cuts are one build behind until this is re-run.

    python -m pipeline.hydro.splits --build output/v2/full     # writes the artifact
    python -m pipeline.build ...                               # the next build cuts there
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

DEFAULT_OUT = Path(__file__).resolve().parents[1] / "gauge_splits.json"

# How far the resolver may look from the station's coordinate for a channel to cut. A gauge
# is often on a bank or a bridge while the FWA line is the channel centre, and on a braided
# reach the far strand can be a few hundred metres off. Wide enough for that, narrow enough
# that it cannot reach the next river over — and the WSC scope is doing the real work.
PROXIMITY_M = 400.0


def split_defs(graph, stations: list[dict], node_for_station: dict[str, str]) -> list[dict]:
    """`SplitDef` dicts — one per station, scoped to the water the matcher resolved.

    Deterministic: sorted by station id, which is also the split id, which is part of the
    section-id ABI. A reordering here would move boundaries between builds for no reason.
    """
    by_id = {s["station"]: s for s in stations}
    out: list[dict] = []
    for station in sorted(node_for_station):
        node_id = node_for_station[station]
        node = graph.nodes.get(node_id)
        s = by_id.get(station)
        if node is None or s is None:
            continue
        # A LAKE STATION IS NOT A CUT. It reports a level for a body of water; there is no
        # "above it" and "below it" along a channel to separate. Lake gauges are linked to
        # the lake itself by `pipeline.hydro.shed.lake_gauge_links`, which is the question
        # they can actually answer.
        if str(getattr(node, "kind", "")).endswith("lake"):
            continue
        lon, lat = s.get("lon"), s.get("lat")
        if lon is None or lat is None:
            continue
        scope = ({"wsc": node.wsc} if getattr(node, "wsc", "")
                 else {"blk": node.blk} if getattr(node, "blk", "") else None)
        if scope is None:
            continue                      # nothing to scope the cut to; a bare point is not one
        out.append({
            "id": f"gauge__{station}",
            "label": f"{station} · {str(s.get('name', '')).title()}",
            "anchor": {"type": "gauge", "coord": [float(lon), float(lat)],
                       "is_lonlat": True},
            **scope,
            "proximity_m": PROXIMITY_M,
            # Review aids, ignored by the loader. `_node` records WHICH water the match
            # chose, which is the one judgement in this file worth a human's eye.
            "_node": node_id,
            "_name": node.display_name or "",
            "_station_name": s.get("name", ""),
        })
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--build", type=Path, default=Path("output/v2/full"),
                    help="a completed build directory (graph.pkl, geometries.pkl)")
    ap.add_argument("--stations", type=Path,
                    default=Path("data/bc_hydrometric_stations.json"))
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    ap.add_argument("--all-stations", action="store_true",
                    help="include discontinued stations (default: transmitting only)")
    a = ap.parse_args()

    import pickle
    from pipeline.hydro.match import load_aliases, match_stations, nodes_for, summarise
    from pipeline.hydro.shed import load_stations

    graph = pickle.load((a.build / "graph.pkl").open("rb"))
    geoms = pickle.load((a.build / "geometries.pkl").open("rb"))
    # TRANSMITTING STATIONS ONLY, by default. A station discontinued in 1974 still sits
    # somewhere, but cutting the river at it buys a boundary no reading will ever appear on.
    stations = [s for s in load_stations(a.stations)
                if a.all_stations or s.get("realtime")]
    matches = match_stations(stations, geoms, graph, aliases=load_aliases())
    print(summarise(matches))

    rows = split_defs(graph, stations, nodes_for(matches))
    a.out.write_text(json.dumps({
        "_about": "Hydrometric stations as split definitions — a `gauge` point anchor at "
                  "each station's published coordinate, scoped to the water the matcher "
                  "resolved it to. GENERATED by `python -m pipeline.hydro.splits` against a "
                  "COMPLETED build; do not hand-edit. Loaded by pipeline.build beside "
                  "splits.json and resolved by the same resolver.",
        "splits": rows,
    }, indent=2) + "\n", encoding="utf-8")
    scoped = sum(1 for r in rows if "wsc" in r)
    print(f"wrote {a.out}  ({len(rows)} station splits, {scoped} scoped by WSC, "
          f"{len(rows) - scoped} by BLK)")


if __name__ == "__main__":
    main()
