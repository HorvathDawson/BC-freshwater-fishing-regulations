"""Read-only, ONE load of the live atlas graph -> a small per-section table in the scratchpad.

Loads data/generated/atlas/full/graph.pkl once (single process, ~6 GB peak; not a build), fits the same
AreaModel the bundle used, and writes for every node: sid, kind, blk, wsc, name, magnitude, edge types, model
area, up/down neighbours — plus `mainstem_of` exactly as the bundle ran it. Every other script here reads that
table instead of the graph.

    .venv/bin/python handoff/PLAN/app-data/side-channels/scripts/extract_graph.py OUT.pkl
"""
import pickle
import sys
import time
from pathlib import Path

sys.path.insert(0, ".")
from pipeline.atlas.gauges.build_panels import _is_side_channel, _kind, mainstem_of  # noqa: E402
from pipeline.common.section_handles import read as read_handles  # noqa: E402
from pipeline.deliver.bundle.build import _area_model  # noqa: E402

# The MAIN checkout's live data (a worktree has no data/); read-only.
MAIN = Path("/Users/dawson.horvath/dev/personal/BC-freshwater-fishing-regulations")
ATLAS = MAIN / "data/generated/atlas/full"


def main(out: str) -> None:
    t = time.time()
    with (ATLAS / "graph.pkl").open("rb") as fh:
        graph = pickle.load(fh)
    print(f"graph loaded {time.time() - t:.0f}s: {len(graph.nodes):,} nodes, {len(graph.edges):,} edges")
    model = _area_model(MAIN / "data/source/bc_fisheries_data.gpkg")

    def area_of(n):
        return model.area_km2(getattr(n, "stream_magnitude", None), getattr(n, "wsc", ""))

    _ids, sid_of = read_handles(ATLAS)
    nodes = {}
    for nid, n in graph.nodes.items():
        nodes[nid] = dict(sid=sid_of.get(nid), kind=_kind(n), blk=n.blk, wsc=n.wsc, name=n.display_name,
                          gnis=n.gnis_id, mag=n.stream_magnitude, et=tuple(n.edge_types or ()),
                          side=_is_side_channel(n), area=area_of(n), length=n.length_m)
    edges = [(e.from_node, e.to_node) for e in graph.edges]
    parent = mainstem_of(graph, area_of)
    print(f"{sum(1 for v in nodes.values() if v['side'] and v['kind'] == 'stream'):,} side-channel stream "
          f"sections; mainstem_of adopted {len(parent):,}")
    with open(out, "wb") as fh:
        pickle.dump(dict(nodes=nodes, edges=edges, parent=parent), fh, protocol=pickle.HIGHEST_PROTOCOL)
    print(f"wrote {out} in {time.time() - t:.0f}s")


if __name__ == "__main__":
    main(sys.argv[1])
