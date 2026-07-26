"""Build the single INVERTED stream graph (03 S3): nodes = streams, edges = flows-into.

Each BLK becomes ONE node (a mainstem stays one node no matter how many tributaries join —
no per-confluence segmentation). For each BLK we find, via fid incidence at its mouth, the
BLK it drains into and the confluence measure on that downstream BLK, and add an edge
``tributary -> mainstem``. Tributaries of a node = its ANCESTORS (walk ``up_adj``).

fids appear only inside StreamNode as provenance, never as graph structure. Lakes are not yet
their own nodes (they are internal to a BLK until the sectionizer splits BLKs at lakes) —
that refinement, plus hand splits, turns a BLK node into a chain of section nodes.
"""

from __future__ import annotations

from collections import defaultdict

from .blk_chains import FidRow
from .models import BlkChain, FlowEdge, NodeKind, StreamGraph, StreamNode


def _coord(node_id: str) -> tuple[float, float]:
    try:
        xs, ys = node_id.split("_", 1)
        return float(xs), float(ys)
    except ValueError:
        return (0.0, 0.0)


def _wsc_descendant(trib_wsc: str, main_wsc: str) -> bool:
    """True if ``trib_wsc`` is at/below ``main_wsc`` in the FWA hierarchy (prefix match).

    FWA watershed codes nest: a real tributary's code extends its mainstem's code. Equal codes
    (a side channel sharing the mainstem's WSC) also pass. Empty codes are permissive.
    """
    return not main_wsc or trib_wsc.startswith(main_wsc)


def build_stream_graph(chains: list[BlkChain], fid_rows: list[FidRow],
                       apply_wsc_filter: bool = True) -> StreamGraph:
    nodes: dict[str, StreamNode] = {}
    for c in chains:
        nodes[c.blk] = StreamNode(
            node_id=c.blk, kind=NodeKind.stream, blk=c.blk, wsc=c.fwa_watershed_code,
            gnis_id=c.gnis_id,
            display_name=c.name_tuples[0].name if c.name_tuples else "",
            name_tuples=c.name_tuples,
            down_m=c.mouth_measure, up_m=c.mouth_measure + c.length_m, length_m=c.length_m,
            stream_order=c.stream_order, stream_magnitude=c.stream_magnitude,
            member_fids=tuple(f.fid for f in c.fids), edge_types=c.edge_types,
            geometry=c.geometry,
        )

    # For each coordinate, the fids whose UPSTREAM end is there = the flow leaving that point
    # downstream. A tributary X's mouth node M has the mainstem's downstream fid leaving M;
    # that fid's blk is what X drains into, and its up-measure is the confluence position.
    out_at: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for r in fid_rows:
        out_at[r.up_node].append((r.blk, r.up_m))

    # Mouth node of each BLK = the down_node of its lowest-down_m fid.
    mouth: dict[str, tuple[float, str]] = {}
    for r in fid_rows:
        cur = mouth.get(r.blk)
        if cur is None or r.down_m < cur[0]:
            mouth[r.blk] = (r.down_m, r.down_node)

    edges: list[FlowEdge] = []
    for blk, (_dm, mouth_node) in mouth.items():
        if blk not in nodes:
            continue
        candidates = [(b, m) for (b, m) in out_at.get(mouth_node, []) if b != blk and b in nodes]
        # WSC-descendant filter (spike S1): a tributary must sit at/below the mainstem it joins
        # in the FWA hierarchy. This drops braiding-induced reversed edges (Chehalis must not
        # look like a parent of the Harrison) and cross-watershed connector edges (the
        # Columbia/Kootenay canal drains into a different WSC branch) at CREATION time.
        if apply_wsc_filter:
            candidates = [(b, m) for (b, m) in candidates
                          if _wsc_descendant(nodes[blk].wsc, nodes[b].wsc)]
        if not candidates:
            continue  # root: drains to ocean/border, is severed by the filter, or clipped at a bbox
        # Normally one downstream mainstem; on a fork/distributary pick the largest.
        down_blk, at_m = max(candidates, key=lambda c: nodes[c[0]].stream_magnitude or 0)
        x, y = _coord(mouth_node)
        edges.append(FlowEdge(from_node=blk, to_node=down_blk, at_measure=at_m, x=x, y=y))

    up_adj: dict[str, list[int]] = defaultdict(list)
    down_adj: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(edges):
        up_adj[e.to_node].append(i)
        down_adj[e.from_node].append(i)

    return StreamGraph(nodes=nodes, edges=edges, up_adj=dict(up_adj), down_adj=dict(down_adj))


def ancestors(graph: StreamGraph, node_id: str, guarded: bool = True) -> set[str]:
    """All upstream tributary nodes of ``node_id`` (transitive). Cycle-tolerant via a visited set.

    The WSC-descendant filter is already baked into the edge set at build time, so the raw
    closure here is free of braiding/cross-watershed leaks. ``guarded`` (default) additionally
    stops at EDGE_TYPE=2300 barrier nodes (canals/artificial connectors, spike S2): a barrier
    node is not included and the walk does not pass through it. Set ``guarded=False`` for the
    raw connectivity closure (used to diff guarded-vs-raw in the gpkg export / tests).
    """
    seen: set[str] = set()
    stack = [node_id]
    while stack:
        n = stack.pop()
        for ei in graph.up_adj.get(n, []):
            src = graph.edges[ei].from_node
            if src in seen:
                continue
            if guarded and graph.nodes[src].is_barrier:
                continue  # stop at the canal: exclude it and everything above it
            seen.add(src)
            stack.append(src)
    return seen
