"""Step 5 (03 S5, 04): apply CURATED splits to the graph — subdivide stream-piece nodes.

Lakes are already their own nodes (they split BLKs in the graph build). This step handles only
hand-authored splits, and it runs **before any tributary walk** so a curated section is a
first-class graph node: "tributaries of X between A and B" is then a walk over the A–B node.

Splitting a piece P at route measure M yields P_low = [P.down_m, M] (keeps P's node_id) and
P_high = [M, P.up_m] (new id "{blk}:{int(M)}"), joined by a **continuation** edge P_high→P_low.
Incoming tributary edges reattach by their confluence `at_measure` (< M → low, ≥ M → high). The
two bounds of each piece drive its `location_identifier` via the 04 table.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from typing import Optional

from shapely.ops import substring

from .models import FlowEdge, NodeKind, SplitPoint, StreamGraph


def _label(lower: str, upper: str) -> Optional[str]:
    """04 table: bound toward the mouth = 'downstream of', toward the source = 'upstream of'."""
    if not lower and not upper:
        return None
    if not lower:
        return f"downstream of {upper}"
    if not upper:
        return f"upstream of {lower}"
    return f"between {lower} and {upper}"


def _rebuild_adj(edges):
    up: dict[str, list[int]] = defaultdict(list)
    down: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(edges):
        up[e.to_node].append(i)
        down[e.from_node].append(i)
    return dict(up), dict(down)


def _find_piece(graph: StreamGraph, blk: str, m: float) -> Optional[str]:
    for nid, n in graph.nodes.items():
        if n.kind == NodeKind.stream and n.blk == blk and n.down_m < m < n.up_m:
            return nid
    return None


def _split_one(graph: StreamGraph, geoms: dict, bounds: dict, blk: str, sp: SplitPoint) -> bool:
    M = sp.route_measure
    pid = _find_piece(graph, blk, M)
    if pid is None:
        return False                          # measure at a boundary / in a lake / off-blk
    hi_id = f"{blk}:{int(M)}"
    if hi_id == pid or hi_id in graph.nodes:
        return False                          # degenerate or id collision — skip
    P = graph.nodes[pid]
    lo_prev, hi_prev = bounds.get(pid, ("", ""))
    label = sp.label or sp.split_id

    cx = cy = 0.0
    g = geoms.get(pid)
    if g is not None and not g.is_empty:
        a = min(max(M - P.down_m, 0.0), g.length)
        pt = g.interpolate(a)
        cx, cy = pt.x, pt.y
        geoms[pid] = substring(g, 0.0, a)
        geoms[hi_id] = substring(g, a, g.length)

    graph.nodes[pid] = replace(P, up_m=M, length_m=max(M - P.down_m, 0.0))
    graph.nodes[hi_id] = replace(P, node_id=hi_id, down_m=M, up_m=P.up_m,
                                 length_m=max(P.up_m - M, 0.0))
    bounds[pid] = (lo_prev, label)
    bounds[hi_id] = (label, hi_prev)

    for i, e in enumerate(graph.edges):
        if e.to_node == pid and e.at_measure >= M:
            graph.edges[i] = replace(e, to_node=hi_id)   # tributary above the cut moves up
    graph.edges.append(FlowEdge(from_node=hi_id, to_node=pid, at_measure=M,
                                x=cx, y=cy, kind="continuation"))
    return True


def split_graph_at(graph: StreamGraph, geoms: dict,
                   split_points: list[SplitPoint]) -> StreamGraph:
    """Subdivide piece nodes at curated ``split_points`` (in place) and set location_identifier.

    Splits on one BLK are applied mouth→source so each lands in the current top piece. Returns
    the same graph (mutated) with rebuilt adjacency.
    """
    bounds: dict[str, tuple[str, str]] = {}
    by_blk: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in split_points:
        by_blk[sp.blk].append(sp)
    applied = 0
    for blk, sps in by_blk.items():
        for sp in sorted(sps, key=lambda s: s.route_measure):
            applied += _split_one(graph, geoms, bounds, blk, sp)

    for nid, (lo, hi) in bounds.items():
        if nid in graph.nodes:
            graph.nodes[nid] = replace(graph.nodes[nid], location_identifier=_label(lo, hi))

    graph.up_adj, graph.down_adj = _rebuild_adj(graph.edges)
    return graph
