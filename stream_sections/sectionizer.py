"""Step 5 (03 S5, 04): apply CURATED splits to the graph — subdivide stream-piece nodes.

Lakes are already their own nodes (they split BLKs in the graph build). This step handles only
hand-authored splits, and it runs **before any tributary walk** so a curated section is a
first-class graph node: "tributaries of X between A and B" is then a walk over the A–B node.

Splitting a piece P at route measure M yields P_low = [P.down_m, M] (keeps P's node_id) and
P_high = [M, P.up_m] (new id "{blk}:{int(M)}"), joined by a **continuation** edge P_high→P_low.
Incoming tributary edges reattach by their confluence `at_measure` (< M → low, ≥ M → high). Each
side inherits the other bound of P and gains a structured `SectionBoundary` at M, so
`location_identifier` and range regulations both work. Per-piece order/magnitude and member_fids
are re-partitioned by measure so the front-end line weight of each piece is correct.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from typing import Optional

from shapely.ops import substring

from .models import (BoundaryKind, FlowEdge, NodeKind, SectionBoundary, SplitPoint,
                     StreamGraph)

_ANCHOR_KIND = {"point": BoundaryKind.split, "line": BoundaryKind.split,
                "confluence": BoundaryKind.confluence, "mu_boundary": BoundaryKind.mu,
                "lake": BoundaryKind.lake, "border": BoundaryKind.border,
                "area_boundary": BoundaryKind.area}


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


def _repartition(member_fids, fid_index, lo, hi):
    """(order, magnitude, member_fids) restricted to fids overlapping measure range [lo, hi]."""
    if not fid_index:
        return None, None, member_fids
    order = mag = None
    kept = []
    for f in member_fids:
        info = fid_index.get(f)
        if info is None:
            kept.append(f)
            continue
        fd, fu, fo, fm = info
        if fd < hi and fu > lo:                       # overlaps the range
            kept.append(f)
            order = fo if order is None else (max(order, fo) if fo is not None else order)
            mag = fm if mag is None else (max(mag, fm) if fm is not None else mag)
    return order, mag, tuple(kept)


def _blk_extent(graph, blk):
    ds = [n.down_m for n in graph.nodes.values() if n.kind == NodeKind.stream and n.blk == blk]
    us = [n.up_m for n in graph.nodes.values() if n.kind == NodeKind.stream and n.blk == blk]
    return (min(ds), max(us)) if ds else (None, None)


def _pickup(graph, blk, sp) -> bool:
    """Proximity pickup (docs/04): if an existing INTERIOR boundary on ``blk`` (a lake edge, a
    border split, or an earlier curated cut) sits within ``sp.proximity_m`` of this split's
    measure, RELABEL it with this split instead of cutting a near-duplicate. Returns True if it
    picked up an existing boundary (so the caller skips the cut). Natural mouth/source ends are
    excluded — a pickup only ever reuses a real interior boundary."""
    lo_ext, hi_ext = _blk_extent(graph, blk)
    if lo_ext is None:
        return False
    M = sp.route_measure
    best_m, best_d = None, (sp.proximity_m or 100.0)
    for n in graph.nodes.values():
        if n.kind != NodeKind.stream or n.blk != blk:
            continue
        for m in (n.down_m, n.up_m):
            if m <= lo_ext or m >= hi_ext:      # skip the blk's natural ends
                continue
            d = abs(m - M)
            if d <= best_d:
                best_m, best_d = m, d
    if best_m is None:
        return False
    bnd = SectionBoundary(boundary_id=f"split:{sp.split_id}",
                          kind=_ANCHOR_KIND.get(sp.anchor_type.value, BoundaryKind.split),
                          route_measure=best_m, label=(sp.label or sp.split_id))
    for nid, n in list(graph.nodes.items()):
        if n.kind != NodeKind.stream or n.blk != blk:
            continue
        if abs(n.up_m - best_m) < 1e-6:
            graph.nodes[nid] = replace(n, upper_bound=bnd)
        if abs(n.down_m - best_m) < 1e-6:
            graph.nodes[nid] = replace(n, lower_bound=bnd)
    return True


def _split_one(graph, geoms, blk, sp, fid_index) -> bool:
    M = sp.route_measure
    pid = _find_piece(graph, blk, M)
    if pid is None:
        return False                          # measure at a boundary / in a lake / off-blk
    hi_id = f"{blk}:{int(M)}"
    if hi_id == pid or hi_id in graph.nodes:
        return False                          # degenerate or id collision — skip
    P = graph.nodes[pid]
    bnd = SectionBoundary(boundary_id=f"split:{sp.split_id}",
                          kind=_ANCHOR_KIND.get(sp.anchor_type.value, BoundaryKind.split),
                          route_measure=M, label=(sp.label or sp.split_id))

    cx = cy = 0.0
    g = geoms.get(pid)
    if g is not None and not g.is_empty:
        a = min(max(M - P.down_m, 0.0), g.length)
        pt = g.interpolate(a)
        cx, cy = pt.x, pt.y
        geoms[pid] = substring(g, 0.0, a)
        geoms[hi_id] = substring(g, a, g.length)

    lo_ord, lo_mag, lo_fids = _repartition(P.member_fids, fid_index, P.down_m, M)
    hi_ord, hi_mag, hi_fids = _repartition(P.member_fids, fid_index, M, P.up_m)
    graph.nodes[pid] = replace(P, up_m=M, length_m=max(M - P.down_m, 0.0), upper_bound=bnd,
                               stream_order=lo_ord if fid_index else P.stream_order,
                               stream_magnitude=lo_mag if fid_index else P.stream_magnitude,
                               member_fids=lo_fids)
    graph.nodes[hi_id] = replace(P, node_id=hi_id, down_m=M, up_m=P.up_m,
                                 length_m=max(P.up_m - M, 0.0), lower_bound=bnd,
                                 upper_bound=P.upper_bound,
                                 stream_order=hi_ord if fid_index else P.stream_order,
                                 stream_magnitude=hi_mag if fid_index else P.stream_magnitude,
                                 member_fids=hi_fids)

    for i, e in enumerate(graph.edges):
        if e.to_node == pid and e.at_measure >= M:
            graph.edges[i] = replace(e, to_node=hi_id)   # tributary above the cut moves up
    graph.edges.append(FlowEdge(from_node=hi_id, to_node=pid, at_measure=M,
                                x=cx, y=cy, kind="continuation"))
    return True


def split_graph_at(graph: StreamGraph, geoms: dict, split_points: list[SplitPoint],
                   fid_index: Optional[dict] = None, proximity_pickup: bool = False,
                   applied: Optional[list] = None) -> StreamGraph:
    """Subdivide piece nodes at curated ``split_points`` (in place); rebuild adjacency.

    Splits on one BLK are applied mouth→source so each lands in the current top piece.
    ``fid_index`` (fid -> (down_m, up_m, order, magnitude)) enables per-piece order/magnitude
    re-partitioning; omit it (synthetic tests) to keep the parent piece's values.
    ``proximity_pickup``: reuse an existing interior boundary (lake/border/earlier cut) within
    ``proximity_m`` instead of cutting a near-duplicate — how the Kootenay "Idaho border" and
    "Koocanusa Reservoir" curated points snap onto the auto border splits (docs/04).
    ``applied`` (optional): each SplitPoint that landed is appended, with ``picked_up`` set — the
    record the gpkg ``split_points`` layer + ``splits.resolved.json`` consume.
    """
    from dataclasses import replace as _replace
    by_blk: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in split_points:
        by_blk[sp.blk].append(sp)
    for blk, sps in by_blk.items():
        for sp in sorted(sps, key=lambda s: s.route_measure):
            picked = _pickup(graph, blk, sp) if proximity_pickup else False
            if not picked:
                _split_one(graph, geoms, blk, sp, fid_index)
            if applied is not None:
                applied.append(_replace(sp, picked_up=picked))
    graph.up_adj, graph.down_adj = _rebuild_adj(graph.edges)
    return graph
