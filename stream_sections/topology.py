"""Step 3-4 (03 S3-4): build the contracted topology graph from stream fids.

1. Map every endpoint that touches an under-lake fid to a single lake node ("lake:{wbk}").
2. Contract each BLK's open-channel fids into maximal Segments between SIGNIFICANT nodes
   (lake nodes, confluences, ends). Also split at EDGE_TYPE transitions so each Segment is
   homogeneous — this preserves the 2300 barrier at segment granularity.
3. Emit Topology(nodes, segments, down_adj, up_adj).

Splits (04) are NOT flow barriers and do NOT appear here — they are section boundaries only,
applied downstream in the sections step. Lakes are the only structural barrier (is_barrier).

Edges are directed downstream (from_node=upstream, to_node=downstream); up_adj is the
tributary (upstream) walk. FWA is already a DAG so there is no SCC condensation (spike 10).
"""

from __future__ import annotations

from typing import Optional

from . import cutting
from .blk_chains import FidRow
from .models import NodeKind, Segment, Topology, TopologyNode


def _lake_endpoint_map(fid_rows: list[FidRow], lake_wbk_kind: dict[str, str]) -> dict[str, str]:
    """Coordinate node id -> lake node id, for every endpoint touching an under-lake fid."""
    mapping: dict[str, str] = {}
    for r in fid_rows:
        if r.wbk in lake_wbk_kind:
            lake_node = f"lake:{r.wbk}"
            mapping.setdefault(r.down_node, lake_node)
            mapping.setdefault(r.up_node, lake_node)
    return mapping


def _max_opt(a: Optional[int], b: Optional[int]) -> Optional[int]:
    vals = [v for v in (a, b) if v is not None]
    return max(vals) if vals else None


def build_topology(fid_rows: list[FidRow], lake_wbk_kind: dict[str, str]) -> Topology:
    lake_map = _lake_endpoint_map(fid_rows, lake_wbk_kind)

    def node_of(coord_node: str) -> str:
        return lake_map.get(coord_node, coord_node)

    # Open-channel fids only (under-lake fids are absorbed into the lake node).
    open_rows = [r for r in fid_rows if r.wbk not in lake_wbk_kind]

    # Node significance: incident distinct-fid endpoint count and set of blks.
    fid_count: dict[str, int] = {}
    node_blks: dict[str, set[str]] = {}
    for r in open_rows:
        for n in (node_of(r.down_node), node_of(r.up_node)):
            fid_count[n] = fid_count.get(n, 0) + 1
            node_blks.setdefault(n, set()).add(r.blk)

    def significant(n: str) -> bool:
        return n.startswith("lake:") or fid_count.get(n, 0) != 2 or len(node_blks.get(n, ())) > 1

    # Group open fids per BLK, ordered mouth->source.
    by_blk: dict[str, list[FidRow]] = {}
    for r in open_rows:
        by_blk.setdefault(r.blk, []).append(r)

    segments: dict[str, Segment] = {}
    for blk, group in by_blk.items():
        group.sort(key=lambda r: r.down_m)
        # Split the ordered fid list into runs at significant boundary nodes / edge_type changes.
        runs: list[list[FidRow]] = []
        run: list[FidRow] = []
        for r in group:
            if run:
                boundary = node_of(r.down_node)  # == node_of(prev.up_node) when contiguous
                if significant(boundary) or run[-1].edge_type != r.edge_type \
                        or node_of(run[-1].up_node) != boundary:
                    runs.append(run)
                    run = []
            run.append(r)
        if run:
            runs.append(run)

        for members in runs:
            to_node = node_of(members[0].down_node)    # downstream (mouth) boundary
            from_node = node_of(members[-1].up_node)    # upstream (source) boundary
            down_m, up_m = members[0].down_m, members[-1].up_m
            seg_id = f"{blk}|{int(round(down_m))}|{int(round(up_m))}"  # unique per run
            order = magnitude = None
            for m in members:
                order = _max_opt(order, m.stream_order)
                magnitude = _max_opt(magnitude, m.stream_magnitude)
            segments[seg_id] = Segment(
                segment_id=seg_id, blk=blk, wsc=members[0].wsc,
                from_node=from_node, to_node=to_node,
                down_m=down_m, up_m=up_m,
                member_fids=tuple(m.fid for m in members),
                gnis_id=next((m.gnis_id for m in members if m.gnis_id), ""),
                stream_order=order, stream_magnitude=magnitude,
                edge_type=members[0].edge_type,
                geometry=cutting.merge_ordered([m.geometry for m in members]),
            )

    # Adjacency + degrees.
    down_adj: dict[str, list[str]] = {}
    up_adj: dict[str, list[str]] = {}
    indeg: dict[str, int] = {}
    outdeg: dict[str, int] = {}
    for seg in segments.values():
        down_adj.setdefault(seg.from_node, []).append(seg.segment_id)
        up_adj.setdefault(seg.to_node, []).append(seg.segment_id)
        outdeg[seg.from_node] = outdeg.get(seg.from_node, 0) + 1  # flows downstream out of from_node
        indeg[seg.to_node] = indeg.get(seg.to_node, 0) + 1

    nodes: dict[str, TopologyNode] = {}
    all_node_ids = set(down_adj) | set(up_adj)
    for nid in all_node_ids:
        if nid.startswith("lake:"):
            kind = NodeKind.lake
            is_barrier = True
            wbk = nid.split(":", 1)[1]
        else:
            is_barrier = False
            wbk = ""
            has_down = outdeg.get(nid, 0) > 0   # something flows downstream from here
            has_up = indeg.get(nid, 0) > 0      # something flows into here from upstream
            if not has_down:
                kind = NodeKind.outlet          # nothing leaves downstream -> mouth/outlet
            elif not has_up:
                kind = NodeKind.headwater       # nothing arrives from upstream -> source
            else:
                kind = NodeKind.confluence
        x = y = None
        if not nid.startswith("lake:") and "_" in nid:
            try:
                xs, ys = nid.split("_", 1)
                x, y = float(xs), float(ys)
            except ValueError:
                x = y = None
        nodes[nid] = TopologyNode(node_id=nid, kind=kind, x=x, y=y, wbk=wbk, is_barrier=is_barrier)

    return Topology(nodes=nodes, segments=segments, down_adj=down_adj, up_adj=up_adj)
