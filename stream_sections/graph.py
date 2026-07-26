"""Build the single INVERTED stream graph (03 S3): nodes = streams/lakes, edges = flows-into.

Two node kinds:
  - stream piece : a BLK cut at its contiguous lake/manmade ``wbk`` runs (and, later, curated
                   splits). A mainstem that crosses no lake stays ONE node no matter how many
                   tributaries join it. node_id = ``"{blk}:{int(down_m)}"``.
  - lake         : ONE node per lake/manmade ``wbk`` (wetlands are NOT noded). It collects every
                   fid carrying that wbk (the through-river's under-lake run plus tributary
                   mouths dipping into the lake). node_id = ``"lake:{wbk}"``.

Edges are built from fid endpoint incidence: at a coordinate C, every owner with a fid whose
DOWNSTREAM end is C (i.e. it sits above C) flows into the owner whose UPSTREAM end is C (sits
below C). Same-owner boundaries (a piece's internal fid joints, and a mainstem spanning a
confluence) produce no edge, so a mainstem is one node; a tributary/lake boundary produces one.
A lake node's incoming edges are its inlets, its outgoing edges its outlet(s).

Guards baked in here (spike 10): the WSC-descendant filter drops braiding-reversed and
cross-watershed stream->stream edges at creation; distinct EDGE_TYPEs are preserved per node so
a 2300 connector is ``is_barrier`` and the guarded ancestor walk stops at it.

Geometry is kept OUT of the graph (``build_section_geometries`` returns a node_id -> geometry
sidecar). fids appear only inside a node as provenance, never as graph structure.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional

from . import cutting
from .blk_chains import FidRow
from .models import BlkChain, FlowEdge, NameSource, NameTuple, NodeKind, StreamGraph, StreamNode


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


def _max_opt(a: Optional[int], b: Optional[int]) -> Optional[int]:
    vals = [v for v in (a, b) if v is not None]
    return max(vals) if vals else None


def _lake_owner(wbk: str) -> str:
    return f"lake:{wbk}"


def _assign_owners(fid_rows: list[FidRow], lake_kind: dict[str, str]):
    """Assign every fid to an owner node id and group fids by owner.

    Walking each BLK mouth->source, a contiguous run of NON-lake fids is one stream piece
    (id ``"{blk}:{int(first_down_m)}"``); a fid whose ``wbk`` is a lake/manmade wbk belongs to
    that lake node and BREAKS the current piece (so the fids on each side of a lake become
    separate pieces). Returns (owner_of_fid, piece_fids, lake_fids).
    """
    by_blk: dict[str, list[FidRow]] = defaultdict(list)
    for r in fid_rows:
        by_blk[r.blk].append(r)

    owner_of_fid: dict[str, str] = {}
    piece_fids: dict[str, list[FidRow]] = defaultdict(list)
    lake_fids: dict[str, list[FidRow]] = defaultdict(list)

    for blk, group in by_blk.items():
        group.sort(key=lambda r: r.down_m)
        cur_piece: Optional[str] = None
        for r in group:
            w = str(r.wbk) if r.wbk else ""
            if w in lake_kind:
                owner = _lake_owner(w)
                lake_fids[owner].append(r)
                cur_piece = None                      # a lake breaks the stream run
            else:
                if cur_piece is None:
                    cur_piece = f"{blk}:{int(r.down_m)}"
                owner = cur_piece
                piece_fids[owner].append(r)
            owner_of_fid[r.fid] = owner

    return owner_of_fid, piece_fids, lake_fids


def _edge_types(frs: list[FidRow]) -> tuple[str, ...]:
    return tuple(sorted({r.edge_type for r in frs if r.edge_type}))


def build_stream_graph(chains: list[BlkChain], fid_rows: list[FidRow],
                       lake_kind: Optional[dict[str, str]] = None,
                       lake_names: Optional[dict[str, str]] = None,
                       apply_wsc_filter: bool = True) -> StreamGraph:
    lake_kind = lake_kind or {}
    lake_names = lake_names or {}
    chain_by_blk = {c.blk: c for c in chains}
    owner_of_fid, piece_fids, lake_fids = _assign_owners(fid_rows, lake_kind)

    nodes: dict[str, StreamNode] = {}

    # --- stream piece nodes (name/gnis inherited from the whole BLK chain) ---
    for owner, frs in piece_fids.items():
        frs.sort(key=lambda r: r.down_m)
        blk = frs[0].blk
        c = chain_by_blk.get(blk)
        name_tuples = c.name_tuples if c else ()
        order = mag = None
        for r in frs:
            order = _max_opt(order, r.stream_order)
            mag = _max_opt(mag, r.stream_magnitude)
        nodes[owner] = StreamNode(
            node_id=owner, kind=NodeKind.stream, blk=blk, wsc=frs[0].wsc,
            gnis_id=(c.gnis_id if c else ""),
            display_name=name_tuples[0].name if name_tuples else "",
            name_tuples=name_tuples,
            down_m=frs[0].down_m, up_m=frs[-1].up_m, length_m=frs[-1].up_m - frs[0].down_m,
            stream_order=order, stream_magnitude=mag,
            member_fids=tuple(r.fid for r in frs), edge_types=_edge_types(frs),
        )

    # --- lake nodes (one per wbk; name = layer GNIS, else a threading river) ---
    for owner, frs in lake_fids.items():
        wbk = owner.split(":", 1)[1]
        through = tuple(sorted({r.gnis_name for r in frs if r.gnis_name}))
        lake_name = lake_names.get(wbk, "")
        display = lake_name or (through[0] if through else "")
        name_tuples = (NameTuple(lake_name, NameSource.gazette),) if lake_name else ()
        order = mag = None
        for r in frs:
            order = _max_opt(order, r.stream_order)
            mag = _max_opt(mag, r.stream_magnitude)
        nodes[owner] = StreamNode(
            node_id=owner, kind=NodeKind.lake, wbk=wbk, wsc=frs[0].wsc,
            display_name=display, name_tuples=name_tuples, through_names=through,
            stream_order=order, stream_magnitude=mag,
            member_fids=tuple(r.fid for r in frs), edge_types=_edge_types(frs),
        )

    # --- edges from endpoint incidence ---
    # At coord C: owners with a fid whose DOWN end is C sit ABOVE C; the owner whose UP end is C
    # sits BELOW C. Flow crosses C from each above-owner into each below-owner.
    above_at: dict[str, set[str]] = defaultdict(set)          # C -> owners above C
    below_at: dict[str, list[tuple[str, float]]] = defaultdict(list)  # C -> (owner below C, measure at C)
    for r in fid_rows:
        o = owner_of_fid[r.fid]
        above_at[r.down_node].add(o)
        below_at[r.up_node].append((o, r.up_m))

    edges: list[FlowEdge] = []
    seen: set[tuple[str, str]] = set()
    for c_node, belows in below_at.items():
        aboves = above_at.get(c_node, ())
        for d_owner, at_m in belows:
            for u_owner in aboves:
                if u_owner == d_owner or (u_owner, d_owner) in seen:
                    continue
                un, dn = nodes[u_owner], nodes[d_owner]
                # WSC-descendant filter on stream->stream only (lakes are legitimate junctions).
                if (apply_wsc_filter and un.kind == NodeKind.stream and dn.kind == NodeKind.stream
                        and not _wsc_descendant(un.wsc, dn.wsc)):
                    continue
                seen.add((u_owner, d_owner))
                kind = ("lake_out" if un.kind == NodeKind.lake else
                        "lake_in" if dn.kind == NodeKind.lake else "confluence")
                x, y = _coord(c_node)
                edges.append(FlowEdge(from_node=u_owner, to_node=d_owner, at_measure=at_m,
                                      x=x, y=y, kind=kind))

    edges.sort(key=lambda e: (e.from_node, e.to_node))   # deterministic order
    up_adj: dict[str, list[int]] = defaultdict(list)
    down_adj: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(edges):
        up_adj[e.to_node].append(i)
        down_adj[e.from_node].append(i)

    return StreamGraph(nodes=nodes, edges=edges, up_adj=dict(up_adj), down_adj=dict(down_adj))


def build_section_geometries(chains: list[BlkChain], fid_rows: list[FidRow],
                             lake_kind: Optional[dict[str, str]] = None) -> dict:
    """node_id -> geometry sidecar (kept OUT of the graph). Stream pieces are a route-measure
    ``substring`` of the merged BLK line; lake nodes are the stitched under-lake fid lines."""
    lake_kind = lake_kind or {}
    chain_by_blk = {c.blk: c for c in chains}
    _owner, piece_fids, lake_fids = _assign_owners(fid_rows, lake_kind)

    geoms: dict = {}
    for owner, frs in piece_fids.items():
        c = chain_by_blk.get(frs[0].blk)
        if c is None or c.geometry is None:
            continue
        frs.sort(key=lambda r: r.down_m)
        geoms[owner] = cutting.substring_cut(c.geometry, c.mouth_measure,
                                             frs[0].down_m, frs[-1].up_m)
    for owner, frs in lake_fids.items():
        frs.sort(key=lambda r: r.down_m)
        geoms[owner] = cutting.merge_ordered([r.geometry for r in frs])
    return geoms


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
