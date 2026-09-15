"""Step 5 (03 S5, 04): apply CURATED splits to the graph — subdivide stream-piece nodes.

Lakes are already their own nodes (they split BLKs in the graph build). This step handles only
hand-authored splits, and it runs **before any tributary walk** so a curated section is a
first-class graph node: "tributaries of X between A and B" is then a walk over the A–B node.

Splitting a piece P at route measure M yields P_low = [P.down_m, M] (keeps P's node_id) and
P_high = [M, P.up_m] (new id "{blk}:{int(M)}"), joined by a **continuation** edge P_high→P_low.
Incoming tributary edges reattach by their confluence `at_measure` (< M → low, ≥ M → high).
OUTGOING edges reattach too, but they cannot use `at_measure` — that is measured on the edge's
`to_node`, so for "mainstem flows into side channel" it is a measure along the SIDE CHANNEL and says
nothing about where the channel leaves P. They are placed by projecting the confluence coordinate the
edge carries onto P instead. Missing this pass stranded every outgoing edge on P_low (the piece that
keeps P's id, nearest the mouth): a braid leaving the river at 19,002 m was recorded as leaving the
0–11,988 m piece, so its two ends named pieces either side of a cut it is nowhere near, and no reach
containing it could be resolved. Each side inherits the other bound of P and gains a structured
`SectionBoundary` at M, so `location_identifier` and range regulations both work. Per-piece order/magnitude and member_fids
are re-partitioned by measure so the front-end line weight of each piece is correct.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import replace
from typing import Optional

from shapely.geometry import Point
from shapely.ops import substring

from pipeline.common.models import (BoundaryKind, FlowEdge, NodeKind, SectionBoundary, SplitPoint,
                     StreamGraph)

_ANCHOR_KIND = {"point": BoundaryKind.split, "line": BoundaryKind.split,
                "confluence": BoundaryKind.confluence, "mu_boundary": BoundaryKind.mu,
                "lake": BoundaryKind.lake, "border": BoundaryKind.border,
                "area_boundary": BoundaryKind.area}

# Route-measure radius for merge/pickup. Deliberately tiny: pickup should only reuse a boundary that
# is effectively COINCIDENT (a lake edge / border split the curated point sits on), never collapse two
# genuinely distinct curated cuts. Keep it decoupled from SplitDef.proximity_m — that value is the
# (much larger) tolerance for projecting a coord onto the blue line, not a merge distance. Using it
# here merged distinct cuts, e.g. a dam and its +100 m offset sibling.
_MERGE_PROXIMITY_M = 5.0
# SplitDef.proximity_m default (projection tolerance). A SplitPoint carrying exactly this value has an
# inherited default, not a deliberate pickup radius, so pickup ignores it and uses _MERGE_PROXIMITY_M.
_DEFAULT_PROJECTION_M = 500.0


def _rebuild_adj(edges):
    up: dict[str, list[int]] = defaultdict(list)
    down: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(edges):
        up[e.to_node].append(i)
        down[e.from_node].append(i)
    return dict(up), dict(down)


def _find_piece(graph: StreamGraph, blk: str, m: float, by_blk: Optional[dict] = None) -> Optional[str]:
    nids = by_blk.get(blk, ()) if by_blk is not None else graph.nodes
    for nid in nids:
        n = graph.nodes[nid]
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


def _blk_extent(graph, blk, by_blk=None):
    nids = by_blk.get(blk, ()) if by_blk is not None else graph.nodes
    ds = [graph.nodes[nid].down_m for nid in nids
          if graph.nodes[nid].kind == NodeKind.stream and graph.nodes[nid].blk == blk]
    us = [graph.nodes[nid].up_m for nid in nids
          if graph.nodes[nid].kind == NodeKind.stream and graph.nodes[nid].blk == blk]
    return (min(ds), max(us)) if ds else (None, None)


def _pickup(graph, blk, sp, by_blk=None) -> bool:
    """Proximity pickup (docs/04): if an existing INTERIOR boundary on ``blk`` (a lake edge, a
    border split, or an earlier curated cut) sits within the merge radius (``_MERGE_PROXIMITY_M``,
    or an explicitly authored ``sp.proximity_m``) of this split's measure, RELABEL it with this split
    instead of cutting a near-duplicate. Returns True if it
    picked up an existing boundary (so the caller skips the cut). A natural mouth or source end
    is reachable too, but only by a CURATED split and only where the end is bare or carries a
    lake edge — see the long comment below."""
    lo_ext, hi_ext = _blk_extent(graph, blk, by_blk)
    if lo_ext is None:
        return False
    M = sp.route_measure
    blk_nids = list(by_blk.get(blk, ())) if by_blk is not None else list(graph.nodes)
    # Merge radius: an explicitly authored pickup radius wins, else the tiny coincident-only default.
    # NOT SplitDef.proximity_m (that's the projection tolerance) — see _MERGE_PROXIMITY_M.
    radius = sp.proximity_m if 0.0 < sp.proximity_m < _DEFAULT_PROJECTION_M else _MERGE_PROXIMITY_M
    best_m, best_d = None, radius
    for nid in blk_nids:
        n = graph.nodes[nid]
        if n.kind != NodeKind.stream or n.blk != blk:
            continue
        for m, bnd in ((n.down_m, n.lower_bound), (n.up_m, n.upper_bound)):
            if m <= lo_ext or m >= hi_ext:
                # THE END OF THE LINE IS NOT NOTHING.
                #
                # The first version of this refused every natural end, on the reasoning that a
                # mouth is where the water stops being itself, so snapping a cut there turns
                # "downstream of X" into the whole river. That danger is real, but it is the
                # RADIUS that holds it off, not this test: a split only reaches an end it is
                # already within a few metres of, and at that distance "the whole river" and
                # "all but 58 m of it" are not two different rivers. What the refusal actually
                # produced was the sliver — a cut a stone's throw from the end, and a stretch
                # the page draws as 0 km with a name on both sides of it.
                #
                # Two ends may now be taken, for opposite reasons:
                #
                #   a LAKE EDGE, because it is a real named bound and rules bind to it. The Stamp
                #   River is the case: its blue line stops at 20,232 m, the Great Central Lake
                #   edge sits ON that end, and the dam at the lake's outlet resolves 0.5 m away —
                #   refused purely for being at the end, leaving two boundaries at one place.
                #
                #   a BARE END, because there is nothing there to lose. No boundary object means
                #   no id, no alias, nothing a rule could already be bound to — the pickup only
                #   moves the cut onto the end and names it. The Bella Coola is the case: its
                #   head is the confluence where the Talchako meets the Atnarko, FWA leaves no
                #   boundary there, and the curated Talchako split landed 58 m below it. That
                #   58 m became its own stretch, "From Talchako River -> Bella Coola River To the
                #   head", which is not a reach anybody wrote a regulation about.
                #
                # Any OTHER bound at an end is still refused — an end already claimed by another
                # curated split is a collision, not a pickup.
                #
                # AND ONLY A CURATED SPLIT MAY TAKE AN END AT ALL.
                #
                # NOT because a minted split is a lesser name — rules bind to twelve `gauge__`
                # stations across all seven catalogue files, and `parse_context` shows those ids
                # to the parser on purpose. The reason is narrower: a pickup RELABELS, and the
                # relabel can destroy a name something else resolves THROUGH. `gauge__08HB008`
                # sat half a metre from the Sproat Lake edge — inside any radius, so distance was
                # never going to catch it — and taking that edge cost `sproat_river__sproat_lake`
                # the boundary it resolves through, collapsing "No Fishing from Sproat Lake to the
                # Hwy 4 signs" to an empty reach. The displaced id IS carried forward as an alias;
                # it did not save the reach, because alias coverage leaks (docs/04).
                #
                # So this is a naming rule, not a geometric one, and the radius cannot stand in
                # for it. Two known gaps, both deliberate to leave until the alias path is sound:
                # this test runs only in the natural-end branch, so an auto split may still
                # relabel an INTERIOR lake edge; and it reads authorship off the id spelling,
                # which is a list that has already been wrong once — it belongs on SplitPoint,
                # set where each family is minted.
                if _auto_split(sp.split_id):
                    continue
                if bnd is not None and not str(bnd.boundary_id).startswith("lake:"):
                    continue
            d = abs(m - M)
            if d <= best_d:
                best_m, best_d = m, d
    if best_m is None:
        return False
    # Pickup RELABELS the boundary it reuses, so every id that already named this position has to be
    # carried forward as an alias or it is destroyed. The Region 2/3 MU boundary on the Fraser lands
    # at exactly the same measure as the Spuzzum Creek confluence — the region boundary follows the
    # creek — so whichever was cut second silently erased the other, and the rule bound to the loser
    # ("between Spuzzum Creek and Hell's Gate") resolved to nothing with no indication why.
    prior: set[str] = set()
    for nid in blk_nids:
        n = graph.nodes[nid]
        if n.kind != NodeKind.stream or n.blk != blk:
            continue
        for b in (n.lower_bound, n.upper_bound):
            if b is not None and abs(b.route_measure - best_m) < 1e-6:
                prior.add(b.boundary_id)
                prior.update(b.aliases or ())
    mine = f"split:{sp.split_id}"
    bnd = SectionBoundary(boundary_id=mine,
                          kind=_ANCHOR_KIND.get(sp.anchor_type.value, BoundaryKind.split),
                          route_measure=best_m, label=(sp.label or sp.split_id),
                          aliases=tuple(sorted(prior - {mine})))
    for nid in blk_nids:
        n = graph.nodes[nid]
        if n.kind != NodeKind.stream or n.blk != blk:
            continue
        if abs(n.up_m - best_m) < 1e-6:
            graph.nodes[nid] = replace(n, upper_bound=bnd)
        if abs(n.down_m - best_m) < 1e-6:
            graph.nodes[nid] = replace(n, lower_bound=bnd)
    return True


#: Split families the pipeline mints itself. They are positions, not names anybody binds to, so
#: they may never relabel a named boundary at a natural end — see `_pickup`.
_AUTO_SPLIT_PREFIXES = frozenset({"gauge", "area", "border", "length"})


def _auto_split(split_id: str) -> bool:
    """A split the pipeline minted itself, by either of the two spellings it uses.

    `gauge__08HB008` separates with a double underscore; `area:6`, `border:360512399:0` and
    `length:356559402:23614` separate with a colon. Testing only the first — which is what the
    first version of this did — let every area, border and length split through.
    """
    head = split_id.split("__")[0] if "__" in split_id else split_id.split(":")[0]
    return head in _AUTO_SPLIT_PREFIXES

_ALIAS_MAX_FROM_EDGE_M = 3000.0   # how far into the lake a split may sit and still mean its edge


def _alias_onto_lake(graph, blk, sp, by_blk=None) -> tuple[str, float] | None:
    """Record a curated split that could NOT be cut as an alias on the LAKE it landed in.

    A cut is refused when its measure falls where the blue line has no stream piece, and on this data
    that means one thing: a LAKE RUN. A dam, weir or "at the outlet" sign sits at the lake's edge and
    the authored coordinate projects a little way into the water, so the split lands in the gap
    between the piece below the lake and the piece above it. Duncan Dam, the Mitchell Lake dam and the
    Babine juvenile counting weir all do this, and all used to vanish, taking the binding of every
    rule that named them with them.

    Deliberately narrow. It fires ONLY when the measure is inside a gap whose two sides are the SAME
    lake boundary, and it attaches to whichever edge of that lake is nearer — never to a confluence, a
    border cut or another split that merely happens to be the closest thing on the blue line. Anything
    else is left un-aliased and reported, because a split silently attached to the wrong kind of
    boundary is a worse failure than one that is visibly missing.

    Returns (boundary_id, distance from the lake edge) or None."""
    want = f"split:{sp.split_id}"
    M = sp.route_measure
    nids = list(by_blk.get(blk, ())) if by_blk is not None else list(graph.nodes)
    pieces = sorted(
        (graph.nodes[n] for n in nids
         if graph.nodes.get(n) is not None
         and graph.nodes[n].kind == NodeKind.stream and graph.nodes[n].blk == blk),
        key=lambda n: n.down_m)
    for below, above in zip(pieces, pieces[1:]):
        if not (below.up_m <= M <= above.down_m):
            continue                                  # not in this gap
        lo_b, hi_b = below.upper_bound, above.lower_bound
        if lo_b is None or hi_b is None:
            return None
        if not (str(lo_b.boundary_id).startswith("lake:") and lo_b.boundary_id == hi_b.boundary_id):
            return None                               # a gap, but not a lake run — leave it alone
        # attach to the nearer edge: one lake has two bounds, and aliasing both would give the split
        # two measures and make every reach that uses it ambiguous
        target, node, end = ((lo_b, below, "upper_bound") if (M - below.up_m) <= (above.down_m - M)
                             else (hi_b, above, "lower_bound"))
        dist = abs(M - (target.route_measure if target.route_measure is not None else M))
        if dist > _ALIAS_MAX_FROM_EDGE_M:
            return None                               # deep inside the lake: not this edge
        if want in (target.aliases or ()):
            return target.boundary_id, dist
        merged = replace(target, aliases=tuple(target.aliases or ()) + (want,))
        for other in nids:                            # the bound is shared by the pieces it separates
            n = graph.nodes.get(other)
            if n is None:
                continue
            patch = {}
            for e in ("lower_bound", "upper_bound"):
                b = getattr(n, e)
                if b is not None and b.boundary_id == target.boundary_id \
                        and b.route_measure == target.route_measure:
                    patch[e] = merged
            if patch:
                graph.nodes[other] = replace(n, **patch)
        return target.boundary_id, dist
    return None


def _split_one(graph, geoms, blk, sp, fid_index, by_blk=None, edges_by_to=None,
               edges_by_from=None) -> bool:
    M = sp.route_measure
    pid = _find_piece(graph, blk, M, by_blk)
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
    if by_blk is not None:
        by_blk.setdefault(blk, []).append(hi_id)          # keep the blk index current for later cuts

    # Tributaries joining ABOVE the cut move to the upper piece. With an edge index we touch only the
    # edges INTO pid, not all ~2.15M edges (the border stage's O(splits x edges) killer).
    if edges_by_to is not None:
        into_pid = edges_by_to.get(pid, [])
        moved = [i for i in into_pid if graph.edges[i].at_measure >= M]
        for i in moved:
            graph.edges[i] = replace(graph.edges[i], to_node=hi_id)
        if moved:
            ms = set(moved)
            edges_by_to[pid] = [i for i in into_pid if i not in ms]
            edges_by_to.setdefault(hi_id, []).extend(moved)
        edges_by_to.setdefault(pid, []).append(len(graph.edges))   # the continuation edge (to_node=pid)
    else:
        for i, e in enumerate(graph.edges):
            if e.to_node == pid and e.at_measure >= M:
                graph.edges[i] = replace(e, to_node=hi_id)   # tributary above the cut moves up

    # Outgoing edges leaving ABOVE the cut move up too. `at_measure` is on the to_node and is no use
    # here, so the edge's confluence coordinate is projected onto P (`g`, still the PRE-split
    # geometry) to find where along P it actually leaves. An edge with no coordinate is left alone —
    # guessing would be worse than the known-imperfect placement.
    if g is not None and not g.is_empty:
        def _leaves_above(e) -> bool:
            if not e.x and not e.y:
                return False
            return P.down_m + g.project(Point(e.x, e.y)) >= M

        if edges_by_from is not None:
            from_pid = edges_by_from.get(pid, [])
            up = [i for i in from_pid if _leaves_above(graph.edges[i])]
            for i in up:
                graph.edges[i] = replace(graph.edges[i], from_node=hi_id)
            if up:
                us = set(up)
                edges_by_from[pid] = [i for i in from_pid if i not in us]
                edges_by_from.setdefault(hi_id, []).extend(up)
        else:
            for i, e in enumerate(graph.edges):
                if e.from_node == pid and _leaves_above(e):
                    graph.edges[i] = replace(e, from_node=hi_id)

    if edges_by_from is not None:
        edges_by_from.setdefault(hi_id, []).append(len(graph.edges))   # continuation (from_node=hi_id)
    graph.edges.append(FlowEdge(from_node=hi_id, to_node=pid, at_measure=M,
                                x=cx, y=cy, kind="continuation"))
    return True


def split_graph_at(graph: StreamGraph, geoms: dict, split_points: list[SplitPoint],
                   fid_index: Optional[dict] = None, proximity_pickup: bool = False,
                   applied: Optional[list] = None, aliased: Optional[list] = None) -> StreamGraph:
    """Subdivide piece nodes at curated ``split_points`` (in place); rebuild adjacency.

    Splits on one BLK are applied mouth→source so each lands in the current top piece.
    ``fid_index`` (fid -> (down_m, up_m, order, magnitude)) enables per-piece order/magnitude
    re-partitioning; omit it (synthetic tests) to keep the parent piece's values.
    ``proximity_pickup``: reuse an existing interior boundary (lake/border/earlier cut) within
    ``proximity_m`` instead of cutting a near-duplicate — how the Kootenay "Idaho border" and
    "Koocanusa Reservoir" curated points snap onto the auto border splits (docs/04).
    ``applied`` (optional): each SplitPoint that landed is appended, with ``picked_up`` set — the
    record the gpkg ``split_points`` layer + ``splits.resolved.json`` consume.
    ``aliased`` (optional): ``(split_id, boundary_id)`` for each split that could not be cut and was
    recorded as an ALIAS on an existing boundary instead — see ``_alias_onto_nearest``.
    """
    from dataclasses import replace as _replace
    # Indexes built ONCE (maintained incrementally by _split_one): node ids per blk, and edge ids per
    # to_node. They turn each split's piece-find + tributary-reattach from O(all nodes)+O(all edges)
    # into O(local) — the fix for the border stage's ~1600s split cost.
    node_by_blk: dict[str, list[str]] = defaultdict(list)
    for nid, n in graph.nodes.items():
        if n.kind == NodeKind.stream and n.blk:
            node_by_blk[n.blk].append(nid)
    edges_by_to: dict[str, list[int]] = defaultdict(list)
    edges_by_from: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(graph.edges):
        edges_by_to[e.to_node].append(i)
        edges_by_from[e.from_node].append(i)

    sp_by_blk: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in split_points:
        sp_by_blk[sp.blk].append(sp)
    for blk, sps in sp_by_blk.items():
        for sp in sorted(sps, key=lambda s: s.route_measure):
            picked = _pickup(graph, blk, sp, node_by_blk) if proximity_pickup else False
            cut = False
            if not picked:
                cut = _split_one(graph, geoms, blk, sp, fid_index, node_by_blk, edges_by_to,
                                 edges_by_from)
                if not cut and aliased is not None:
                    # No piece to cut. If it landed in a LAKE RUN, record it as another name for that
                    # lake's boundary instead of dropping it silently; anything else stays missing.
                    # Only for a caller that asked (`aliased` given) — the CURATED pass, whose split
                    # ids rules actually bind. The border pass shares this function but mints
                    # `border:{blk}:{m}` ids that nothing binds, so aliasing them is pure noise.
                    got = _alias_onto_lake(graph, blk, sp, node_by_blk)
                    if got is not None:
                        aliased.append((sp.split_id, got[0], round(got[1], 1)))
            if applied is not None:
                applied.append(_replace(sp, picked_up=picked))
    graph.up_adj, graph.down_adj = _rebuild_adj(graph.edges)
    return graph
