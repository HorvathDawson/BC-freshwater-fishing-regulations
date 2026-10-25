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

from pipeline.common import tolerances as _tol
from pipeline.common.models import (BoundaryKind, FlowEdge, NodeKind, SectionBoundary, SplitPoint,
                     StreamGraph)

_ANCHOR_KIND = {"point": BoundaryKind.split, "line": BoundaryKind.split,
                "confluence": BoundaryKind.confluence, "mu_boundary": BoundaryKind.mu,
                "lake": BoundaryKind.lake, "border": BoundaryKind.border,
                "area_boundary": BoundaryKind.area}

# ------------------------------------------------------------------------------------------------
# ONE PLACEMENT PASS (T179, owner 2026-10-24: "splits can merge with lake or gauge or other auto
# generated splits, but also handle real splits close together").
#
# The old pass cut one split at a time and decided "same place?" as each arrived (`_pickup`), so the
# result hung on ORDER and on `proximity_m` — one field read as both the projection tolerance and a
# pickup radius, with 500 m as an "inherited" sentinel. A gauge running second relabelled the boundary
# it reused (25 boundaries showed a station code), a gauge running first was never merged at all, and
# authorship was read off the id spelling (`gauge__`, `area:`, ...).
#
# Now, per blue line, every position is COLLECTED first — the boundaries already standing (lake edges,
# earlier passes' cuts, the natural ends) and every new point — and then PLACED:
#   1. An AUTHORED point (`SplitPoint.source` "curated" or "", set where it is minted) takes a lake edge,
#      a natural end or a minted boundary standing within `AUTHORED_MERGE_M` (the Stamp dam 0.5 m from
#      Great Central Lake); an `at_edge` point reaches `AT_EDGE_M` for one (the Bella Coola head, 58 m).
#      Nearest pair first. Two AUTHORED positions are never merged by distance — both are real — unless
#      they are one spot written twice (`SAME_SPOT_M`: the Thompson mouth authored on both waters, the
#      Region 2/3 line on the Spuzzum confluence). Closer than `SLIVER_M` but not one spot, both are cut,
#      the pair is reported as a CONFLICT and the sliver gate stops the build: fix the source (the
#      2026-10-06 ruling — no snap, no merge, no metre floor).
#   2. An AUTO point (gauge / area / border / length) joins the nearest standing boundary or authored
#      place within its family's `MERGE_M`; the rest cluster among themselves. It never takes a BARE
#      natural end (that would name the end with a station code) and never relabels a name.
#   3. Naming: an authored name wins; a lake edge yields only to an authored split (its label travels
#      as `label:` so the registry still spells the bindable id); among auto ids border > area > gauge
#      > length. Every other id at the place is an alias, so every id still binds.
# Then each place is cut once. The same inputs in any order give the same boundaries.
# ------------------------------------------------------------------------------------------------

#: Families the pipeline mints. Authorship is the point's `source`, set where each family is minted
#: (`anchors._emit`, `border`, `area_splits`, `clean_cut`, `length_splits`) — never parsed from an id.
AUTO_SOURCES = frozenset({"gauge", "area", "border", "length"})

#: An authored point this close to a lake edge, a natural end or a minted boundary IS at it: the piece
#: between would be a sliver (pipeline/common/tolerances.py).
AUTHORED_MERGE_M = _tol.SLIVER_M     # 5 m
#: Two AUTHORED points closer than this are one spot written twice and share one boundary. Farther
#: apart they are two cuts, however close.
SAME_SPOT_M = _tol.AT_CUT_M          # 1 m
#: How far an `at_edge` cut reaches for the lake edge / natural end / minted boundary it stands at. The
#: eight curated cuts that used to carry a pickup radius in `proximity_m` (30-300 m) set `at_edge`.
AT_EDGE_M = 300.0
#: An auto family's merge distance is its own positional uncertainty. A GAUGE is a published station
#: coordinate projected onto its channel (`gauges.consume.cuts.PROXIMITY_M`, 400 m — the value the old
#: pickup read, one-sidedly, as its radius); area, border and length cuts are exact geometry, so they
#: join only what IS the same point (`SAME_PLACE_M`, float identity — the 2026-10-06 ruling).
MERGE_M = {"gauge": 400.0, "length": _tol.SAME_PLACE_M, "area": _tol.SAME_PLACE_M,
           "border": _tol.SAME_PLACE_M}
#: Which auto id names a place no authored name stands at.
_AUTO_RANK = {"border": 0, "area": 1, "gauge": 2, "length": 3}


def is_authored(source: str) -> bool:
    """A position somebody wrote (a curated split; a lake edge, `source` ""), not one the pipeline minted."""
    return str(source or "") not in AUTO_SOURCES


def minted_id(split_id: str) -> bool:
    """For a reader holding only an ID (the registry, the review app, the bundle): is it a family the
    pipeline mints — `gauge__08HB008`, `area:6`, `border:360512399:0`, `length:356559402:23614`? The
    placement above never asks this; it reads `source`."""
    sid = str(split_id or "")
    sid = sid[len("split:"):] if sid.startswith("split:") else sid
    head = sid.split("__")[0] if "__" in sid else sid.split(":")[0]
    return head in AUTO_SOURCES


def _rank(source: str) -> int:
    return -1 if is_authored(source) else _AUTO_RANK.get(str(source), len(_AUTO_RANK))


def _merge_m(sp: SplitPoint) -> float:
    return MERGE_M.get(sp.source, _tol.SAME_PLACE_M)


def _is_lake(b: Optional[SectionBoundary]) -> bool:
    return b is not None and b.kind == BoundaryKind.lake


def _key(sp: SplitPoint):
    return (sp.route_measure, sp.split_id, sp.source)


class _Place:
    """One place on a blue line: a position already standing (a piece end) or a new cut."""
    __slots__ = ("m", "bnd", "existing", "end", "members")

    def __init__(self, m, bnd=None, existing=False, end=False):
        self.m, self.bnd, self.existing, self.end = m, bnd, existing, end
        self.members: list[SplitPoint] = []

    def authored_names(self) -> list[str]:
        """The authored names already here: a curated cut standing, or authored points placed here."""
        out = []
        if self.bnd is not None and is_authored(self.bnd.source) and not _is_lake(self.bnd):
            out.append(str(self.bnd.boundary_id))
        return out + [f"split:{s.split_id}" for s in self.members if is_authored(s.source)]


def _nearest(places, M: float, r: float):
    best = None
    for p in places:
        d = abs(p.m - M)
        if (d < r or d <= _tol.SAME_PLACE_M) and (best is None or (d, p.m) < (abs(best.m - M), best.m)):
            best = p
    return best


def _plan_blk(graph, blk, sps, nids, conflicts=None) -> list[_Place]:
    """COLLECT then PLACE every point of one blue line (the block comment above). Returns the places that
    received a point, mouth to source. Pure: nothing is cut or relabelled here."""
    pieces = [graph.nodes[n] for n in nids
              if graph.nodes.get(n) is not None and graph.nodes[n].kind == NodeKind.stream
              and graph.nodes[n].blk == blk]
    standing: dict[float, _Place] = {}
    if pieces:
        lo = min(n.down_m for n in pieces)
        hi = max(n.up_m for n in pieces)
        for n in pieces:
            for m, b in ((n.down_m, n.lower_bound), (n.up_m, n.upper_bound)):
                p = standing.get(m)
                if p is None:
                    standing[m] = _Place(m, b, existing=True, end=(m <= lo or m >= hi))
                elif p.bnd is None and b is not None:
                    p.bnd = b
    existing = sorted(standing.values(), key=lambda p: p.m)

    # 1. AUTHORED. (a) Each takes a standing lake edge / end / minted boundary within reach, nearest pair
    #    first, one authored point per place. (b) The rest join an authored name on the same spot, or
    #    stand as their own cut.
    authored = sorted((s for s in sps if is_authored(s.source)), key=_key)
    takeable = [q for q in existing if not q.authored_names()]
    pairs = []
    for sp in authored:
        reach = AT_EDGE_M if sp.at_edge else AUTHORED_MERGE_M
        for q in takeable:
            d = abs(q.m - sp.route_measure)
            if d < reach or d <= _tol.SAME_PLACE_M:
                pairs.append((d, q.m, _key(sp), sp, q))
    taken: dict[int, _Place] = {}
    for _d, _m, _k, sp, q in sorted(pairs, key=lambda t: t[:3]):
        if id(sp) not in taken and not q.members:
            q.members.append(sp)
            taken[id(sp)] = q
    new: list[_Place] = []
    named = [q for q in existing if q.authored_names()]
    for sp in authored:
        if id(sp) in taken:
            continue
        p = _nearest(named + [q for q in existing if q.members] + new, sp.route_measure, SAME_SPOT_M)
        if p is None:
            p = _Place(sp.route_measure)
            new.append(p)
        p.members.append(sp)
    if conflicts is not None:                          # too close to be two stretches, too far to be one
        held = sorted((p.m, p.authored_names()) for p in existing + new if p.authored_names())
        for (m1, n1), (m2, n2) in zip(held, held[1:]):
            if m2 - m1 < _tol.SLIVER_M:
                conflicts.append({"blk": blk, "ids": sorted(set(n1) | set(n2)), "at_m": round(m1, 2),
                                  "distance_m": round(m2 - m1, 2)})

    # 2. AUTO: the nearest standing boundary or authored place within the family's distance — never a
    #    BARE natural end. What is left clusters among itself, mouth to source.
    anchors = [p for p in existing if not (p.end and p.bnd is None)] + new
    rest: list[SplitPoint] = []
    for sp in sorted((s for s in sps if not is_authored(s.source)), key=_key):
        p = _nearest(anchors, sp.route_measure, _merge_m(sp))
        if p is not None:
            p.members.append(sp)
        else:
            rest.append(sp)
    clusters: list[_Place] = []
    for sp in rest:
        c = clusters[-1] if clusters else None
        if c is not None and sp.route_measure - c.m < min([_merge_m(sp)] + [_merge_m(s) for s in c.members]):
            c.members.append(sp)
        else:
            c = _Place(sp.route_measure)
            c.members.append(sp)
            clusters.append(c)
    for c in clusters:                                # the place is its best-ranked member's
        c.m = min(c.members, key=lambda s: (_rank(s.source),) + _key(s)).route_measure
    return sorted((p for p in existing + new + clusters if p.members), key=lambda p: (p.m, not p.existing))


def _canonical(place: _Place):
    """Who names the place: a standing authored name, else the nearest authored member, else a lake edge,
    else the best-ranked auto id (standing or new). A SectionBoundary (it keeps its id) or a SplitPoint."""
    old = place.bnd
    auth = [s for s in place.members if is_authored(s.source)]
    if old is not None and is_authored(old.source) and not _is_lake(old):
        return old
    if auth:
        return min(auth, key=lambda s: (abs(s.route_measure - place.m),) + _key(s))
    if old is not None and is_authored(old.source):
        return old                                     # a lake edge: auto never relabels a name
    best = min(place.members, key=lambda s: (_rank(s.source), abs(s.route_measure - place.m)) + _key(s))
    if old is not None and _rank(old.source) <= _rank(best.source):
        return old
    return best


def _place_boundary(place: _Place) -> SectionBoundary:
    """The one boundary standing at `place`: the canonical name, every other id an alias."""
    canon = _canonical(place)
    old = place.bnd
    ids: set[str] = {f"split:{s.split_id}" for s in place.members}
    if old is not None:
        ids.add(str(old.boundary_id))
        ids.update(old.aliases or ())
    if isinstance(canon, SectionBoundary):
        return replace(canon, aliases=tuple(sorted(ids - {str(canon.boundary_id)})))
    mine = f"split:{canon.split_id}"
    # CARRYING THE REF IS NOT CARRYING THE NAME: a displaced lake edge's ref is `lake:{wbk}`, which no
    # rule writes; the registry spells the bindable id from the LABEL (`registry.build._expand`), so the
    # label travels (the Sproat reach). A displaced split's ref already is its name.
    if old is not None and old.label and not str(old.boundary_id).startswith("split:"):
        ids.add(f"label:{old.label}")
    return SectionBoundary(boundary_id=mine,
                           kind=_ANCHOR_KIND.get(canon.anchor_type.value, BoundaryKind.split),
                           route_measure=place.m, label=(canon.label or canon.split_id),
                           aliases=tuple(sorted(ids - {mine})), source=canon.source)


def _stream_nids_by_blk(graph, blks) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for nid, n in graph.nodes.items():
        if n.kind == NodeKind.stream and n.blk and (blks is None or n.blk in blks):
            out[n.blk].append(nid)
    return out


def place_points(graph: StreamGraph, split_points: list[SplitPoint],
                 conflicts: Optional[list] = None) -> list[SplitPoint]:
    """Where each point WILL stand, without cutting: a copy at its place's measure, `picked_up` when it
    joins a standing boundary or another point's cut. Lets a caller judge what depends on the cut (the
    braid sweep) at the place the cut is made. Idempotent: `split_graph_at` places the result where this
    said. ``conflicts`` collects every authored join `CONFLICT_M` or more apart."""
    nids = _stream_nids_by_blk(graph, {sp.blk for sp in split_points})
    by_blk: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in split_points:
        by_blk[sp.blk].append(sp)
    out: list[SplitPoint] = []
    for blk in sorted(by_blk):
        for place in _plan_blk(graph, blk, by_blk[blk], nids.get(blk, ()), conflicts):
            canon = None if place.existing else _canonical(place)
            for sp in place.members:
                out.append(replace(sp, route_measure=place.m, picked_up=place.existing or sp is not canon))
    return out


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


#: ONE PLACE, computed twice. The border pass and the region cutter intersect the same blue line
#: with the same edge segment (the B.C. outline is the exact union of the units the regions are
#: dissolved from), and two computations of one intersection agree to ~1e-9 m. This is that identity
#: — float noise on a shared point — not a positional tolerance: two cuts a millimetre apart are two
#: places and are both cut.
SAME_PLACE_M = _tol.SAME_PLACE_M


def _coincident(graph, blk, sp, by_blk=None) -> bool:
    """A cut AT an existing boundary (within `SAME_PLACE_M`) is that boundary under another name.

    It joins the boundary's `aliases` on every piece the boundary bounds, so a rule binding either
    name binds the same place. Without this the cut was dropped in silence (`_find_piece` finds no
    piece strictly around a measure that IS a boundary), or — a hair inside a piece — cut a
    zero-length sliver. THE BORDER IS NAMED THE BORDER: where the provincial border and a region
    edge are one cut, the border is the canonical id (the run's end token reads `bc_border`) and the
    region cut is its alias, whichever came first. Returns True when it aliased."""
    M = sp.route_measure
    mine = f"split:{sp.split_id}"
    kind = _ANCHOR_KIND.get(sp.anchor_type.value, BoundaryKind.split)
    nids = list(by_blk.get(blk, ())) if by_blk is not None else list(graph.nodes)
    hit = None
    for nid in nids:
        n = graph.nodes.get(nid)
        if n is None or n.kind != NodeKind.stream or n.blk != blk:
            continue
        for b in (n.lower_bound, n.upper_bound):
            if b is not None and abs(b.route_measure - M) <= SAME_PLACE_M:
                hit = b
                break
        if hit is not None:
            break
    if hit is None:
        return False
    if mine == hit.boundary_id or mine in (hit.aliases or ()):
        return True
    if kind == BoundaryKind.border and hit.kind != BoundaryKind.border:
        prior = set(hit.aliases or ()) | {str(hit.boundary_id)}
        if hit.label and not str(hit.boundary_id).startswith("split:"):
            prior.add(f"label:{hit.label}")
        merged = SectionBoundary(boundary_id=mine, kind=kind, route_measure=hit.route_measure,
                                 label=(sp.label or sp.split_id),
                                 aliases=tuple(sorted(prior - {mine})))
    else:
        merged = replace(hit, aliases=tuple(hit.aliases or ()) + (mine,))
    for nid in nids:                                  # the bound is shared by the pieces it separates
        n = graph.nodes.get(nid)
        if n is None or n.kind != NodeKind.stream or n.blk != blk:
            continue
        patch = {e: merged for e in ("lower_bound", "upper_bound")
                 if getattr(n, e) is not None and getattr(n, e).boundary_id == hit.boundary_id
                 and getattr(n, e).route_measure == hit.route_measure}
        if patch:
            graph.nodes[nid] = replace(n, **patch)
    return True


def _split_one(graph, geoms, blk, sp, fid_index, by_blk=None, edges_by_to=None,
               edges_by_from=None, bnd: Optional[SectionBoundary] = None) -> bool:
    """Cut the piece around ``sp.route_measure``; the new bound is ``bnd`` (a placed boundary) or one
    minted from ``sp``."""
    M = sp.route_measure if bnd is None else bnd.route_measure
    pid = _find_piece(graph, blk, M, by_blk)
    if pid is None:
        return False                          # measure at a boundary / in a lake / off-blk
    hi_id = f"{blk}:{int(M)}"
    if hi_id == pid or hi_id in graph.nodes:
        return False                          # degenerate or id collision — skip
    P = graph.nodes[pid]
    if bnd is None:
        bnd = SectionBoundary(boundary_id=f"split:{sp.split_id}",
                              kind=_ANCHOR_KIND.get(sp.anchor_type.value, BoundaryKind.split),
                              route_measure=M, label=(sp.label or sp.split_id), source=sp.source)

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
                   fid_index: Optional[dict] = None, exact: bool = False,
                   applied: Optional[list] = None, aliased: Optional[list] = None,
                   conflicts: Optional[list] = None) -> StreamGraph:
    """Subdivide piece nodes at ``split_points`` (in place); rebuild adjacency.

    Each blue line's points are PLACED together first (`_plan_blk`: collect every standing position and
    every point, merge auto into names, keep real cuts apart) and each place is then cut once, so the
    result does not depend on the order the points arrive in. ``exact`` skips placement — every point is
    cut where it is unless it IS a standing boundary (`_coincident`): the braid cuts, which must sit on
    the chord their host cut was judged by.
    ``fid_index`` (fid -> (down_m, up_m, order, magnitude)) enables per-piece order/magnitude
    re-partitioning; omit it (synthetic tests) to keep the parent piece's values.
    ``applied`` (optional): each SplitPoint, at the measure it was placed at, with ``picked_up`` set when
    it joined a standing boundary or another point's cut — the record ``splits.resolved.json`` consumes.
    ``aliased`` (optional): ``(split_id, boundary_id, distance)`` for each split that could not be cut
    because it landed in a lake run and was recorded as an ALIAS on the lake's edge (`_alias_onto_lake`).
    ``conflicts`` (optional): authored cuts joined because they stood under `AUTHORED_MERGE_M` apart.
    """
    from dataclasses import replace as _replace
    # Indexes built ONCE (maintained incrementally by _split_one): node ids per blk, and edge ids per
    # to_node. They turn each split's piece-find + tributary-reattach from O(all nodes)+O(all edges)
    # into O(local) — the fix for the border stage's ~1600s split cost.
    node_by_blk = _stream_nids_by_blk(graph, None)
    edges_by_to: dict[str, list[int]] = defaultdict(list)
    edges_by_from: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(graph.edges):
        edges_by_to[e.to_node].append(i)
        edges_by_from[e.from_node].append(i)

    def _lake_alias(blk, sp):
        # No piece to cut. If it landed in a LAKE RUN, record it as another name for that lake's
        # boundary instead of dropping it silently; anything else stays missing. Only for a caller that
        # asked (`aliased` given) — the CURATED pass, whose split ids rules actually bind.
        if aliased is not None:
            got = _alias_onto_lake(graph, blk, sp, node_by_blk)
            if got is not None:
                aliased.append((sp.split_id, got[0], round(got[1], 1)))

    sp_by_blk: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in split_points:
        sp_by_blk[sp.blk].append(sp)
    placed_by_blk: dict[str, list[SplitPoint]] = defaultdict(list)   # where each point stands
    for blk in sorted(sp_by_blk):
        sps = sp_by_blk[blk]
        if exact:
            for sp in sorted(sps, key=_key):
                picked = _coincident(graph, blk, sp, node_by_blk)
                if not picked and not _split_one(graph, geoms, blk, sp, fid_index, node_by_blk,
                                                 edges_by_to, edges_by_from):
                    _lake_alias(blk, sp)
                placed_by_blk[blk].append(sp)
                if applied is not None:
                    applied.append(_replace(sp, picked_up=picked))
            continue
        for place in _plan_blk(graph, blk, sps, node_by_blk.get(blk, ()), conflicts):
            bnd = _place_boundary(place)
            canon = _canonical(place)
            cut = False
            if place.existing:
                for nid in node_by_blk.get(blk, ()):        # the bound is shared by the pieces it separates
                    n = graph.nodes[nid]
                    patch = {}
                    if abs(n.up_m - place.m) < 1e-6:
                        patch["upper_bound"] = bnd
                    if abs(n.down_m - place.m) < 1e-6:
                        patch["lower_bound"] = bnd
                    if patch:
                        graph.nodes[nid] = _replace(n, **patch)
            else:
                cut = _split_one(graph, geoms, blk, canon, fid_index, node_by_blk, edges_by_to,
                                 edges_by_from, bnd=bnd)
                if not cut:
                    for sp in place.members:
                        _lake_alias(blk, sp)
            for sp in place.members:
                at = _replace(sp, route_measure=place.m,
                              picked_up=place.existing or (cut and sp is not canon))
                placed_by_blk[blk].append(at)
                if applied is not None:
                    applied.append(at)
    _name_the_crossings(graph, placed_by_blk, node_by_blk)
    graph.up_adj, graph.down_adj = _rebuild_adj(graph.edges)
    return graph


def _name_the_crossings(graph, sp_by_blk, node_by_blk) -> None:
    """WHICH CROSSING — `area:5@down` and `area:5@up` as ALIASES on the boundaries that are there.

    A region boundary can meet the same river several times and every crossing is minted under the
    one id `area:5`, so a rule cannot say WHICH. Region 3's "Hells Gate upstream to the Region 3
    boundary" is authored as `upstream_of` Hells Gate with nothing to stop it, and it runs on into
    Region 5.

    `@down` is the FIRST crossing and `@up` the LAST, by route measure from the mouth — the bottom
    and the top of the straddle. Between them the water is in neither region cleanly, and a rule
    forced to pick should pick `@up`, so the grey stretch stays inside the rule rather than falling
    out of every rule.

    THESE ARE ALIASES, NOT CUTS. Minting them as SplitPoints instead put a second cut at a measure
    that already had one, which risks the zero-length piece that breaks both halves, and it let the
    qualified id shadow the plain one in code that takes the first point for a blk. `aliases` is
    the field that already exists for exactly this — "other boundary_ids this one ALSO represents".

    A boundary crossed ONCE gets nothing: `area:5` is unambiguous there, and `@down`/`@up` would be
    two more names for one place.
    """
    from dataclasses import replace as _replace
    for blk, sps in sp_by_blk.items():
        by_area: dict[str, list[SplitPoint]] = defaultdict(list)
        for sp in sps:
            if str(sp.split_id).startswith("area:") and "@" not in str(sp.split_id):
                by_area[sp.split_id].append(sp)
        for sid, ps in by_area.items():
            if len(ps) < 2:
                continue
            ps = sorted(ps, key=lambda q: q.route_measure)
            for end, q in (("down", ps[0]), ("up", ps[-1])):
                want = f"split:{sid}@{end}"
                for nid in node_by_blk.get(blk, ()):
                    n = graph.nodes[nid]
                    for b in (n.lower_bound, n.upper_bound):
                        if b is None or abs(b.route_measure - q.route_measure) > 1e-6:
                            continue
                        if want in (b.aliases or ()):
                            continue
                        merged = _replace(b, aliases=tuple(b.aliases or ()) + (want,))
                        if n.lower_bound is b:
                            graph.nodes[nid] = _replace(n, lower_bound=merged)
                        else:
                            graph.nodes[nid] = _replace(n, upper_bound=merged)
