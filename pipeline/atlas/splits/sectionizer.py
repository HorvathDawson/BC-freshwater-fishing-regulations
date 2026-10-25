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

from shapely.geometry import LineString, Point
from shapely.ops import substring

from pipeline.common import tolerances as _tol
from pipeline.common.models import (AnchorType, BoundaryKind, FlowEdge, NodeKind, SectionBoundary,
                                    SplitPoint, StreamGraph)

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
#   1. An AUTHORED point (`SplitPoint.source` "curated" or "", set where it is minted) NEVER MOVES (owner
#      2026-10-24). A minted boundary standing within `AUTHORED_MERGE_M` (a border cut from the earlier
#      pass) joins IT: that cut is shifted onto the authored measure (`_shift_cut`), the authored id is
#      canonical, the minted id an alias. A lake edge or a natural line end within `AUTHORED_MERGE_M` is
#      NOT taken: the point is cut where it stands, the pair goes to `split_conflicts.json` (kind
#      `lake_edge` / `line_end`) with a proposal to re-anchor the split on the lake / its own mouth
#      (`split_lake_anchor_proposals.json` — the owner accepts it; no tool writes splits.json), and the
#      sliver gate stops the build on the piece between. Only an `at_edge` point (its curated location
#      IS the edge: the Stamp dam, the Bella Coola head, 58 m) still takes a lake edge, a natural end or
#      a minted boundary within `AT_EDGE_M`, nearest pair first. Two AUTHORED positions are never merged by distance — both are real — unless
#      they are one spot written twice (`SAME_SPOT_M`: the Thompson mouth authored on both waters, the
#      Region 2/3 line on the Spuzzum confluence). Closer than `SLIVER_M` but not one spot, both are cut,
#      the pair is reported as a CONFLICT and the sliver gate stops the build: fix the source (the
#      2026-10-06 ruling — no snap, no merge, no metre floor).
#   2. An AUTO point (gauge / area / border / length) joins the nearest standing boundary or authored
#      place within its family's `MERGE_M`, and any authored place within `AUTHORED_MERGE_M`; the rest cluster among themselves. It never takes a BARE
#      natural end (that would name the end with a station code) and never relabels a name.
#   3. Naming: an authored name wins; a lake edge yields only to an authored split (its label travels
#      as `label:` so the registry still spells the bindable id); among auto ids border > area > gauge
#      > length. Every other id at the place is an alias, so every id still binds.
# Then each place is cut once. The same inputs in any order give the same boundaries.
# ------------------------------------------------------------------------------------------------

#: Families the pipeline mints. Authorship is the point's `source`, set where each family is minted
#: (`anchors._emit`, `border`, `area_splits`, `clean_cut`, `length_splits`) — never parsed from an id.
AUTO_SOURCES = frozenset({"gauge", "area", "border", "length"})

#: A minted boundary (or auto point) this close to an authored point joins it, at the authored measure;
#: a lake edge or natural end this close is a conflict — the piece between would be a sliver
#: (pipeline/common/tolerances.py).
AUTHORED_MERGE_M = _tol.SLIVER_M     # 5 m
#: Two AUTHORED points closer than this are one spot written twice and share one boundary. Farther
#: apart they are two cuts, however close.
SAME_SPOT_M = _tol.AT_CUT_M          # 1 m
#: How far an `at_edge` cut reaches for the lake edge / natural end / minted boundary it stands at. The
#: eight curated cuts that used to carry a pickup radius in `proximity_m` (30-300 m) set `at_edge`.
AT_EDGE_M = 300.0
#: How far a lake edge (or the line's own mouth, offered as a `confluence` anchor) may stand from an
#: authored cut and still draw a RE-ANCHOR PROPOSAL (owner 2026-10-24: 25 m). Placement ignores it: the
#: cut stands where it is; only under `AUTHORED_MERGE_M` is the pair also a sliver conflict.
PROPOSAL_M = 25.0
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
    # `gauge__<station>`, or `<family>:...` — read by its FIRST separator, so an area slug carrying a
    # double underscore (`area:x__y`) is still minted and a curated `stamp_river__lake` is not
    return sid.startswith("gauge__") or (":" in sid and sid.split(":", 1)[0] in AUTO_SOURCES)


def _other_names(label: str, *labels) -> tuple[str, ...]:
    """Every name at a place but the canonical `label`, sorted, once (`SectionBoundary.names`)."""
    return tuple(sorted({str(x) for x in labels if x} - {str(label or "")}))


def _union_bound(a: SectionBoundary, b: SectionBoundary) -> SectionBoundary:
    """One boundary seen on the two pieces it separates: whatever either copy carries, kept."""
    if a is b or b is None or a.boundary_id != b.boundary_id:
        return a
    al = tuple(sorted(set(a.aliases or ()) | set(b.aliases or ())))
    nm = tuple(sorted(set(a.names or ()) | set(b.names or ())))
    return a if (al == tuple(a.aliases or ()) and nm == tuple(a.names or ())) else \
        replace(a, aliases=al, names=nm)


def _rank(source: str) -> int:
    return -1 if is_authored(source) else _AUTO_RANK.get(str(source), len(_AUTO_RANK))


def _merge_m(sp: SplitPoint) -> float:
    return MERGE_M.get(sp.source, _tol.SAME_PLACE_M)


def _is_lake(b: Optional[SectionBoundary]) -> bool:
    return b is not None and b.kind == BoundaryKind.lake


def _bound_wbk(b: Optional[SectionBoundary]) -> str:
    """The wbk a lake boundary (`lake:{wbk}`) stands at; "" for any other boundary."""
    sid = str(getattr(b, "boundary_id", "") or "")
    return sid.split(":", 1)[1] if _is_lake(b) and sid.startswith("lake:") else ""


def _folds(b: Optional[SectionBoundary], fold) -> bool:
    """A lake edge onto a polygon the registry folds into its stream (`registry.flowing`): part of the
    river, not another water — no lake-side alias, no `lake_edge` conflict, no lake-anchor proposal."""
    return bool(fold) and _bound_wbk(b) in fold


def _fold_wbks(graph, fold):
    if fold is not None:
        return frozenset(fold)
    from pipeline.atlas.registry.flowing import folding_polygon_wbks
    return folding_polygon_wbks(graph)


def _key(sp: SplitPoint):
    return (sp.route_measure, sp.split_id, sp.source)


class _Place:
    """One place on a blue line: a position already standing (a piece end) or a new cut."""
    __slots__ = ("m", "bnd", "existing", "end", "members", "edge", "from_m", "fold")

    def __init__(self, m, bnd=None, existing=False, end=False, edge=""):
        self.m, self.bnd, self.existing, self.end, self.edge = m, bnd, existing, end, edge
        #: The edge of a polygon the registry folds into this stream (`_folds`): not a lake edge.
        self.fold = False
        self.members: list[SplitPoint] = []
        #: A standing MINTED cut moved onto an authored point: the measure it stood at (`_shift_cut`).
        self.from_m: Optional[float] = None

    @property
    def stands(self) -> bool:
        """Already cut where it is: nothing to cut or move, only to relabel."""
        return self.existing and self.from_m is None

    def authored_names(self) -> list[str]:
        """The authored names already here: a curated cut standing, or authored points placed here."""
        out = []
        if self.bnd is not None and is_authored(self.bnd.source) and not _is_lake(self.bnd):
            out.append(str(self.bnd.boundary_id))
        return out + [f"split:{s.split_id}" for s in self.members if is_authored(s.source)]


def _nearest(places, M: float, r: float, r_named: Optional[float] = None):
    """The nearest place within `r` (`r_named` for a place an authored name stands at)."""
    best = None
    for p in places:
        d = abs(p.m - M)
        rr = r_named if (r_named is not None and p.authored_names()) else r
        if (d < rr or d <= _tol.SAME_PLACE_M) and (best is None or (d, p.m) < (abs(best.m - M), best.m)):
            best = p
    return best


def _proposal(graph, blk, sp: SplitPoint, q: "_Place", kind: str, d: float) -> dict:
    """A PROPOSAL (never applied — no tool writes splits.json, T185) to re-curate an authored point a
    sliver from a lake edge / line end as anchored ON it: a `lake` anchor (wbk — the anchor the
    split-alias lake boundaries use; note it emits both of the lake's edges on the blk) or, at the
    line's own mouth, a `confluence` anchor on the blk itself. A source end has no anchor: report only."""
    out = {"split_id": sp.split_id, "blk": blk, "kind": kind, "edge": q.edge or "",
           "distance_m": round(d, 2), "split_m": round(sp.route_measure, 2), "edge_m": round(q.m, 2),
           # under `AUTHORED_MERGE_M` the pair is also a sliver conflict (split_conflicts.json)
           "band": "0-5m" if d < AUTHORED_MERGE_M else f"5-{int(PROPOSAL_M)}m"}
    if _is_lake(q.bnd):
        wbk = str(q.bnd.boundary_id).split(":", 1)[1]
        lake = graph.nodes.get(f"lake:{wbk}")
        out.update(lake=q.bnd.label or "", wbk=wbk, gnis_id=getattr(lake, "gnis_id", "") or "",
                   anchor={"type": "lake", "wbk": wbk})
    elif q.edge == "mouth":
        out.update(anchor={"type": "confluence", "tributary_blk": blk})
    else:
        out.update(anchor=None)
    return out


def _plan_blk(graph, blk, sps, nids, conflicts=None, proposals=None, fold=frozenset()) -> list[_Place]:
    """COLLECT then PLACE every point of one blue line (the block comment above). Returns the places that
    received a point, mouth to source. Pure: nothing is cut or relabelled here."""
    pieces = [graph.nodes[n] for n in nids
              if graph.nodes.get(n) is not None and graph.nodes[n].kind == NodeKind.stream
              and graph.nodes[n].blk == blk]
    standing: dict[float, _Place] = {}
    below_at: set[float] = set()                       # measures a piece ends at (a piece below)
    above_at: set[float] = set()                       # measures a piece starts at (a piece above)
    if pieces:
        lo = min(n.down_m for n in pieces)
        hi = max(n.up_m for n in pieces)
        for n in pieces:
            below_at.add(n.up_m)
            above_at.add(n.down_m)
            # a lake bounding a piece's TOP is the lake's outlet; bounding its BOTTOM, the inlet
            for m, b, edge in ((n.down_m, n.lower_bound, "inlet"), (n.up_m, n.upper_bound, "outlet")):
                if not _is_lake(b):
                    edge = "mouth" if m <= lo else "source" if m >= hi else ""
                p = standing.get(m)
                if p is None:
                    standing[m] = _Place(m, b, existing=True, end=(m <= lo or m >= hi), edge=edge)
                elif p.bnd is None and b is not None:
                    p.bnd, p.edge = b, edge
                elif b is not None:                    # both copies: no alias or name left behind
                    p.bnd = _union_bound(p.bnd, b)
    for p in standing.values():
        p.fold = _folds(p.bnd, fold)
    existing = sorted(standing.values(), key=lambda p: p.m)

    def _lake(q: _Place) -> bool:
        """A lake edge that IS another water's (a folded polygon's edge is the river's own)."""
        return _is_lake(q.bnd) and not q.fold

    # THE RIVER'S OWN POLYGON (owner 2026-10-25, Cayoosh Falls in wbk 329505890): the gap between the
    # piece below a folded polygon and the piece above it is the river, drawn as a polygon. An authored
    # point inside it has no piece to cut, so it is cut at the polygon's nearer edge as a plain point —
    # never aliased onto the polygon as if it were a lake (the fold drops that edge and refuses the alias).
    fold_gaps: list[tuple[_Place, _Place]] = []
    if fold:
        ordered = sorted(pieces, key=lambda n: n.down_m)
        for below, above in zip(ordered, ordered[1:]):
            ub, lb = below.upper_bound, above.lower_bound
            if (_folds(ub, fold) and _folds(lb, fold) and ub.boundary_id == lb.boundary_id
                    and below.up_m < above.down_m):
                fold_gaps.append((standing[below.up_m], standing[above.down_m]))

    def _shiftable(q: _Place, M: float) -> bool:
        """A standing minted cut with a piece on each side, movable to M without crossing another."""
        if q.end or _is_lake(q.bnd) or q.m not in below_at or q.m not in above_at:
            return False
        i = existing.index(q)
        return ((i == 0 or existing[i - 1].m < M) and
                (i == len(existing) - 1 or M < existing[i + 1].m))

    # 1. AUTHORED — never moves. (a) An `at_edge` point takes a standing lake edge / end / minted
    #    boundary within `AT_EDGE_M`; any other point takes only the same point, or a minted cut within
    #    `AUTHORED_MERGE_M`, which moves onto IT. Nearest pair first, one authored point per place. A
    #    lake edge / end within `AUTHORED_MERGE_M` is `near`: reported, never taken. (b) The rest join an
    #    authored name on the same spot, or stand as their own cut.
    authored = sorted((s for s in sps if is_authored(s.source)), key=_key)
    takeable = [q for q in existing if not q.authored_names()]
    pairs, near = [], []
    in_fold: set[int] = set()                          # authored points inside a folded polygon
    for sp in authored:
        M = sp.route_measure
        for lo_q, hi_q in fold_gaps:
            if lo_q.m + _tol.SAME_PLACE_M < M < hi_q.m - _tol.SAME_PLACE_M:
                in_fold.add(id(sp))
                for q in (lo_q, hi_q):
                    if q in takeable:
                        pairs.append((abs(q.m - M), q.m, _key(sp), sp, q))
        if id(sp) in in_fold:
            continue
        for q in takeable:
            d = abs(q.m - sp.route_measure)
            if d <= _tol.SAME_PLACE_M or (sp.at_edge and d < AT_EDGE_M):
                pairs.append((d, q.m, _key(sp), sp, q))
            elif not sp.at_edge and d < AUTHORED_MERGE_M:
                if _shiftable(q, sp.route_measure):
                    pairs.append((d, q.m, _key(sp), sp, q))
                else:
                    near.append((d, q.m, _key(sp), sp, q))
    taken: dict[int, _Place] = {}
    for _d, _m, _k, sp, q in sorted(pairs, key=lambda t: t[:3]):
        if id(sp) not in taken and not q.members:
            q.members.append(sp)
            taken[id(sp)] = q
            if (not sp.at_edge and id(sp) not in in_fold
                    and abs(q.m - sp.route_measure) > _tol.SAME_PLACE_M):
                q.from_m, q.m = q.m, sp.route_measure      # the minted cut moves, not the name
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
                conflicts.append({"blk": blk, "kind": "authored_pair", "ids": sorted(set(n1) | set(n2)),
                                  "at_m": round(m1, 2), "distance_m": round(m2 - m1, 2)})
    # An authored point a sliver from a lake edge / natural end: it stays, the pair is reported, and a
    # re-anchoring is PROPOSED (it is usually meant to BE at the lake) for the owner to accept.
    def _kind(q: _Place) -> str:
        return ("lake_edge" if _lake(q) else "polygon_edge" if q.fold
                else "line_end" if q.end else "minted_edge")

    seen: set[int] = set()
    sliver: set[tuple[int, int]] = set()               # (point, place) pairs reported as conflicts
    for d, _m, _k, sp, q in sorted(near, key=lambda t: t[:3]):
        if id(sp) in taken or id(sp) in seen:
            continue
        seen.add(id(sp))
        sliver.add((id(sp), id(q)))
        mine = f"split:{sp.split_id}"
        other = str(q.bnd.boundary_id) if q.bnd is not None else f"end:{q.edge or 'bare'}"
        if conflicts is not None:
            conflicts.append({"blk": blk, "kind": _kind(q), "ids": sorted({mine, other}),
                              "at_m": round(min(q.m, sp.route_measure), 2), "distance_m": round(d, 2)})
    # PROPOSALS (owner 2026-10-24): an authored cut within `PROPOSAL_M` of a lake edge or of its line's
    # mouth (a `confluence` anchor) is offered a re-anchoring there — placement unchanged, the cut stands
    # where it is. A source end or a minted cut offers no anchor: proposed only for the sliver conflict
    # itself (report-only). The nearest wins; a cut standing ON a place (taken there) needs none.
    # An AREA boundary (a park's edge) is offered nothing: it is not a confluence or a lake, whatever
    # it lands near (owner 2026-10-25, Tweedsmuir / Pinnacles; the conflict is still reported).
    if proposals is not None:
        offer = []
        for sp in authored:
            if sp.at_edge or sp.anchor_type == AnchorType.area_boundary or id(sp) in in_fold:
                continue
            on = taken.get(id(sp))
            if on is not None and abs((on.from_m if on.from_m is not None else on.m)
                                      - sp.route_measure) <= _tol.SAME_PLACE_M:
                continue
            for q in existing:
                if q is on:
                    continue
                d = abs(q.m - sp.route_measure)
                anchored = _lake(q) or (q.end and q.edge == "mouth")
                if (anchored and _tol.SAME_PLACE_M < d < PROPOSAL_M) or (id(sp), id(q)) in sliver:
                    offer.append((d, q.m, _key(sp), sp, q))
        done: set[int] = set()
        for d, _m, _k, sp, q in sorted(offer, key=lambda t: t[:3]):
            if id(sp) not in done:
                done.add(id(sp))
                proposals.append(_proposal(graph, blk, sp, q, _kind(q), d))

    # 2. AUTO: the nearest standing boundary or authored place within the family's distance (an
    #    authored place within at least `AUTHORED_MERGE_M`: it joins the name, at the name's measure) —
    #    never a BARE natural end. What is left clusters among itself, mouth to source.
    anchors = [p for p in existing if not (p.end and p.bnd is None)] + new
    rest: list[SplitPoint] = []
    for sp in sorted((s for s in sps if not is_authored(s.source)), key=_key):
        p = _nearest(anchors, sp.route_measure, _merge_m(sp), max(_merge_m(sp), AUTHORED_MERGE_M))
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
    # EVERY NAME TOO (owner 2026-10-24): each member's label, and the standing boundary's own
    labels = [s.label or s.split_id for s in place.members]
    if old is not None:
        ids.update(old.aliases or ())
        if not place.fold:                             # a folded polygon is no water of its own
            ids.add(str(old.boundary_id))
            labels += [old.label, *(old.names or ())]
    if isinstance(canon, SectionBoundary):
        return replace(canon, aliases=tuple(sorted(ids - {str(canon.boundary_id)})),
                       names=_other_names(canon.label, *labels))
    mine = f"split:{canon.split_id}"
    # CARRYING THE REF IS NOT CARRYING THE NAME: a displaced lake edge's ref is `lake:{wbk}`, which no
    # rule writes; the registry spells the bindable id from the LABEL (`registry.build._expand`), so the
    # label travels (the Sproat reach). A displaced split's ref already is its name.
    if old is not None and old.label and not place.fold \
            and not str(old.boundary_id).startswith("split:"):
        ids.add(f"label:{old.label}")
    label = canon.label or canon.split_id
    kind = _ANCHOR_KIND.get(canon.anchor_type.value, BoundaryKind.split)
    if kind == BoundaryKind.border and is_authored(canon.source):
        # a CURATED split on the border ("U.S. border") is a named cut like any point: the border id
        # rides as an alias; `border` kind stays the minted outline's own (the bundle's `bc_border`)
        kind = BoundaryKind.split
    return SectionBoundary(boundary_id=mine,
                           kind=kind,
                           route_measure=place.m, label=label,
                           aliases=tuple(sorted(ids - {mine})), source=canon.source,
                           names=_other_names(label, *labels))


def _stream_nids_by_blk(graph, blks) -> dict[str, list[str]]:
    out: dict[str, list[str]] = defaultdict(list)
    for nid, n in graph.nodes.items():
        if n.kind == NodeKind.stream and n.blk and (blks is None or n.blk in blks):
            out[n.blk].append(nid)
    return out


def place_points(graph: StreamGraph, split_points: list[SplitPoint],
                 conflicts: Optional[list] = None,
                 proposals: Optional[list] = None, fold_wbks=None) -> list[SplitPoint]:
    """Where each point WILL stand, without cutting: a copy at its place's measure, `picked_up` when it
    joins a standing boundary or another point's cut. Lets a caller judge what depends on the cut (the
    braid sweep) at the place the cut is made. Idempotent: `split_graph_at` places the result where this
    said. ``conflicts`` collects every authored point a sliver from another authored point, a lake edge
    or a line end; ``proposals`` the lake / mouth re-anchorings offered for the latter two.
    ``fold_wbks``: the polygons the registry folds into their stream (`_folds`); None reads them off the
    graph (`registry.flowing.folding_polygon_wbks`)."""
    fold = _fold_wbks(graph, fold_wbks)
    nids = _stream_nids_by_blk(graph, {sp.blk for sp in split_points})
    by_blk: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in split_points:
        by_blk[sp.blk].append(sp)
    out: list[SplitPoint] = []
    for blk in sorted(by_blk):
        for place in _plan_blk(graph, blk, by_blk[blk], nids.get(blk, ()), conflicts, proposals, fold):
            canon = None if place.stands else _canonical(place)
            for sp in place.members:
                out.append(replace(sp, route_measure=place.m, picked_up=place.stands or sp is not canon))
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


def _alias_onto_lake(graph, blk, sp, by_blk=None, fold=frozenset()) -> tuple[str, float] | None:
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
        if _folds(lo_b, fold):
            return None                               # the river's own polygon, not a lake (`_folds`)
        # attach to the nearer edge: one lake has two bounds, and aliasing both would give the split
        # two measures and make every reach that uses it ambiguous
        target, node, end = ((lo_b, below, "upper_bound") if (M - below.up_m) <= (above.down_m - M)
                             else (hi_b, above, "lower_bound"))
        dist = abs(M - (target.route_measure if target.route_measure is not None else M))
        if dist > _ALIAS_MAX_FROM_EDGE_M:
            return None                               # deep inside the lake: not this edge
        if want in (target.aliases or ()):
            return target.boundary_id, dist
        merged = replace(target, aliases=tuple(target.aliases or ()) + (want,),
                         names=_other_names(target.label, *(target.names or ()), sp.label))
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
        label = sp.label or sp.split_id
        merged = SectionBoundary(boundary_id=mine, kind=kind, route_measure=hit.route_measure,
                                 label=label, aliases=tuple(sorted(prior - {mine})),
                                 source=sp.source,
                                 names=_other_names(label, hit.label, *(hit.names or ())))
    else:
        merged = replace(hit, aliases=tuple(hit.aliases or ()) + (mine,),
                         names=_other_names(hit.label, *(hit.names or ()), sp.label))
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


def _concat(a, b):
    """Two pieces' lines, end to end (the lower ends where the upper starts)."""
    if a is None or a.is_empty:
        return b
    if b is None or b.is_empty:
        return a
    try:
        return LineString(list(a.coords) + list(b.coords)[1:])
    except (NotImplementedError, AttributeError):          # a MultiLineString: merge what touches
        from shapely.ops import linemerge
        return linemerge([a, b])


def _shift_cut(graph, geoms, blk, frm: float, bnd: SectionBoundary, fid_index, by_blk=None,
               edges_by_to=None, edges_by_from=None) -> bool:
    """Move the standing cut at ``frm`` to ``bnd.route_measure`` — a MINTED cut joining an authored
    point a sliver away (an authored cut never moves, owner 2026-10-24). The two pieces it separates
    keep their own ids and attributes (an out-of-B.C. flag stays on its side); their measures,
    geometry, fids and the edges between ``frm`` and the new measure are re-homed, and the bound on
    both becomes ``bnd``. False (nothing changed) when there is no piece on each side or the move
    would cross another boundary."""
    M = bnd.route_measure
    nids = list(by_blk.get(blk, ())) if by_blk is not None else list(graph.nodes)
    lo = hi = None
    for nid in nids:
        n = graph.nodes.get(nid)
        if n is None or n.kind != NodeKind.stream or n.blk != blk:
            continue
        if abs(n.up_m - frm) < 1e-6:
            lo = nid
        elif abs(n.down_m - frm) < 1e-6:
            hi = nid
    if lo is None or hi is None:
        return False
    A, B = graph.nodes[lo], graph.nodes[hi]
    if not (A.down_m < M < B.up_m):
        return False
    gA, gB = geoms.get(lo), geoms.get(hi)
    full = _concat(gA, gB)
    cx = cy = 0.0
    if full is not None and not full.is_empty:
        a = min(max(M - A.down_m, 0.0), full.length)
        pt = full.interpolate(a)
        cx, cy = pt.x, pt.y
        geoms[lo] = substring(full, 0.0, a)
        geoms[hi] = substring(full, a, full.length)
    fids = tuple(dict.fromkeys(tuple(A.member_fids) + tuple(B.member_fids)))
    lo_ord, lo_mag, lo_fids = _repartition(fids, fid_index, A.down_m, M)
    hi_ord, hi_mag, hi_fids = _repartition(fids, fid_index, M, B.up_m)
    graph.nodes[lo] = replace(A, up_m=M, length_m=max(M - A.down_m, 0.0), upper_bound=bnd,
                              stream_order=lo_ord if fid_index else A.stream_order,
                              stream_magnitude=lo_mag if fid_index else A.stream_magnitude,
                              member_fids=lo_fids if fid_index else A.member_fids)
    graph.nodes[hi] = replace(B, down_m=M, length_m=max(B.up_m - M, 0.0), lower_bound=bnd,
                              stream_order=hi_ord if fid_index else B.stream_order,
                              stream_magnitude=hi_mag if fid_index else B.stream_magnitude,
                              member_fids=hi_fids if fid_index else B.member_fids)

    def _leaves_above(e) -> Optional[bool]:
        if full is None or full.is_empty or (not e.x and not e.y):
            return None
        return A.down_m + full.project(Point(e.x, e.y)) >= M

    def _move(i, field, new):
        old = getattr(graph.edges[i], field)
        graph.edges[i] = replace(graph.edges[i], **{field: new})
        idx = edges_by_to if field == "to_node" else edges_by_from
        if idx is not None:
            idx[old] = [j for j in idx.get(old, []) if j != i]
            idx.setdefault(new, []).append(i)

    cand_to = (list(edges_by_to.get(lo, [])) + list(edges_by_to.get(hi, []))) if edges_by_to is not None \
        else [i for i, e in enumerate(graph.edges) if e.to_node in (lo, hi)]
    cand_from = (list(edges_by_from.get(lo, [])) + list(edges_by_from.get(hi, []))) \
        if edges_by_from is not None else [i for i, e in enumerate(graph.edges) if e.from_node in (lo, hi)]
    for i in sorted(set(cand_to) | set(cand_from)):
        e = graph.edges[i]
        if e.kind == "continuation" and e.from_node == hi and e.to_node == lo:
            graph.edges[i] = replace(e, at_measure=M, x=cx, y=cy)      # the cut itself
            continue
        if i in cand_to:                                 # a tributary IN: by its measure
            want = hi if e.at_measure >= M else lo
            if e.to_node != want:
                _move(i, "to_node", want)
        if i in cand_from:                               # a flow OUT: by where it leaves the line
            above = _leaves_above(graph.edges[i])
            if above is not None:
                want = hi if above else lo
                if graph.edges[i].from_node != want:
                    _move(i, "from_node", want)
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
                   conflicts: Optional[list] = None, fold_wbks=None) -> StreamGraph:
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
    ``fold_wbks`` (optional): the polygons folded into their stream (`place_points`); None reads the graph.
    """
    from dataclasses import replace as _replace
    fold = _fold_wbks(graph, fold_wbks)
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
            got = _alias_onto_lake(graph, blk, sp, node_by_blk, fold)
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
        for place in _plan_blk(graph, blk, sps, node_by_blk.get(blk, ()), conflicts, fold=fold):
            bnd = _place_boundary(place)
            canon = _canonical(place)
            cut = False
            if place.from_m is not None:
                # a standing MINTED cut joins the authored point: it moves, the name does not
                cut = _shift_cut(graph, geoms, blk, place.from_m, bnd, fid_index, node_by_blk,
                                 edges_by_to, edges_by_from)
                if not cut:          # refused: cut the name where it is anyway; the sliver gate sees it
                    cut = _split_one(graph, geoms, blk, canon, fid_index, node_by_blk,
                                     edges_by_to, edges_by_from, bnd=bnd)
            elif place.existing:
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
                              picked_up=place.stands or (cut and sp is not canon))
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
