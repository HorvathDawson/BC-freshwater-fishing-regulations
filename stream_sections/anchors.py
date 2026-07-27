"""Split anchor resolution (04): an authored SplitDef -> concrete SplitPoint(s) on blue line(s).

Every anchor normalizes to an absolute DOWNSTREAM_ROUTE_MEASURE on a target BLK, using geometry
(so it runs before/independent of the graph). Target scope selects the eligible blue lines:
`blk` (one), `gnis_id` (all with that gnis), or `wsc` (river + side channels) — a point/line/mu
cut may thus land on several BLKs (main + side channels), one SplitPoint each.

- point       : project the coord onto the target blue line (kept iff within `proximity_m`).
- line        : intersect the cut line with the target blue line; measure the crossing.
- confluence  : the tributary (by `tributary_blk` or `tributary_wsc`) has a mouth; project it
                onto the target (parent) blue line -> the confluence measure. The parent may be
                unnamed (author supplies `label` + a name override elsewhere).
- lake        : the target's waterbody-run for `wbk` -> its boundary measure. NOTE lakes already
                split the BLK in the graph build, so this is a no-op split; it exists so a
                lake-anchored reg resolves to the (already-present) boundary for matching.
- mu_boundary : the shared boundary line of two ADJACENT MUs (`mu_a`,`mu_b`) intersected with the
                target blue line -> the crossing measure(s). Needs `mu_polys` context.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Optional

from shapely.geometry import LineString, Point

from .models import AnchorType, BlkChain, SplitDef, SplitPoint


def _target_blks(sd: SplitDef, chains: list[BlkChain]) -> list[str]:
    if sd.blk:
        return [sd.blk]
    if sd.gnis_id:
        return [c.blk for c in chains if c.gnis_id == sd.gnis_id]
    if sd.wsc:
        return [c.blk for c in chains if c.fwa_watershed_code == sd.wsc]
    return []


def _points(geom) -> list[Point]:
    """Extract crossing points from a stream ∩ boundary result (Point/MultiPoint/Line/collection).
    For an overlap LineString (river running along the boundary) use its endpoints (enter/exit)."""
    if geom is None or geom.is_empty:
        return []
    t = geom.geom_type
    if t == "Point":
        return [geom]
    if t in ("MultiPoint", "GeometryCollection", "MultiLineString"):
        out: list[Point] = []
        for g in geom.geoms:
            out.extend(_points(g))
        return out
    if t == "LineString":
        c = list(geom.coords)
        return [Point(c[0]), Point(c[-1])]
    return []


def _tributary_chain(anchor, by_blk, by_wsc) -> Optional[BlkChain]:
    if anchor.tributary_blk:
        return by_blk.get(anchor.tributary_blk)
    if anchor.tributary_wsc:
        cands = by_wsc.get(anchor.tributary_wsc, [])
        # the main channel of that WSC (side channels share it) = the largest.
        return max(cands, key=lambda c: (c.stream_magnitude or 0, c.length_m)) if cands else None
    return None


def resolve_split_defs(split_defs: list[SplitDef], chains: list[BlkChain],
                       mu_polys: Optional[dict] = None) -> list[SplitPoint]:
    by_blk = {c.blk: c for c in chains}
    by_wsc: dict[str, list[BlkChain]] = defaultdict(list)
    for c in chains:
        by_wsc[c.fwa_watershed_code].append(c)
    out: list[SplitPoint] = []

    def _emit(sd, blk, m):
        out.append(SplitPoint(split_id=sd.id, blk=blk, route_measure=float(m), fid="",
                              label=(sd.label or sd.id), anchor_type=sd.anchor.type))

    for sd in split_defs:
        a = sd.anchor
        targets = [(blk, by_blk[blk]) for blk in _target_blks(sd, chains) if blk in by_blk]

        if a.type in (AnchorType.point, AnchorType.line):
            for blk, c in targets:
                g = c.geometry
                if g is None or g.is_empty:
                    continue
                if a.type == AnchorType.point and a.coord is not None:
                    p = Point(a.coord)
                    d = g.project(p)
                    if g.interpolate(d).distance(p) <= sd.proximity_m:
                        _emit(sd, blk, c.mouth_measure + d)
                elif a.type == AnchorType.line and a.coords:
                    for p in _points(g.intersection(LineString(a.coords))):
                        _emit(sd, blk, c.mouth_measure + g.project(p))

        elif a.type == AnchorType.confluence:
            trib = _tributary_chain(a, by_blk, by_wsc)
            if trib is None or trib.geometry is None:
                continue
            mouth = Point(trib.geometry.coords[0])
            for blk, c in targets:
                if c.geometry is None:
                    continue
                d = c.geometry.project(mouth)
                if c.geometry.interpolate(d).distance(mouth) <= sd.proximity_m:
                    _emit(sd, blk, c.mouth_measure + d)

        elif a.type == AnchorType.lake:
            for blk, c in targets:
                for run in c.waterbody_runs:
                    if str(run.wbk) == a.wbk:
                        _emit(sd, blk, run.down_m)   # no-op split (lake already split the BLK)

        elif a.type == AnchorType.mu_boundary:
            if not mu_polys:
                continue
            pa, pb = mu_polys.get(a.mu_a), mu_polys.get(a.mu_b)
            if pa is None or pb is None:
                continue
            shared = pa.boundary.intersection(pb.boundary)
            if shared.is_empty:
                continue                              # MUs are not adjacent -> nothing to cut
            for blk, c in targets:
                if c.geometry is None:
                    continue
                for p in _points(c.geometry.intersection(shared)):
                    _emit(sd, blk, c.mouth_measure + c.geometry.project(p))

    return out
