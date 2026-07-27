"""Split anchor resolution (04): an authored SplitDef -> concrete SplitPoint(s) on blue line(s).

Each anchor normalizes to an absolute DOWNSTREAM_ROUTE_MEASURE on a BLK. Implemented now:
- `point` : project the coord onto the target blue line; keep it iff within `proximity_m`.
- `line`  : intersect the cut line with the target blue line; measure the crossing.
Deferred (need lake/MU/graph context): `lake`, `mu_boundary`, `confluence`.

Target scope selects the eligible blue lines: `blk` (one), `gnis_id` (all with that gnis), or
`wsc` (river + side channels). A `point`/`line` may resolve to several BLKs under a gnis/wsc
target — one SplitPoint each — which is how one authored cut lands on a main + side channels.
"""

from __future__ import annotations

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


def _measure_on(sd: SplitDef, chain: BlkChain) -> Optional[float]:
    g = chain.geometry
    if g is None or g.is_empty:
        return None
    a = sd.anchor
    if a.type == AnchorType.point and a.coord is not None:
        p = Point(a.coord)
        d = g.project(p)
        if g.interpolate(d).distance(p) > sd.proximity_m:
            return None                       # this blue line is too far from the cut point
        return chain.mouth_measure + d
    if a.type == AnchorType.line and a.coords:
        inter = g.intersection(LineString(a.coords))
        if inter.is_empty:
            return None
        p = inter if inter.geom_type == "Point" else inter.representative_point()
        return chain.mouth_measure + g.project(p)
    return None                               # lake / mu_boundary / confluence -> follow-up


def resolve_split_defs(split_defs: list[SplitDef], chains: list[BlkChain]) -> list[SplitPoint]:
    by_blk = {c.blk: c for c in chains}
    out: list[SplitPoint] = []
    for sd in split_defs:
        for blk in _target_blks(sd, chains):
            c = by_blk.get(blk)
            if c is None:
                continue
            m = _measure_on(sd, c)
            if m is None:
                continue
            out.append(SplitPoint(
                split_id=sd.id, blk=blk, route_measure=float(m), fid="",
                label=(sd.label or sd.id), anchor_type=sd.anchor.type))  # boundary's own name
    return out
