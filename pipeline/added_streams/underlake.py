"""Tag the under-lake portions of an added stream with the FWA lake's WATERBODY_KEY.

Municipal stream lines carry no waterbody key, but FWA breaks a blue line at each lake it runs under
(those fids carry the lake's wbk) so `build_blk_chains` can hand the under-lake run to the lake node.
`assign_under_lake` reproduces that: it splits a line where it crosses FWA lake/manmade polygons and
tags the inside pieces with that polygon's wbk, so the minted stream ties into the EXISTING lake node.
"""

from __future__ import annotations

from typing import Any

from shapely.geometry import LineString
from shapely.strtree import STRtree


class LakeIndex:
    """STRtree over FWA lake/manmade polygons (EPSG:3005) with a parallel wbk array."""
    def __init__(self, polys: list[tuple[Any, str]]):
        self.geoms = [g for g, _ in polys]
        self.wbk = [str(w) for _, w in polys]
        self.tree = STRtree(self.geoms) if self.geoms else None

    def candidates(self, line: LineString) -> list[int]:
        if self.tree is None:
            return []
        return [int(i) for i in self.tree.query(line)]


def _lines(geom) -> list[LineString]:
    if geom is None or geom.is_empty:
        return []
    if geom.geom_type == "LineString":
        return [geom] if geom.length > 0 else []
    if geom.geom_type in ("MultiLineString", "GeometryCollection"):
        out: list[LineString] = []
        for g in geom.geoms:
            out.extend(_lines(g))
        return out
    return []


def assign_under_lake(line: LineString, index: LakeIndex) -> list[tuple[LineString, str]]:
    """Split ``line`` (EPSG:3005) at FWA lake/manmade polygon boundaries -> ordered
    (segment, wbk) pairs mouth->source; inside-a-lake segments carry that lake's wbk, others ``""``.
    No lake crossing -> ``[(line, "")]``."""
    inside: list[tuple[LineString, str]] = []
    union = None
    for i in index.candidates(line):
        poly = index.geoms[i]
        if not line.intersects(poly):
            continue
        for g in _lines(line.intersection(poly)):
            inside.append((g, index.wbk[i]))
        union = poly if union is None else union.union(poly)
    if union is None:
        return [(line, "")]
    pieces = inside + [(g, "") for g in _lines(line.difference(union))]
    pieces.sort(key=lambda gw: line.project(gw[0].interpolate(0.5, normalized=True)))
    return pieces
