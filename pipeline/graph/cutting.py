"""Shared geometry + id helpers for the section pipeline.

- endpoint_id : node id for a coordinate (mirrors graph_builder get_endpoints, 3-dp).
- line_coords_2d / merge_ordered : flatten 3D (Multi)LineStrings and stitch a BLK chain.
- substring_cut / section_id : used by the later `sections` step (03 S5).
"""

from __future__ import annotations

import hashlib
from typing import Any, Optional

from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge, substring

_COORD_PRECISION = 3  # decimals; must match pipeline/graph/graph_builder.py get_endpoints


def endpoint_id(x: float, y: float) -> str:
    """Node id for a coordinate. Mirrors graph_builder.get_endpoints (3-dp rounding)."""
    return f"{round(x, _COORD_PRECISION)}_{round(y, _COORD_PRECISION)}"


def line_coords_2d(geom: Any) -> list[tuple[float, float]]:
    """Return the ordered 2D coordinate list of a (Multi)LineString.

    FWA geometry is native downstream->upstream (coords[0] = mouth). MultiLineStrings are
    merged first; if a merge still yields multiple parts, the longest is used (rare braided
    rows).
    """
    if geom is None:
        return []
    if isinstance(geom, MultiLineString):
        merged = linemerge(geom)
        if isinstance(merged, MultiLineString):
            if len(merged.geoms) == 0:
                return []
            merged = max(merged.geoms, key=lambda g: g.length)
        geom = merged
    if isinstance(geom, LineString):
        return [(float(c[0]), float(c[1])) for c in geom.coords]
    return []


def blk_endpoints(geom: Any) -> tuple[Optional[str], Optional[str]]:
    """(down_node, up_node): coords[0]=mouth(down), coords[-1]=source(up)."""
    coords = line_coords_2d(geom)
    if len(coords) < 2:
        return (None, None)
    return (endpoint_id(*coords[0]), endpoint_id(*coords[-1]))


def merge_ordered(geoms: list[Any]) -> LineString:
    """Stitch geometries already sorted mouth->source into one 2D LineString."""
    coords: list[tuple[float, float]] = []
    for g in geoms:
        gc = line_coords_2d(g)
        if not gc:
            continue
        if coords and coords[-1] == gc[0]:
            coords.extend(gc[1:])   # drop the duplicate shared vertex
        else:
            coords.extend(gc)
    return LineString(coords) if len(coords) >= 2 else LineString()


def substring_cut(geometry: Any, mouth_measure: float, start_m: float, end_m: float) -> Any:
    """Sub-LineString between absolute route measures [start_m, end_m] (03 S5).

    Offsets are measured from the merged geometry's start (the mouth). For 2-point straight
    LineStrings shapely interpolates linearly, which is the documented fallback (10 S3).
    """
    a = max(0.0, start_m - mouth_measure)
    b = end_m - mouth_measure
    return substring(geometry, a, b)


def section_id(blk: str, lower_boundary_id: str, upper_boundary_id: str,
               lake_wbk: str = "", lower_route_measure: Optional[float] = None) -> str:
    """Stable section id from the two immediate bounds (ABI — docs/02).

    Readable form when a start measure is known: f"{blk}:{int(lower_route_measure)}".
    Otherwise sha1 over (blk|lower|upper|lake_wbk)[:16].
    """
    if lower_route_measure is not None:
        return f"{blk}:{int(round(lower_route_measure))}"
    raw = "|".join([blk, lower_boundary_id, upper_boundary_id, lake_wbk or ""])
    return hashlib.sha1(raw.encode()).hexdigest()[:16]
