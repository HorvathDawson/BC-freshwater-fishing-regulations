"""
Pure geometry helpers shared by the Canonical Data Store and Exporters.

All functions are stateless — no class instances, no side effects.
"""

from typing import List, Tuple

import numpy as np
import geopandas as gpd
from shapely.geometry import (
    LineString,
    MultiLineString,
    GeometryCollection,
    box,
)
from shapely.geometry.base import BaseGeometry


# ---------------------------------------------------------------------------
# Coordinate rounding
# ---------------------------------------------------------------------------


def round_coords(geom_dict: dict, precision: int = 7) -> dict:
    """Round all coordinates in a ``__geo_interface__`` geometry dict.

    Reduces GeoJSON coordinate precision from float64 (14-15 digits) to
    *precision* decimal places.  7 digits ≈ 1.1 cm at the equator — more
    than sufficient for map display.  This typically halves the byte size
    of coordinate-heavy features.
    """

    def _round(coords):
        """Recursively round nested coordinate tuples/lists."""
        if isinstance(coords, (float, int)):
            return round(coords, precision)
        return [_round(c) for c in coords]

    return {
        **geom_dict,
        "coordinates": _round(geom_dict["coordinates"]),
    }


# ---------------------------------------------------------------------------
# Line merging
# ---------------------------------------------------------------------------


def merge_lines(geom_list: List[LineString]) -> BaseGeometry:
    """Merge a list of line geometries into a single geometry.

    Returns a ``MultiLineString`` when *geom_list* contains more than
    one element; otherwise returns the single ``LineString`` directly.
    """
    if not geom_list:
        raise ValueError("merge_lines called with empty geometry list")
    return MultiLineString(geom_list) if len(geom_list) > 1 else geom_list[0]


# ---------------------------------------------------------------------------
# Bounding-box helpers
# ---------------------------------------------------------------------------


def geoms_to_wgs84_bbox(
    geoms: List[BaseGeometry],
) -> Tuple[float, float, float, float]:
    """Compute a WGS 84 bounding box from EPSG:3005 geometries."""
    if not geoms:
        raise ValueError("geoms_to_wgs84_bbox called with empty geometry list")
    bounds = np.array([g.bounds for g in geoms])
    bbox_3005 = box(
        bounds[:, 0].min(),
        bounds[:, 1].min(),
        bounds[:, 2].max(),
        bounds[:, 3].max(),
    )
    return (
        gpd.GeoSeries([bbox_3005], crs="EPSG:3005").to_crs("EPSG:4326").iloc[0].bounds
    )


# ---------------------------------------------------------------------------
# Geometry decomposition
# ---------------------------------------------------------------------------


def extract_line_components(geom: BaseGeometry) -> list:
    """Extract LineString/MultiLineString parts from a geometry result.

    ``intersection()`` and ``difference()`` can return GeometryCollections
    containing points or other degenerate artefacts.  This keeps only
    linear components.
    """
    if geom is None or geom.is_empty:
        return []
    if isinstance(geom, LineString):
        return [geom] if geom.length > 0 else []
    if isinstance(geom, MultiLineString):
        return [g for g in geom.geoms if g.length > 0]
    if isinstance(geom, GeometryCollection):
        parts: list = []
        for g in geom.geoms:
            if isinstance(g, (LineString, MultiLineString)) and g.length > 0:
                if isinstance(g, MultiLineString):
                    parts.extend(p for p in g.geoms if p.length > 0)
                else:
                    parts.append(g)
        return parts
    return []


def extract_geoms(geom: BaseGeometry) -> List[BaseGeometry]:
    """Decompose a multi-geometry into its constituent parts.

    Returns the ``.geoms`` iterator for multi-geometries, or a single-element
    list for simple geometries.
    """
    return list(geom.geoms) if hasattr(geom, "geoms") else [geom]
