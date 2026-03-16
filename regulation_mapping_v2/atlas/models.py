"""
Immutable data records for the FreshWaterAtlas.

All records are frozen dataclasses — once built, they never change.
These carry zero regulation data; only permanent geographic attributes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from shapely.geometry.base import BaseGeometry


@dataclass(frozen=True)
class StreamRecord:
    """One atomic FWA stream segment (graph edge)."""

    fid: str  # linear_feature_id — the permanent stable ID
    geometry: BaseGeometry  # Full polyline from GPKG (fallback: 2-point LineString)
    display_name: str  # gnis_name ("" if unnamed)
    blk: str  # blue_line_key
    stream_order: Optional[int]
    stream_magnitude: Optional[int]
    waterbody_key: str  # "" if open air
    fwa_watershed_code: str = ""  # cleaned FWA watershed code
    minzoom: int = 11


@dataclass(frozen=True)
class PolygonRecord:
    """One waterbody polygon (lake, wetland, or manmade)."""

    waterbody_key: str
    geometry: BaseGeometry
    display_name: str  # GNIS_NAME_1
    area: float  # m²
    gnis_id: str = ""  # GNIS_ID_1 — needed for synopsis lake matching
    minzoom: int = 11


@dataclass(frozen=True)
class AdminRecord:
    """One admin boundary polygon (national park, eco reserve)."""

    admin_id: str
    geometry: BaseGeometry
    display_name: str
    admin_type: str  # "parks_nat" | "eco_reserve"
    area: float  # m²
    minzoom: int = 11
