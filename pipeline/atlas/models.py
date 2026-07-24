"""
Immutable data records for the FreshWaterAtlas.

All records are frozen dataclasses — once built, they never change.
These carry zero regulation data; only permanent geographic attributes.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from shapely.geometry.base import BaseGeometry

# Re-export for backward compatibility — canonical home is pipeline.utils.wsc
from pipeline.utils.wsc import format_wsc_50k, trim_wsc  # noqa: F401


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
    gnis_id: str = ""  # GNIS_ID — the canonical stream→reach grouping key
    fwa_watershed_code: str = ""  # cleaned FWA watershed code
    watershed_code_50k: str = ""  # FWA 1:50k watershed code (padded; "9999…" = unknown)
    minzoom: int = 11


@dataclass(frozen=True)
class PolygonRecord:
    """One waterbody polygon (lake, wetland, or manmade)."""

    waterbody_key: str
    geometry: BaseGeometry
    display_name: str  # GNIS_NAME_1
    area: float  # m²
    gnis_id: str = ""  # GNIS_ID_1 — needed for synopsis lake matching
    waterbody_key_group_code_50k: str = ""  # FWA 1:50k waterbody group code (e.g. "00081HARR")
    watershed_code_50k: str = ""  # FWA 1:50k watershed code (padded; "9999…" = unknown)
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
    restriction_level: str = ""  # "closed" | "restricted" — land_access only
    access: str = ""  # raw OSM access tag ("no" | "private" | "permit" | ...) — land_access only


@dataclass(frozen=True)
class PointRecord:
    """One point-of-interest feature (water access, waterfall)."""

    id: str
    geometry: BaseGeometry  # Point (way-geometry features use their centroid)
    display_name: str
    poi_type: str  # "boat_launch" | "pier" | "fishing_platform" | "waterfall"
    # NOTE: this is the tippecanoe tile-inclusion hint, not the UI zoom gate —
    # keep it low (0) so tippecanoe actually includes these features in the
    # tile pyramid. The tileset's global --maximum-zoom=12 means any
    # per-feature tippecanoe.minzoom above 12 causes tippecanoe to silently
    # drop the feature from every tile (there's no z13+ tile for it to land
    # in). The "only show at z13+" behavior is a client-side concern —
    # handled by the MapLibre *layer's* own `minzoom: 13`, not this field.
    minzoom: int = 0
    extra: str = ""  # small JSON-encoded string for type-specific fields


@dataclass(frozen=True)
class RoadRecord:
    """One BC Forest Service Road section."""

    fid: str
    geometry: BaseGeometry
    display_name: str
    status: str  # LIFE_CYCLE_STATUS_CODE — "ACTIVE" | "RETIRED"
    minzoom: int = 10
