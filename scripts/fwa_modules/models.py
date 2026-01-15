"""
Data models for FWA processing using attrs for type safety and performance.

All models are frozen (immutable) and use slots for memory efficiency.
"""

import attrs
from typing import Optional, Tuple, List


@attrs.define(frozen=True, slots=True, cache_hash=True)
class StreamNode:
    """Network node representing stream endpoint.

    Attributes:
        node_id: Unique identifier (e.g., "layer_idx_start" or "layer_idx_end")
        position: (x, y) coordinates in BC Albers (EPSG:3005)
        watershed_code: FWA watershed code
        is_lake_node: True if this node is inside a lake polygon
        lake_name: Name of lake if is_lake_node is True
        lake_poly_id: Polygon ID of lake if is_lake_node is True
    """

    node_id: str
    position: Tuple[float, float]
    watershed_code: str
    is_lake_node: bool = False
    lake_name: Optional[str] = None
    lake_poly_id: Optional[int] = None


@attrs.define(frozen=True, slots=True)
class StreamEdge:
    """Network edge representing stream segment.

    Attributes:
        linear_feature_id: FWA linear feature ID
        gnis_name: GNIS name if stream is named, None otherwise
        clean_code: Cleaned FWA watershed code (no 000000 segments)
        local_code: Cleaned LOCAL watershed code (junction position)
        parent_code: Parent watershed code
        route_measure: Downstream route measure
        waterbody_key: WATERBODY_KEY if stream is in/touches a lake
        layer_name: Source layer name
        start_node_id: ID of upstream node
        end_node_id: ID of downstream node
    """

    linear_feature_id: str
    gnis_name: Optional[str]
    clean_code: str
    local_code: Optional[str]
    parent_code: Optional[str]
    route_measure: float
    waterbody_key: Optional[int]
    layer_name: str
    start_node_id: str
    end_node_id: str

    @property
    def is_named(self) -> bool:
        """True if this stream segment has a GNIS name."""
        return self.gnis_name is not None and self.gnis_name.strip() != ""


@attrs.define
class TributaryAssignment:
    """Tributary relationship result.

    Attributes:
        linear_feature_id: Stream segment ID
        tributary_of: Name of waterbody this flows into
        lake_poly_id: ID of lake if segment is mostly inside lake
        distance_to_named: Topological distance to nearest named feature
    """

    linear_feature_id: str
    tributary_of: Optional[str] = None
    lake_poly_id: Optional[int] = None
    distance_to_named: Optional[int] = None


@attrs.define
class ZoneAssignment:
    """Zone membership for a feature.

    Attributes:
        zones: Zone IDs ordered by proportion (largest first)
        proportions: Proportion of feature in each zone (0-1, sums to ~1)
        is_clipped: True if feature was split across multiple zones
    """

    zones: List[str] = attrs.field(factory=list)
    proportions: List[float] = attrs.field(factory=list)
    is_clipped: bool = False

    def primary_zone(self) -> Optional[str]:
        """Get the zone containing the largest proportion of the feature."""
        return self.zones[0] if self.zones else None

    def add_zone(self, zone: str, proportion: float):
        """Add a zone assignment and resort by proportion.

        Args:
            zone: Zone ID
            proportion: Proportion of feature in this zone
        """
        self.zones.append(zone)
        self.proportions.append(proportion)

        # Sort by proportion descending
        sorted_pairs = sorted(
            zip(self.proportions, self.zones), key=lambda x: x[0], reverse=True
        )
        self.proportions, self.zones = zip(*sorted_pairs) if sorted_pairs else ([], [])
        self.proportions = list(self.proportions)
        self.zones = list(self.zones)

        self.is_clipped = len(self.zones) > 1


@attrs.define(frozen=True, slots=True)
class StreamMetadata:
    """Lightweight stream metadata for graph building (no geometry).

    Used in first pass to build network without loading geometries.

    Attributes:
        linear_feature_id: FWA linear feature ID
        watershed_code: FWA watershed code
        gnis_name: GNIS name if named
        route_measure: Downstream route measure
        layer_name: Source layer
        start_point: (x, y) of upstream endpoint
        end_point: (x, y) of downstream endpoint
    """

    linear_feature_id: str
    watershed_code: str
    gnis_name: Optional[str]
    route_measure: float
    layer_name: str
    start_point: Tuple[float, float]
    end_point: Tuple[float, float]
