"""
AdminTarget — Paired (layer, feature_id) type for admin polygon matching.

Each AdminTarget identifies one or more admin polygons within a specific
layer, enabling multi-layer matching within a single regulation.

Used by AdminDirectMatch, ProvincialRegulation, and ZoneRegulation.
"""

from typing import NamedTuple, Optional


class AdminTarget(NamedTuple):
    """Identifies admin polygon(s) within a specific layer.

    Args:
        layer: Admin layer key (e.g., "parks_bc", "watersheds").
            See ADMIN_LAYER_CONFIG in metadata_builder.py for valid keys.
        feature_id: Specific polygon ID within the layer. If None, selects
            all polygons in the layer (optionally narrowed by code_filter).
        code_filter: Classification code to pre-filter the layer
            (e.g., "OI" for ecological reserves in parks_bc).
    """

    layer: str
    feature_id: Optional[str] = None
    code_filter: Optional[str] = None
