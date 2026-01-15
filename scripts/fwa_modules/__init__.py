"""
FWA Processing Modules - Memory-Efficient Stream Processing

This package provides modular components for processing BC Freshwater Atlas data
with a focus on memory efficiency and correctness.
"""

from .utils import (
    clean_watershed_code,
    get_parent_code,
    get_code_depth,
    setup_logging,
)

from .models import (
    StreamNode,
    StreamEdge,
    TributaryAssignment,
    ZoneAssignment,
    StreamMetadata,
)

__all__ = [
    "clean_watershed_code",
    "get_parent_code",
    "get_code_depth",
    "setup_logging",
    "StreamNode",
    "StreamEdge",
    "TributaryAssignment",
    "ZoneAssignment",
    "StreamMetadata",
]
