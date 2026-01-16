"""
FWA Processing Modules - Simplified Watershed Hierarchy Approach

This package provides modular components for processing BC Freshwater Atlas data
using watershed code hierarchy instead of complex network analysis.
"""

from .utils import (
    clean_watershed_code,
    get_parent_code,
    get_code_depth,
    setup_logging,
)

from .models import ProcessingStats

__all__ = [
    "clean_watershed_code",
    "get_parent_code",
    "get_code_depth",
    "setup_logging",
    "ProcessingStats",
]
