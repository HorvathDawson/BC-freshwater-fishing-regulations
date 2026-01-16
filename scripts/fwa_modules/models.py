"""
Simplified data models for FWA processing.

Removed complex network structures in favor of simple attribute-based processing.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class ProcessingStats:
    """Track statistics for logging."""

    total_streams: int = 0
    originally_named: int = 0
    inherited_same_code: int = 0  # Unnamed streams with same code as named
    river_tributaries: int = 0  # Assigned via parent watershed code
    lake_tributaries: int = 0  # Assigned via WATERBODY_KEY
    braids_fixed: int = 0  # Braids that inherited names
    final_named_count: int = 0
    total_lakes: int = 0
    total_wetlands: int = 0
    total_manmade: int = 0
    total_kml_points: int = 0
