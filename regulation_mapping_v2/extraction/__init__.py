"""extraction — PDF extraction for synopsis data (v2 standalone).

Self-contained extraction package for BC Freshwater Fishing Regulations.
Contains complete extraction logic, models, and CLI entry points.

Usage
-----
    python -m regulation_mapping_v2.extraction.extract_synopsis <pdf> <output_dir>
"""

from .models import ExtractionResults, PageMetadata, PageResult, WaterbodyRow
from .extract_synopsis import FishingSynopsisParser, SynopsisExtractor

__all__ = [
    "ExtractionResults",
    "FishingSynopsisParser",
    "PageMetadata",
    "PageResult",
    "SynopsisExtractor",
    "WaterbodyRow",
]
