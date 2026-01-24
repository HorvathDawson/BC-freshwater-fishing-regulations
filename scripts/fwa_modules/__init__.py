"""
FWA Processing Modules - Graph-Based Stream Network Analysis

This package provides modules for processing BC Freshwater Atlas data:
- graph_builder: Build stream network graph with tributary enrichment
- index_builder: Build searchable JSON index for web application
"""

from .graph_builder import FWAPrimalGraph
from .index_builder import IndexBuilder

__all__ = [
    "FWAPrimalGraph",
    "IndexBuilder",
]
