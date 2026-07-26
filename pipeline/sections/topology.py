"""Step 3-4 (03 S3-4): build the contracted topology graph.

1. Build the fine directed micro-graph (edges downstream; reverse adj = upstream).
2. Collapse each lake/manmade WATERBODY_KEY to ONE barrier node; contract degree-2
   pass-throughs into maximal BLK-run Segments.
3. Insert barrier:true split nodes. Keep the strict "missing edge_type raises" guard.

NOTE (spike 10): FWA is already a DAG, so SCC condensation is a no-op and is NOT used. Every
Segment carries fwa_watershed_code + edge_type so the tributary walk (tributaries.py) can
apply the WSC-descendant filter and the EDGE_TYPE=2300 barrier — both REQUIRED (a naive
directional walk leaks the parent mainstem at confluences and across 2300 canals).
"""

from __future__ import annotations

from .models import BlkChain, Topology


def build_topology(chains: list[BlkChain], lake_manmade_wbks: set[str],
                   barrier_split_nodes: list | None = None) -> Topology:
    """Return the condensed, contracted topology graph."""
    raise NotImplementedError
