"""Step 6 (03 S6): build the SectionGraph and precompute tributary reachability.

The walk runs on Topology.up_adj from every node a section spans, stopping at is_barrier
nodes (lakes, barrier splits). Collected segments map through segment_to_section ->
tributary_section_ids. Cache by seed-set. tributary_only regs assign only the trib set.

REQUIRED guards (spike 10 — directionality alone is NOT enough):
- WSC-descendant filter: only include segments whose fwa_watershed_code is a descendant
  (prefix-extension) of the seed section's trimmed WSC. Stops the confluence-parent leak
  (Chehalis climbing into the Harrison mainstem).
- EDGE_TYPE=2300 barrier: may pass through consecutive 2300 segments but not exit a 2300 back
  into a non-2300 segment. Stops the Kootenay<->Columbia canal leak (lake-collapse does not).
"""

from __future__ import annotations

from .models import Section, SectionGraph, Topology


def build_section_graph(sections: list[Section], topology: Topology) -> SectionGraph:
    raise NotImplementedError


def tributary_section_ids(section_id: str, graph: SectionGraph, topology: Topology,
                          cross_lakes: bool = False) -> tuple[str, ...]:
    """Upstream closure of a section over the topology, honoring barriers."""
    raise NotImplementedError
