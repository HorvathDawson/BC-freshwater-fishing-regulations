"""The stream graph (INVERTED): a NODE is a stream piece or a lake; an EDGE is "flows into".

A mainstem node has many incoming edges (its tributaries) and one outgoing edge (its own
confluence). fids never appear in the graph — only inside StreamNode as provenance. Tributaries of a
node = its ANCESTORS (walk incoming edges upstream).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from pipeline.common.models.enums import NodeKind
from pipeline.common.models.names import NameTuple
from pipeline.common.models.sections import SectionBoundary


@dataclass(frozen=True)
class StreamNode:
    """A graph node: a stream piece (a BLK cut at lake-runs + curated splits) or a lake.

    Geometry is NOT stored here — the graph is pure topology + attributes. Geometry is looked
    up by ``node_id`` in the sidecar produced by ``graph.build_section_geometries`` (so the
    graph pickle stays light and a geometry-free consumer never pays for shapely).
    """
    node_id: str                 # stream piece: "{blk}:{int(down_m)}"; lake: "lake:{wbk}"
    kind: NodeKind               # stream | lake
    blk: str = ""                # "" for lake nodes
    wbk: str = ""                # set for lake nodes
    wsc: str = ""
    gnis_id: str = ""
    display_name: str = ""
    name_tuples: tuple[NameTuple, ...] = ()
    through_names: tuple[str, ...] = ()         # lake nodes: GNIS names of rivers threading it
    down_m: float = 0.0          # stream piece: measure sub-range on its BLK; 0 for lakes
    up_m: float = 0.0
    length_m: float = 0.0
    stream_order: Optional[int] = None
    stream_magnitude: Optional[int] = None      # max in this piece -> front-end line weight
    member_fids: tuple[str, ...] = ()
    member_wbks: tuple[str, ...] = ()           # non-lake wbks the fids pass through (wetland/river
                                                # OVERLAYS — named, but NOT nodes/splits/barriers)
    edge_types: tuple[str, ...] = ()            # this piece's distinct EDGE_TYPEs; "2300" => barrier
    out_of_bc: bool = False                      # piece lies OUTSIDE the BC boundary (a cross-border
                                                # blk, split at the provincial outline). Geometry is
                                                # KEPT (unlike under-lake) for dotted display; NOT a
                                                # barrier — BC regs simply don't apply here.
    mus: tuple[str, ...] = ()                    # wildlife management units this piece TOUCHES
                                                # ("2-8", "2-9"). Administrative geography, not a
                                                # regulation: every square metre of BC is in an MU
                                                # whether or not anything is regulated there, which
                                                # is why this may ride on a tile feature while
                                                # `in_areas` (regulated areas only) may not.
                                                # 99.14% of sections touch exactly one; 16,505 touch
                                                # two; 196 touch three or more. Zone regs resolve
                                                # through this — without it an unnamed stream has no
                                                # MU and no zone rule can reach it.
    in_areas: tuple[str, ...] = ()               # admin/park polygons (by label) this piece falls
                                                # INSIDE, set by an `area_boundary` split's inside-flag
                                                # pass. A geometric fact only (rule-agnostic); Phase-5
                                                # matching maps an area -> its closure reg. Drives the
                                                # "within {area}" location_identifier.
    # Structured bounds (04): each end is a lake/split/confluence/mu boundary, or None = natural
    # (outlet toward the mouth, headwaters toward the source). Carry route_measure so a range
    # regulation ("X from lake A to lake C") selects pieces by measure. See location_identifier.
    lower_bound: Optional["SectionBoundary"] = None   # toward the mouth
    upper_bound: Optional["SectionBoundary"] = None   # toward the source

    @property
    def is_barrier(self) -> bool:
        """A 2300 (canal/artificial connector) node stops the tributary walk (spike S2)."""
        return "2300" in self.edge_types

    @property
    def location_identifier(self) -> Optional[str]:
        """Human qualifier derived from the two bounds (04 table). None when the piece spans the
        whole named stream. The display_name (the river name) is unaffected — a piece is always
        e.g. 'Adams River' with an optional qualifier 'downstream of Adams Lake'."""
        if self.in_areas:
            # An inside-an-area piece reads 'within {area}' regardless of which side of the
            # polygon boundary its up/down bounds are — point-in-polygon is the source of truth,
            # so a boundary→headwaters reach INSIDE a park reads 'within', not 'upstream of'.
            return "within " + " and ".join(self.in_areas)
        lo = self.lower_bound.label if self.lower_bound else ""
        hi = self.upper_bound.label if self.upper_bound else ""
        if not lo and not hi:
            return None
        if not lo:
            return f"downstream of {hi}"
        if not hi:
            return f"upstream of {lo}"
        return f"between {lo} and {hi}"


@dataclass(frozen=True)
class FlowEdge:
    """``from_node`` flows into ``to_node`` at ``at_measure`` on the to_node's BLK."""
    from_node: str
    to_node: str
    at_measure: float            # route measure on to_node where the confluence is
    x: float = 0.0               # confluence coordinate (EPSG:3005) — for review/gpkg
    y: float = 0.0
    kind: str = "confluence"     # confluence | lake_in | lake_out | outlet


@dataclass
class StreamGraph:
    """The single stream graph. up_adj[node] = incoming edges = the tributary/ancestor walk."""
    nodes: dict[str, StreamNode] = field(default_factory=dict)
    edges: list[FlowEdge] = field(default_factory=list)
    up_adj: dict[str, list[int]] = field(default_factory=dict)    # to_node -> edge indices (tributaries in)
    down_adj: dict[str, list[int]] = field(default_factory=dict)  # from_node -> edge indices (flows out)


# Which edges continue the SAME water, rather than joining another to it.
#
# One definition, in the module both walkers already import. It was declared twice — in
# `pipeline/atlas/graph/tributaries.py` and `pipeline/atlas/graph/tributaries.py` — for two genuinely
# different algorithms (a set subtraction for one section, a refusing walk over a reach).
# The algorithms should differ; what counts as "still the same river" must not, or the two
# disagree about which water a rule reaches.
MAINSTEM_EDGE_KINDS = frozenset({"continuation", "lake_out"})
