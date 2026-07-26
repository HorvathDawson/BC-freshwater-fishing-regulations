"""Data structures for the v2 section pipeline.

Frozen dataclasses + str-valued enums, matching the house style in ``pipeline/atlas/models.py``
and ``pipeline/enrichment/models.py``. Tuples (not lists) on frozen classes for hashability.
JSON-authored classes (``SplitDef``/``SplitAnchor``) carry ``to_dict``/``from_dict`` and
normalize ids on load (``str(int(float(x)))``, ``trim_wsc``) like ``matching/match_table.py``.

Geometry is typed ``Any`` here to keep the module import-light; concrete builders use shapely
``BaseGeometry``. See ``pipeline/redesign/02-domain-model.md`` and ``03-graph-design.md``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional


# --------------------------------------------------------------------------- enums

class NameSource(str, Enum):
    """Provenance of a name; also the display-priority order (high -> low)."""
    override = "override"                 # manual feature_display_names.json
    gazette = "gazette"                   # direct GNIS on this BLK
    side_channel = "side-channel"         # inherited from same-WSC main-channel BLK
    upstream_inherited = "upstream-inherited"  # nearest upstream named edge


class NodeKind(str, Enum):
    confluence = "confluence"   # "x_y" endpoint where >=1 other BLK attaches
    lake = "lake"               # collapsed lake/manmade wbk (barrier)
    barrier = "barrier"         # a barrier:true split node
    outlet = "outlet"           # outdegree 0 (mouth / ocean / border)
    headwater = "headwater"     # indegree 0 (source)


class BoundaryKind(str, Enum):
    outlet = "outlet"
    headwaters = "headwaters"
    lake = "lake"
    split = "split"


class AnchorType(str, Enum):
    lake = "lake"
    confluence = "confluence"
    linear_feature_id = "linear_feature_id"
    landmark = "landmark"
    point = "point"
    border = "border"
    mu_boundary = "mu_boundary"   # management-unit boundary crossing (07)


# --------------------------------------------------------------------------- names

@dataclass(frozen=True)
class NameTuple:
    """One provenance-tagged name. Display = first by NameSource priority; search = all."""
    name: str
    source: NameSource


# --------------------------------------------------------------- blk-chain building blocks

@dataclass(frozen=True)
class FidSpan:
    """One FWA linear feature's route span within a BLK (provenance; never shipped)."""
    fid: str
    down_m: float   # DOWNSTREAM_ROUTE_MEASURE (absolute, mouth-origin)
    up_m: float     # UPSTREAM_ROUTE_MEASURE; (up_m - down_m) == geom length (~cm)


@dataclass(frozen=True)
class WaterbodyRun:
    """A sub-span of a BLK routed through a lake/manmade waterbody (the under-lake distinction)."""
    wbk: str
    down_m: float
    up_m: float
    kind: str  # "lake" | "manmade"


@dataclass(frozen=True)
class BlkChain:
    """A blue line merged from its ordered linear features. Output of the `blk-chains` step."""
    blk: str
    fwa_watershed_code: str            # trim_wsc()'d; 1:1 with blk
    fids: tuple[FidSpan, ...]          # ordered mouth->source by DOWNSTREAM_ROUTE_MEASURE
    geometry: Any                      # merged 2D LineString, coords[0] = mouth
    mouth_measure: float               # down_m of first fid (offset origin for substring)
    length_m: float
    name_tuples: tuple[NameTuple, ...]
    gnis_id: str = ""
    gnis_name: str = ""
    stream_order: Optional[int] = None
    stream_magnitude: Optional[int] = None   # max over fids -> per-section minzoom
    waterbody_runs: tuple[WaterbodyRun, ...] = ()


# ------------------------------------------------------------------- topology graph

@dataclass(frozen=True)
class TopologyNode:
    node_id: str            # "x_y" | "lake:{wbk}" | "split:{split_id}"
    kind: NodeKind
    x: Optional[float] = None
    y: Optional[float] = None
    wbk: str = ""           # set for lake nodes
    is_barrier: bool = False


@dataclass(frozen=True)
class Segment:
    """A topology EDGE: maximal BLK-run between two nodes, directed downstream."""
    segment_id: str         # f"{blk}|{int(down_m)}|{int(up_m)}" (unique per run)
    blk: str
    wsc: str
    from_node: str          # UPSTREAM end
    to_node: str            # DOWNSTREAM end (mouthward)
    down_m: float
    up_m: float
    member_fids: tuple[str, ...] = ()   # composing linear_feature_ids (mouth->source)
    gnis_id: str = ""
    stream_order: Optional[int] = None
    stream_magnitude: Optional[int] = None
    edge_type: str = ""     # FWA class; missing must raise upstream (strict guard)
    geometry: Any = None    # kept OUT of the graph pickle; in companion geoparquet


@dataclass
class Topology:
    """The contracted graph artifact (post SCC-condensation). Geometry stripped from pickle."""
    nodes: dict[str, TopologyNode] = field(default_factory=dict)
    segments: dict[str, Segment] = field(default_factory=dict)
    down_adj: dict[str, list[str]] = field(default_factory=dict)  # node -> downstream segment_ids
    up_adj: dict[str, list[str]] = field(default_factory=dict)    # node -> upstream segment_ids (trib walk)


# ------------------------------------------------------------------------ splits (04)

@dataclass(frozen=True)
class SplitAnchor:
    """WHERE to cut. Only the fields relevant to ``type`` are populated.

    Primary form is ``point`` + a coordinate: snap it to the nearest point on the target
    stream(s) and cut there. Other types locate the position differently.
    """
    type: AnchorType
    coord: Optional[tuple[float, float]] = None   # (x, y) EPSG:3005, or (lng, lat) if is_lonlat
    is_lonlat: bool = False
    wbk: str = ""                 # lake anchor
    fid: str = ""                 # linear_feature_id anchor (exact)
    mu_id: str = ""               # mu_boundary anchor
    name: str = ""                # landmark / confluence descriptor
    tributary_gnis_id: str = ""   # confluence anchor

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SplitAnchor":
        coord = d.get("coord")
        return cls(
            type=AnchorType(d["type"]),
            coord=(float(coord[0]), float(coord[1])) if coord else None,
            is_lonlat=bool(d.get("is_lonlat", False)),
            wbk=str(d.get("wbk", "")), fid=str(d.get("fid", "")),
            mu_id=str(d.get("mu_id", "")), name=str(d.get("name", "")),
            tributary_gnis_id=str(d.get("tributary_gnis_id", "")),
        )


@dataclass(frozen=True)
class SplitDef:
    """One hand-authored split from splits.json. ``id`` is a STABLE key (part of the ABI).

    Target (which streams to cut) is exactly one of:
      - ``blk``     : cut this single blue line (one cut).
      - ``wsc``     : cut EVERY blue line sharing this watershed code (main + side channels)
                      at the anchor — the multi-cut form for zone/section boundaries.
      - ``gnis_id`` : resolve to the named stream's blk(s), then behave like ``blk``.
    """
    id: str
    anchor: SplitAnchor
    blk: str = ""
    wsc: str = ""
    gnis_id: str = ""
    stream_name: str = ""
    label: str = ""
    barrier: bool = False

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SplitDef":
        targets = [k for k in ("blk", "wsc", "gnis_id") if d.get(k)]
        if len(targets) != 1:
            raise ValueError(f"split {d.get('id')!r} must set exactly one of blk/wsc/gnis_id, got {targets}")
        return cls(
            id=str(d["id"]), anchor=SplitAnchor.from_dict(d["anchor"]),
            blk=str(d.get("blk", "")), wsc=str(d.get("wsc", "")),
            gnis_id=str(d.get("gnis_id", "")), stream_name=str(d.get("stream_name", "")),
            label=str(d.get("label", "")), barrier=bool(d.get("barrier", False)),
        )


@dataclass(frozen=True)
class SplitPoint:
    """A resolved split. Written back to splits.resolved.json for reviewable, deterministic builds."""
    split_id: str
    blk: str                # the specific blue line this cut lands on (one per blk for wsc targets)
    route_measure: float    # absolute DOWNSTREAM_ROUTE_MEASURE cut position
    fid: str                # containing fid (stored back)
    label: str
    barrier: bool
    anchor_type: AnchorType
    snap_dist_m: float = 0.0  # distance from the authored coord to the snapped point (review aid)


# ---------------------------------------------------------------------- sections (03/04)

@dataclass(frozen=True)
class SectionBoundary:
    boundary_id: str  # "outlet" | "headwaters" | "lake:{wbk}" | "split:{split_id}"
    kind: BoundaryKind
    route_measure: Optional[float] = None
    label: str = ""


@dataclass(frozen=True)
class Section:
    """The atomic matching/display/tile unit. Output of the `sections` step."""
    section_id: str
    blk: str
    fwa_watershed_code: str
    name_tuples: tuple[NameTuple, ...]
    display_name: str
    location_identifier: Optional[str]   # AUTO per 04 table; None if BLK has no splits
    lower_bound: SectionBoundary         # toward the mouth
    upper_bound: SectionBoundary         # toward the source
    lake_wbk: str = ""
    geometry: Any = None                 # NEW cut geometry via shapely.ops.substring
    bbox: tuple[float, float, float, float] = ()
    length_km: float = 0.0
    member_segment_ids: tuple[str, ...] = ()
    gnis_id: str = ""
    stream_order: Optional[int] = None
    max_magnitude: Optional[int] = None
    min_zoom: int = 11
    mu_ids: tuple[str, ...] = ()         # MUs the section intersects (07)
    tributary_section_ids: tuple[str, ...] = ()
    is_lake: bool = False                # a lake polygon acting as a section (keyed by lake_wbk)


@dataclass
class SectionGraph:
    """Section-level adjacency for tributary walks (walk runs on Topology.up_adj)."""
    segment_to_section: dict[str, str] = field(default_factory=dict)
    section_to_segments: dict[str, tuple[str, ...]] = field(default_factory=dict)
    tributary_index: dict[str, tuple[str, ...]] = field(default_factory=dict)
    # section_id -> (downstream_section_id, upstream_section_id) on the same BLK
    mainstem_neighbors: dict[str, tuple[Optional[str], Optional[str]]] = field(default_factory=dict)


# ------------------------------------------------------------------ matching output (07/08)

@dataclass(frozen=True)
class SectionRegs:
    """Regulations resolved onto a section. Base/zone regs are an MU overlay, not stored here."""
    section_id: str
    reg_set_index: int                      # single reg set per section (07)
    named_reg_ids: tuple[str, ...] = ()     # direct named/override matches (provenance)
    tributary_reg_ids: tuple[str, ...] = ()  # inherited via tributary_section_ids (provenance)
