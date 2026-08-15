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
    override = "override"                 # manual name_variants.json / feature_display_names.json
    gazette = "gazette"                   # direct GNIS (stream GNIS_NAME; lake GNIS_NAME_1/2/3)
    side_channel = "side-channel"         # inherited from same-WSC main-channel BLK
    gauge = "gauge"                       # hydrometric gauge (WSC) name
    regulation = "regulation"             # name a regulation entry / synopsis uses (by id)
    stocking = "stocking"                 # stocking DB common name (by wbk)
    bathymetry = "bathymetry"             # bathymetry map name (by wbk)
    marker = "marker"                     # bathymetry map marker / point-of-interest name (by wbk)
    alias = "alias"                       # alternate name — SEARCHABLE but never beats gazette
    synopsis = "synopsis"                 # a verbatim name used by a regulation entry


class NodeKind(str, Enum):
    stream = "stream"           # a BLK-merged stream (a section after splitting)
    lake = "lake"               # a lake/manmade waterbody node


class BoundaryKind(str, Enum):
    outlet = "outlet"           # natural mouth end (no named boundary)
    headwaters = "headwaters"   # natural source end
    lake = "lake"               # abuts a lake node
    split = "split"             # a point/line curated cut
    confluence = "confluence"   # a curated cut at a tributary's mouth
    mu = "mu"                   # a curated cut on an MU/zone boundary line
    border = "border"           # the BC provincial boundary (auto split; beyond it = out_of_bc)
    area = "area"               # an admin/park polygon boundary (curated closure; inside = in_areas)


class AnchorType(str, Enum):
    """How a cut GEOMETRY is defined. Every cut is a LINE or a polygon BOUNDARY."""
    point = "point"             # a coord -> auto perpendicular cut line at the target mainstem
    line = "line"              # an explicit cut line (list of coords)
    lake = "lake"              # a lake polygon boundary (wbk)
    mu_boundary = "mu_boundary"  # the shared boundary line between two MUs
    confluence = "confluence"    # a cut line at where a tributary BLK meets the mainstem
    border = "border"           # the BC provincial outline (auto, not hand-authored)
    area_boundary = "area_boundary"  # an admin/park polygon boundary; cut a named water + its WSC
                                # descendants where they cross it, then flag INSIDE pieces (in_areas).


# --------------------------------------------------------------------------- names

@dataclass(frozen=True)
class NameTuple:
    """One provenance-tagged name. Display = first by NameSource priority; search = all.
    ``note`` carries free-text provenance/context (why the variant was added, from the source).
    ``gnis_id`` = the GNIS this name belongs to when known (e.g. a side-channel inherits the main
    channel's name AND its gnis) — the node's own ``gnis_id`` field stays empty so the channels stay
    distinguishable, but consumers (the registry) can still group the whole river by this gnis."""
    name: str
    source: NameSource
    note: str = ""
    gnis_id: str = ""


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
    edge_types: tuple[str, ...] = ()         # distinct FWA EDGE_TYPEs over fids; "2300" => barrier


# --------------------------------------------------------- the stream graph (INVERTED)
# ONE graph. A NODE is a stream (a BLK-merged chain, later split into sections) or a lake.
# An EDGE is "flows into": the from_node drains into the to_node at a confluence measure.
# A mainstem node has many incoming edges (its tributaries) and one outgoing edge (its own
# confluence). fids never appear in the graph — only inside StreamNode as provenance.
# Tributaries of a node = its ANCESTORS (walk incoming edges upstream).

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


# ------------------------------------------------------------------------ splits (04)

@dataclass(frozen=True)
class SplitAnchor:
    """Defines the CUT GEOMETRY (always a line or a polygon boundary). Only the fields
    relevant to ``type`` are populated.

    - point       : a coord -> an auto perpendicular cut line across the target mainstem
                    (length bounded by SplitDef.proximity_m, so it also catches nearby side
                    channels but nothing far away).
    - line        : an explicit cut line (>=2 coords).
    - lake        : the lake polygon boundary (wbk).
    - mu_boundary : the shared boundary line between mu_a and mu_b (needs BOTH).
    - confluence  : an auto cut line where tributary_blk meets the target mainstem.

    A ``point``/``confluence`` anchor may carry an ALONG-CHANNEL offset: the cut lands
    ``offset_m`` metres ``offset_dir`` ("upstream"|"downstream") from the projected coord/mouth,
    following the channel (not straight-line). Lets a reg like "100 m downstream of the falls" be
    authored from the falls point alone; clamped to the channel ends (with a concern if clamped).
    """
    type: AnchorType
    coord: Optional[tuple[float, float]] = None       # point
    coords: tuple[tuple[float, float], ...] = ()      # line (>=2 vertices)
    is_lonlat: bool = False
    offset_m: float = 0.0         # point/confluence: shift the cut this many metres ALONG the
    offset_dir: str = ""          # channel, in offset_dir ("upstream"|"downstream"). 0 => no shift.
    wbk: str = ""                 # lake
    mu_a: str = ""                # mu_boundary (one side)
    mu_b: str = ""                # mu_boundary (other side)
    tributary_blk: str = ""       # confluence: the tributary's BLK (an id, not a name)
    tributary_wsc: str = ""       # confluence: OR the tributary's WSC (trimmed) — its mouth
    area_layer: str = ""          # area_boundary: the polygon layer (e.g. "parks_bc")
    area_name_field: str = ""     # area_boundary: the layer field to match on (e.g. PROTECTED_LANDS_NAME)
    area_name: str = ""           # area_boundary: the value to match (e.g. "GARIBALDI PARK")
    wsc_descendants: bool = False # area_boundary: target the whole WSC subtree (prefix) not exact WSC

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SplitAnchor":
        t = AnchorType(d["type"])
        coord = d.get("coord")
        coords = tuple((float(c[0]), float(c[1])) for c in d.get("coords", []))
        if t == AnchorType.mu_boundary and not (d.get("mu_a") and d.get("mu_b")):
            raise ValueError("mu_boundary anchor needs both mu_a and mu_b")
        if t == AnchorType.line and len(coords) < 2:
            raise ValueError("line anchor needs >=2 coords")
        if t == AnchorType.confluence and not (d.get("tributary_blk") or d.get("tributary_wsc")):
            raise ValueError("confluence anchor needs tributary_blk or tributary_wsc")
        if t == AnchorType.lake and not d.get("wbk"):
            raise ValueError("lake anchor needs wbk")
        if t == AnchorType.area_boundary and not (d.get("area_layer") and d.get("area_name")):
            raise ValueError("area_boundary anchor needs area_layer and area_name")
        offset_m = float(d.get("offset_m", 0.0))
        offset_dir = str(d.get("offset_dir", ""))
        if offset_m < 0:
            raise ValueError("offset_m must be >= 0 (give the direction via offset_dir)")
        if offset_m:
            if t not in (AnchorType.point, AnchorType.confluence):
                raise ValueError(f"offset_m only valid on point/confluence anchors, not {t.value}")
            if offset_dir not in ("upstream", "downstream"):
                raise ValueError("offset_m requires offset_dir 'upstream' or 'downstream'")
        return cls(
            type=t,
            coord=(float(coord[0]), float(coord[1])) if coord else None,
            coords=coords, is_lonlat=bool(d.get("is_lonlat", False)),
            offset_m=offset_m, offset_dir=offset_dir,
            wbk=str(d.get("wbk", "")), mu_a=str(d.get("mu_a", "")), mu_b=str(d.get("mu_b", "")),
            tributary_blk=str(d.get("tributary_blk", "")),
            tributary_wsc=str(d.get("tributary_wsc", "")),
            area_layer=str(d.get("area_layer", "")),
            area_name_field=str(d.get("area_name_field", "")),
            area_name=str(d.get("area_name", "")),
            wsc_descendants=bool(d.get("wsc_descendants", False)),
        )


@dataclass(frozen=True)
class SplitDef:
    """One hand-authored split. ``id`` is a STABLE key (part of the section_id ABI).

    A split's cut geometry (anchor) intersects streams; each crossed channel within
    ``proximity_m`` and matching the optional target scope is cut where it crosses.

    Target scope (optional, at most one) narrows which channels are eligible:
      - ``blk``     : only this blue line.
      - ``wsc``     : this river + its side channels (share the WSC) — proximity-limited.
      - ``gnis_id`` : resolve to the named stream's blk(s).
    A ``point``/``confluence`` anchor REQUIRES a target (it needs a mainstem to cut across).
    """
    id: str
    anchor: SplitAnchor
    blk: str = ""
    wsc: str = ""
    gnis_id: str = ""
    stream_name: str = ""
    label: str = ""
    proximity_m: float = 500.0    # max distance a channel may be from the cut geometry
    concern: str = ""             # optional free-text caveat (inferred name, multi-crossing collapse,
                                  # unresolved) — surfaced in splits.resolved.json + the gpkg, never silent

    @classmethod
    def from_dict(cls, d: Mapping[str, Any]) -> "SplitDef":
        targets = [k for k in ("blk", "wsc", "gnis_id") if d.get(k)]
        if len(targets) > 1:
            raise ValueError(f"split {d.get('id')!r}: at most one of blk/wsc/gnis_id, got {targets}")
        anchor = SplitAnchor.from_dict(d["anchor"])
        if anchor.type in (AnchorType.point, AnchorType.confluence, AnchorType.area_boundary) and not targets:
            raise ValueError(f"split {d.get('id')!r}: {anchor.type.value} anchor requires a target (blk/wsc/gnis_id)")
        return cls(
            id=str(d["id"]), anchor=anchor,
            blk=str(d.get("blk", "")), wsc=str(d.get("wsc", "")),
            gnis_id=str(d.get("gnis_id", "")), stream_name=str(d.get("stream_name", "")),
            label=str(d.get("label", "")), proximity_m=float(d.get("proximity_m", 500.0)),
            concern=str(d.get("_concern", d.get("concern", ""))),
        )


@dataclass(frozen=True)
class SplitPoint:
    """A resolved cut on one blue line. Written to splits.resolved.json for reviewable builds."""
    split_id: str
    blk: str                # the specific blue line this cut lands on (one per crossed channel)
    route_measure: float    # absolute DOWNSTREAM_ROUTE_MEASURE cut position
    fid: str                # containing fid (stored back)
    label: str
    anchor_type: AnchorType
    offset_m: float = 0.0   # distance from the cut geometry to the channel crossing (review aid)
    proximity_m: float = 0.0  # authored pickup radius (carried from SplitDef) — reuse an existing
                            # boundary within this many route-metres instead of cutting a duplicate
    concern: str = ""       # carried from SplitDef.concern (+ resolver-added caveats)
    picked_up: bool = False # True => this curated split reused an existing (lake/border) boundary
                            # within proximity instead of cutting a new one (docs/04 proximity pickup)


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


# ------------------------------------------------------------------ matching output (07/08)

@dataclass(frozen=True)
class SectionRegs:
    """Regulations resolved onto a section. Base/zone regs are an MU overlay, not stored here."""
    section_id: str
    reg_set_index: int                      # single reg set per section (07)
    named_reg_ids: tuple[str, ...] = ()     # direct named/override matches (provenance)
    tributary_reg_ids: tuple[str, ...] = ()  # inherited via tributary_section_ids (provenance)


# ------------------------------------------------------------------ registry (parser truth)

@dataclass(frozen=True)
class RegistryBoundary:
    """One bindable cut-point on a registry item — what a rule's Extent selects against. Derived
    from the graph nodes' section bounds (curated splits + auto lake/outlet/headwaters)."""
    id: str                      # READABLE token a rule's Extent.splits references (split id | "tenas_lake")
    label: str                   # human-readable ("Goat Creek → Atnarko River", "Tenas Lake")
    kind: str                    # BoundaryKind value: split | confluence | lake | outlet | headwaters | mu | border
    ref: str = ""                # the graph boundary_id (stable): "split:{id}" | "lake:{wbk}" | "outlet" | "headwaters"
    wbk: str = ""                # robust key for lake boundaries


@dataclass(frozen=True)
class RegistryItem:
    """A regulated waterbody the matcher points an Entry at. Identity + names come straight from the
    graph node grouping (which already merged blk-chains + applied name_variants); `boundaries` is the
    parser's catalog of bindable cut-points on this item."""
    id: str                              # gnis:{} -> wsc:{} -> blk:{} (streams); wbk:{} (lakes); area:{} (areas)
    name: str                            # display_name from the graph
    kind: str                            # NodeKind value: stream | lake  (+ 'area' later)
    variants: tuple[str, ...] = ()       # searchable name variants (name_tuples)
    mus: tuple[str, ...] = ()            # management units this item spans (07 overlay; may be empty pre-overlay)
    section_ids: tuple[str, ...] = ()    # its section node_ids
    boundaries: tuple[RegistryBoundary, ...] = ()
