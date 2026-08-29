"""Section datatypes (03/04): the atomic matching/display/tile unit and its bounds."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from pipeline.models.enums import BoundaryKind
from pipeline.models.names import NameTuple


@dataclass(frozen=True)
class SectionBoundary:
    boundary_id: str  # "outlet" | "headwaters" | "lake:{wbk}" | "split:{split_id}"
    kind: BoundaryKind
    route_measure: Optional[float] = None
    label: str = ""
    # Other boundary_ids this one ALSO represents. A curated split whose measure lands inside a lake
    # run has no stream piece to cut — a dam or weir at a lake outlet projects a little way into the
    # lake — and used to be dropped silently, leaving every rule that bound it dangling. It is
    # recorded here instead, on the boundary that stands at the same place, so the split id still
    # resolves and the boundary can say everything it represents.
    aliases: tuple[str, ...] = ()


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
