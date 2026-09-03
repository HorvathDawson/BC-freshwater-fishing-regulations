"""BLK-chain building blocks — output of the `blk-chains` step (merged blue-line features)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional

from pipeline.common.models.names import NameTuple


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
