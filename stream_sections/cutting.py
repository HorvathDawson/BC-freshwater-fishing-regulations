"""Shared geometry/id helpers for the section pipeline.

- ``endpoint_id`` : reuse the graph_builder convention f"{round(x,3)}_{round(y,3)}".
- ``substring_cut`` : cut a merged BLK LineString at absolute route measures.
- ``section_id``   : stable id from the section's two bounds (see 02 / 09).

TODO: implement. Keep the ``section_id`` scheme fixed once chosen — it is an ABI.
"""

from __future__ import annotations

from typing import Any, Optional

_COORD_PRECISION = 3  # decimals; must match pipeline/graph/graph_builder.py get_endpoints


def endpoint_id(x: float, y: float) -> str:
    """Node id for a coordinate. Mirrors graph_builder.get_endpoints (3-dp rounding)."""
    raise NotImplementedError


def substring_cut(geometry: Any, mouth_measure: float, start_m: float, end_m: float) -> Any:
    """Return the sub-LineString between absolute route measures [start_m, end_m].

    Offsets are ``start_m - mouth_measure`` .. ``end_m - mouth_measure`` along ``geometry``.
    Falls back to the nearest fid boundary for 2-point-LineString geometries (03 trap).
    """
    raise NotImplementedError


def section_id(blk: str, lower_boundary_id: str, upper_boundary_id: str,
               lake_wbk: str = "", lower_route_measure: Optional[float] = None) -> str:
    """Stable section id. Hashes ONLY the two bounds so unrelated splits don't perturb it.

    Choose ONE scheme and keep it fixed (02): the readable f"{blk}:{int(lower_route_measure)}"
    or sha1(blk|lower|upper|lake_wbk)[:16].
    """
    raise NotImplementedError
