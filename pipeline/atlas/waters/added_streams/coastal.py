"""Mint a watershed code for a TIDAL stream — one that drains straight to the ocean, so it has no FWA
parent to extend.

FWA roots such streams in the Coastal Rivers drainage: Level-1 is a coastal prefix (900 South Coast
Rivers for Port Moody + Squamish, both south of Cape Caution) and Level-2 (6 digits) is the percent
along the coastline where the river meets the ocean. That coastline-percent is infeasible to compute
here, so we ESTIMATE it from the nearest coastal FWA stream that already meets the ocean (inherit its
Level-1+Level-2), and keep colliding added tidal streams unique with a small deterministic bump.
"""

from __future__ import annotations

from typing import Optional

from shapely.geometry import Point

from pipeline.common.utils.wsc import trim_wsc

_COASTAL_PREFIXES = tuple(str(p) for p in range(900, 1000, 5))   # 900..995 coastal drainages


def is_coastal_wsc(wsc: str) -> bool:
    """True if a WSC is in the Coastal Rivers drainage (a 9xx Level-1 prefix)."""
    return trim_wsc(wsc or "")[:3] in _COASTAL_PREFIXES


def level12(wsc: str) -> tuple[str, str]:
    """(Level-1, Level-2) groups of a WSC, e.g. '900-359486-...' -> ('900','359486'). Missing -> ''. """
    parts = trim_wsc(wsc or "").split("-")
    l1 = parts[0] if parts else ""
    l2 = parts[1] if len(parts) > 1 else ""
    return l1, l2


def estimate_coastal_wsc(mouth: Point, coastal: list[tuple[Point, str]], used: set[str]) -> str:
    """WSC for a tidal stream whose mouth is ``mouth`` (EPSG:3005). ``coastal`` = (mouth_point, wsc)
    of nearby coastal FWA streams. Inherit the NEAREST one's Level-1+Level-2; bump Level-2 until the
    code is unused so multiple added tidal streams stay distinct. ``used`` is updated in place."""
    l1, l2 = "900", "000000"                                    # South Coast fallback if no neighbour
    if coastal:
        _, near_wsc = min(coastal, key=lambda pw: pw[0].distance(mouth))
        nl1, nl2 = level12(near_wsc)
        if nl1:
            l1 = nl1
        if nl2:
            l2 = nl2
    n = int(l2)
    code = f"{l1}-{n:06d}"
    while code in used:                                         # deterministic uniqueness bump
        n = (n + 1) % 1_000_000
        code = f"{l1}-{n:06d}"
    used.add(code)
    return code
