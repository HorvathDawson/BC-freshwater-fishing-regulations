"""Mint a watershed code for a TIDAL stream — one that drains straight to the ocean, so it has no FWA
parent to extend.

FWA roots such streams in the Coastal Rivers drainage: Level-1 is a coastal prefix (900 South Coast
Rivers for Port Moody + Squamish, both south of Cape Caution) and Level-2 (6 digits) is the percent
along the coastline where the river meets the ocean. That coastline-percent is infeasible to compute
here, so we ESTIMATE it from the nearest coastal FWA stream that already meets the ocean (start from its
Level-1+Level-2), and bump it, small and deterministic, until no other added tidal stream AND no FWA line holds it.
(T1246) The bump once looked only at other city codes, so a city creek took the FWA river's own code and the atlas
filed it as a part of that river (Turner Creek under Noons Creek); `validate.fwa_code_collisions` now refuses that.
"""

from __future__ import annotations

from typing import Optional

from shapely.geometry import Point

from pipeline.common.utils.wsc import trim_wsc
from pipeline.common.utils import wsc as _wsc

#: True if a WSC is in the Coastal Rivers drainage (a 9xx Level-1 prefix) — `wsc.is_coastal`, the one owner.
is_coastal_wsc = _wsc.is_coastal


def level12(wsc: str) -> tuple[str, str]:
    """(Level-1, Level-2) groups of a WSC, e.g. '900-359486-...' -> ('900','359486'). Missing -> ''. """
    parts = _wsc.groups(trim_wsc(wsc or ""))
    l1 = parts[0] if parts else ""
    l2 = parts[1] if len(parts) > 1 else ""
    return l1, l2


def fwa_level12_held(fwa_codes) -> set[str]:
    """Every coastal Level-1+Level-2 code ('900-057239') an FWA line holds, its own or as an ancestor of its code."""
    out: set[str] = set()
    for c in fwa_codes:
        if is_coastal_wsc(c or ""):
            l1, l2 = level12(c)
            if l2:
                out.add(f"{l1}-{l2}")
    return out


def estimate_coastal_wsc(mouth: Point, coastal: list[tuple[Point, str]], used: set[str], fwa_held: set[str]) -> str:
    """WSC for a tidal stream whose mouth is ``mouth`` (EPSG:3005). ``coastal`` = (mouth_point, wsc)
    of nearby coastal FWA streams. Start from the NEAREST one's Level-1+Level-2; bump Level-2 until the code is held by
    no other added tidal stream (``used``, updated in place) and by no FWA line (``fwa_held``, `fwa_level12_held`): an
    FWA river's own code would file the city creek as a part of that river (T1246)."""
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
    while code in used or code in fwa_held:                     # deterministic uniqueness bump
        n = (n + 1) % 1_000_000
        code = f"{l1}-{n:06d}"
    used.add(code)
    return code
