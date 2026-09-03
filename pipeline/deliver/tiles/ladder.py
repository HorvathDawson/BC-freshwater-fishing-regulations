"""Zoom ladders — which features exist at which zoom, and why.

This is the whole of "natural complexity reduction", and it is deliberately a pure
function of one attribute per geometry kind so it can be tested without a tile in
sight. Tippecanoe honours a per-feature ``tippecanoe.minzoom``, so the ladder is
applied by stamping every feature rather than by running a different build per zoom.

LINES use FWA **stream magnitude** (the count of headwater streams above a reach),
not Strahler order. Order jumps in big steps — 578 features at >=7, then 10,767 at
>=5, then 48,107 at >=4 — so a ladder built on it either shows almost nothing or
almost everything. Magnitude is nearly continuous and it drops small tributaries
while keeping whole big rivers, which is the picture people expect.

    magnitude >=   20000    1000     250     100      50      20      10       5       2       1
    features          101   1,122   4,569  11,947  24,049  59,906 114,251 217,568 597,596 2,053,299
    zoom                4       6       7       8       9      10      11      12      13      14

POLYGONS use area against the size of a pixel at that zoom, so a lake appears when
it is big enough to see rather than when a hand-picked hectare threshold says so.
"""

from __future__ import annotations

import math

# (minimum magnitude, zoom the feature first appears at). Descending.
MAGNITUDE_LADDER: tuple[tuple[int, int], ...] = (
    (20000, 4), (5000, 5), (1000, 6), (250, 7), (100, 8),
    (50, 9), (20, 10), (10, 11), (5, 12), (2, 13), (0, 14),
)

MIN_ZOOM = 4
MAX_ZOOM = 14

# Web-mercator ground resolution at the equator, metres per pixel at z0 (256 px tiles).
_EQUATOR_M_PER_PX_Z0 = 156543.03392804097
# BC spans ~48.3N to 60N; use the middle so the ladder is not tuned to one corner.
_BC_LAT_COS = math.cos(math.radians(54.0))
# A polygon is worth drawing when its side is about this many pixels.
_VISIBLE_PX = 3.0


def metres_per_pixel(zoom: int) -> float:
    """Ground resolution at ``zoom``, at BC's middle latitude."""
    return _EQUATOR_M_PER_PX_Z0 * _BC_LAT_COS / (2 ** zoom)


def zoom_for_magnitude(magnitude: int | None, floor: int = MIN_ZOOM) -> int:
    """First zoom a stream of this magnitude is drawn at.

    ``None`` and 0 both mean "no magnitude recorded", which is a headwater or an
    artifact; either way it belongs at the bottom of the ladder, never hidden
    entirely — the atlas is what makes the map look like the country.
    """
    m = magnitude or 0
    for threshold, zoom in MAGNITUDE_LADDER:
        if m >= threshold:
            return max(zoom, floor)
    return max(MAX_ZOOM, floor)


def zoom_for_area(area_m2: float | None, floor: int = MIN_ZOOM,
                  visible_px: float = _VISIBLE_PX) -> int:
    """First zoom a polygon of this area is drawn at.

    Derived, not chosen: a polygon appears once its side exceeds ``visible_px``
    pixels. Drawing a 2-hectare pond at z6 costs bytes to render a sub-pixel dot.
    """
    a = area_m2 or 0.0
    if a <= 0:
        return max(MAX_ZOOM, floor)
    side = math.sqrt(a)
    for z in range(MIN_ZOOM, MAX_ZOOM + 1):
        if side >= visible_px * metres_per_pixel(z):
            return max(z, floor)
    return max(MAX_ZOOM, floor)


def zoom_for_contour(depth_m: float | None, index: bool = False,
                     floor: int = 11) -> int:
    """Depth contours come in coarsest-first, so a lake is readable before it is detailed.

    A 10 m contour is worth showing several zooms before a 1 m one. ``index``
    marks the labelled contours, which lead.
    """
    d = abs(depth_m or 0)
    if index or d % 10 == 0:
        return max(floor, 11)
    if d % 5 == 0:
        return max(floor, 12)
    return max(floor, 13)
