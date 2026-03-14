"""
Spatial membership filter for FWA stream segments.

The core problem
----------------
A stream in FWA is many small linear segment features that share an
FWA watershed code.  When we want to know whether a stream is "inside"
a management polygon, we need to be careful:

* Too strict (exact boundary only): a stream that clearly runs through a
  polygon gets missed if the final segments happen to straddle the edge.
* Too loose (buffer only): distant streams that merely graze the buffer
  zone get falsely included.

The two-pass rule
-----------------
1. First check whether the stream *enters* the polygon at all — at least
   one segment must intersect the exact (unbuffered) polygon.
2. Only if the stream enters the polygon do we apply the buffer.  In that
   case every segment that intersects the *buffered* polygon is included —
   giving leniency to the rest of the stream that may be straddling the
   border.

A segment that only touches the buffer, when the stream never enters the
polygon, is always False.  This stops distant features from sneaking in.

Usage
-----
    from shapely.geometry import box, LineString
    from regulation_mapping_v2.enrichment.polygon_filter import stream_polygon_mask

    polygon = box(0, 0, 10, 10)
    segments = [LineString([(8, 5), (12, 5)]),   # straddles border
                LineString([(11, 5), (13, 5)])]  # buffer zone only
    mask = stream_polygon_mask(polygon, segments, buffer_m=2.0)
    # → [True, True]  — stream enters, so the buffer-zone tail is included
"""

from __future__ import annotations

from typing import List, Optional

from shapely.geometry.base import BaseGeometry
from shapely.prepared import PreparedGeometry, prep


def stream_polygon_mask(
    polygon: BaseGeometry,
    stream_geometries: List[BaseGeometry],
    buffer_m: float = 500.0,
    *,
    prepared_exact: Optional[PreparedGeometry] = None,
    prepared_buffered: Optional[PreparedGeometry] = None,
    buffered_polygon: Optional[BaseGeometry] = None,
) -> List[bool]:
    """Return a boolean mask: which stream segments are considered inside *polygon*.

    Parameters
    ----------
    polygon:
        The boundary polygon.  Must be in the same CRS as *stream_geometries*.
        For BC data this is typically EPSG:3005 (BC Albers, metres).
    stream_geometries:
        Stream segment geometries that share one FWA watershed code.
        Order is preserved — mask[i] corresponds to stream_geometries[i].
    buffer_m:
        Buffer distance in the same unit as the geometry CRS (metres for
        EPSG:3005).  Controls how much leniency is granted to segments
        straddling the polygon boundary.  Set to 0 for exact-only matching.

    Returns
    -------
    List[bool] of the same length as *stream_geometries*.
    True  → segment is considered inside the polygon (include in regulation).
    False → segment is outside (exclude).

    Algorithm
    ---------
    Pass 1 — check if any segment touches the exact polygon:
        touches_exact[i] = segment_i.intersects(polygon)
    If no segment touches the exact polygon → all False (stream never enters).

    Pass 2 — the stream does enter.  Apply buffer:
        result[i] = segment_i.intersects(polygon.buffer(buffer_m))
    Segments inside the polygon are True; segments in the buffer leniency
    zone are also True; segments beyond the buffer are False.
    """
    if not stream_geometries:
        return []

    from shapely.ops import unary_union

    # Reuse pre-computed buffered / prepared geometries when provided.
    buf = buffered_polygon if buffered_polygon is not None else polygon.buffer(buffer_m)
    p_exact = prepared_exact if prepared_exact is not None else prep(polygon)
    p_buf = prepared_buffered if prepared_buffered is not None else prep(buf)

    # Fast-fail: if the bounding box of the entire stream group doesn't
    # intersect the bounding box of the (buffered) polygon, skip everything.
    group_bounds = unary_union(stream_geometries).bounds  # (minx, miny, maxx, maxy)
    poly_bounds = buf.bounds
    if (
        group_bounds[2] < poly_bounds[0]  # group max_x < poly min_x
        or group_bounds[0] > poly_bounds[2]  # group min_x > poly max_x
        or group_bounds[3] < poly_bounds[1]  # group max_y < poly min_y
        or group_bounds[1] > poly_bounds[3]  # group min_y > poly max_y
    ):
        return [False] * len(stream_geometries)

    # Pass 1 — does any segment genuinely enter the exact polygon?
    touches_exact = [p_exact.intersects(seg) for seg in stream_geometries]
    stream_enters_polygon = any(touches_exact)

    if not stream_enters_polygon:
        # The stream never enters this polygon — reject everything,
        # even segments that brush the buffer zone.
        return [False] * len(stream_geometries)

    # Pass 2 — stream is known to enter.  Include anything within the buffer.
    return [p_buf.intersects(seg) for seg in stream_geometries]
