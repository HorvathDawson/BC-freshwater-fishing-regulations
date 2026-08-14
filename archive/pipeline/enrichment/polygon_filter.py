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
    from pipeline.enrichment.polygon_filter import stream_polygon_mask

    polygon = box(0, 0, 10, 10)
    segments = [LineString([(8, 5), (12, 5)]),   # straddles border
                LineString([(11, 5), (13, 5)])]  # buffer zone only
    mask = stream_polygon_mask(polygon, segments, buffer_m=2.0)
    # → [True, True]  — stream enters, so the buffer-zone tail is included
"""

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Set, Tuple

from shapely.geometry.base import BaseGeometry
from shapely.prepared import PreparedGeometry, prep

logger = logging.getLogger(__name__)


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


# ---------------------------------------------------------------------------
# Vectorized multi-polygon matching (STRtree)
# ---------------------------------------------------------------------------


def match_features_to_polygons(
    polygons: List[BaseGeometry],
    buffer_m: float = 500.0,
    buffered: Optional[List[BaseGeometry]] = None,
    stream_fids: Optional[List[str]] = None,
    stream_geoms: Optional[List[BaseGeometry]] = None,
    fid_to_wsc: Optional[Dict[str, str]] = None,
    waterbody_keys: Optional[List[str]] = None,
    waterbody_geoms: Optional[List[BaseGeometry]] = None,
) -> List[Tuple[Set[str], Set[str]]]:
    """Match streams and waterbodies to polygons using STRtree + two-pass hysteresis.

    This is the single canonical implementation of the two-pass rule
    applied across one or more target polygons.  All spatial membership
    queries in the pipeline should funnel through here.

    Parameters
    ----------
    polygons :
        Exact (unbuffered) polygon geometries.  Must be in a projected
        CRS with metre units (EPSG:3005 for BC) so *buffer_m* is meaningful.
    buffer_m :
        Buffer distance in CRS units (metres for EPSG:3005).  Controls
        how much leniency is granted to streams straddling polygon edges.
        Ignored when *buffered* is provided.
    buffered :
        Optional pre-computed buffered polygons — same order as *polygons*.
        Pass this to avoid recomputing buffers when calling repeatedly
        with the same polygons.  When ``None``, computed automatically
        from *polygons* and *buffer_m*.
    stream_fids :
        Optional FID for every stream segment.
    stream_geoms :
        Optional geometry for every stream segment — same order as *stream_fids*.
    fid_to_wsc :
        Optional map of each fid to its FWA watershed code (WSC group key).
        Segments sharing a WSC are treated as one logical stream for
        the two-pass hysteresis gate.
    waterbody_keys :
        Optional list of waterbody keys.
    waterbody_geoms :
        Optional list of waterbody geometries — same order as *waterbody_keys*.

    Returns
    -------
    List of ``(matched_stream_fids, matched_waterbody_keys)`` tuples,
    one per polygon in the same order as *polygons*.
    """
    import numpy as np
    from shapely import STRtree

    n = len(polygons)
    if buffered is None:
        buffered = [p.buffer(buffer_m) for p in polygons]
    tree_exact = STRtree(polygons)
    tree_buffered = STRtree(buffered)

    result_fids: List[Set[str]] = [set() for _ in range(n)]
    result_wbks: List[Set[str]] = [set() for _ in range(n)]

    # ── Streams: two-pass hysteresis per WSC group ──
    if stream_fids and stream_geoms and fid_to_wsc:
        geom_arr = np.array(stream_geoms, dtype=object)

        # Pass 1 — exact: which (WSC, polygon_index) pairs genuinely enter?
        exact_s_idx, exact_p_idx = tree_exact.query(geom_arr, predicate="intersects")
        wsc_poly_enters: Set[Tuple[str, int]] = set()
        for si, pi in zip(exact_s_idx.tolist(), exact_p_idx.tolist()):
            wsc_poly_enters.add((fid_to_wsc[stream_fids[si]], pi))
        logger.debug(
            "  Pass 1 (exact): %d stream–poly pairs, %d WSC–poly entries",
            len(exact_s_idx),
            len(wsc_poly_enters),
        )

        # Pass 2 — buffered: include segments only for entered WSC–poly pairs
        buf_s_idx, buf_p_idx = tree_buffered.query(geom_arr, predicate="intersects")
        included = 0
        excluded = 0
        for si, pi in zip(buf_s_idx.tolist(), buf_p_idx.tolist()):
            fid = stream_fids[si]
            if (fid_to_wsc[fid], pi) in wsc_poly_enters:
                result_fids[pi].add(fid)
                included += 1
            else:
                excluded += 1
        logger.debug(
            "  Pass 2 (buffered): %d included, %d excluded by hysteresis",
            included,
            excluded,
        )

    # ── Polygon waterbodies: single vectorized buffered query ──
    if waterbody_keys and waterbody_geoms:
        poly_arr = np.array(waterbody_geoms, dtype=object)
        w_idx, p_idx = tree_buffered.query(poly_arr, predicate="intersects")
        for wi, pi in zip(w_idx.tolist(), p_idx.tolist()):
            result_wbks[pi].add(waterbody_keys[wi])

    return [(result_fids[i], result_wbks[i]) for i in range(n)]
