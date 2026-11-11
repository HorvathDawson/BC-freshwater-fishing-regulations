"""Place a vector depth sheet on the earth: slide and turn its shoreline onto our FWA lake outline (T634 item 3).

The 2001 files are "diagrammatic - no projection": sheet metres at true size, set down anywhere and at any heading.
So the fit is RIGID — a turn and a shift, never a stretch. The scale is not fitted; the area ratio is REPORTED, so a
sheet that is not at true size (or not this lake) shows as a bad ratio instead of being stretched to fit.

    shore(contours, soundings)  the sheet's own lake outline: its shoreline arcs (FCODE GA2711…), gaps at inlets
                          closed, polygonised; the faces holding a contour or a sounding are the water.
    fit(shore, lake)      the best turn + shift by area overlap (IoU), coarse 1° search on simplified shapes, then a
                          fine search on the full ones; returns the transform and the honest measures:
                          iou, area_ratio (sheet / lake), and the shoreline distances after placing (median, p90, m).
                          A mirrored fit is tried too and reported (`mirror_iou`): a mirror that fits better than
                          the true fit means the sheet is not what we think it is.
Nothing here decides a sheet is placed: it measures. A human ticks each one beside the PDF.
"""
from __future__ import annotations

import math
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

#: The sheets' shoreline code, found by drawing them (T634): GA27110000 is the lake shore (Marie Lake's polygonises to
#: 397,440 m2; the chart prints 397,000 sq.m); GA24850000 is a STREAM, not the shore. Variants (GA2711xxxx) kept.
SHORE_PREFIX = "GA2711"
#: The shore line breaks where a stream comes in. Dangling ends are joined to their nearest dangling end, closest
#: pairs first, up to this far apart (m); a sheet still open after that has no shore.
MAX_GAP_M = 300.0


def shore(contours: Iterable[Tuple[Optional[float], Optional[str], Sequence[Tuple[float, float]]]],
          soundings: Sequence[Tuple[float, float, Optional[float]]] = (), *, max_gap: float = MAX_GAP_M):
    """The sheet's water: the shoreline arcs, gaps at inlets closed, polygonised; the faces that hold a depth contour or
    a sounding are the water (an island is a face that holds none, and stays a hole). None when nothing closes."""
    from shapely.geometry import LineString, Point
    from shapely.ops import linemerge, polygonize, unary_union

    contours = list(contours)
    lines = [LineString(pts) for cv, fc, pts in contours
             if fc and fc.startswith(SHORE_PREFIX) and len(pts) >= 2]
    if not lines:
        return None
    u = unary_union(lines)
    merged = linemerge(u) if u.geom_type == "MultiLineString" else u
    parts = list(merged.geoms) if hasattr(merged, "geoms") else [merged]
    ends = [Point(l.coords[k]) for l in parts if not l.is_ring for k in (0, -1)]
    pairs = sorted((ends[i].distance(ends[j]), i, j) for i in range(len(ends)) for j in range(i + 1, len(ends)))
    used, joins = set(), []
    for dist, i, j in pairs:
        if dist > max_gap:
            break
        if i in used or j in used:
            continue
        used |= {i, j}
        joins.append(LineString([ends[i], ends[j]]))
    faces = list(polygonize(unary_union(parts + joins)))
    probes = [Point(x, y) for x, y, _ in soundings]
    probes += [LineString(pts).interpolate(0.5, normalized=True) for cv, _fc, pts in contours
               if cv not in (None, 0) and len(pts) >= 2]
    water = [f for f in faces if any(f.contains(q) for q in probes)] if probes else faces[:1]
    return unary_union(water) if water else None


def _place(g, angle_deg: float, dx: float, dy: float, mirror: bool, origin):
    from shapely import affinity
    if mirror:
        g = affinity.scale(g, xfact=-1, yfact=1, origin=origin)
    g = affinity.rotate(g, angle_deg, origin=origin)
    return affinity.translate(g, dx, dy)


def _iou(a, b) -> float:
    inter = a.intersection(b).area
    u = a.area + b.area - inter
    return inter / u if u > 0 else 0.0


def _search(src, dst, mirror: bool, angles: Iterable[float], shifts: Sequence[Tuple[float, float]], origin, base):
    best = (-1.0, 0.0, 0.0, 0.0)
    for a in angles:
        for sx, sy in shifts:
            g = _place(src, a, base[0] + sx, base[1] + sy, mirror, origin)
            s = _iou(g, dst)
            if s > best[0]:
                best = (s, a, base[0] + sx, base[1] + sy)
    return best


def fit(sheet_shore, lake, *, coarse_step: float = 2.0) -> Dict[str, object]:
    """Rigid fit of the sheet's shore onto the lake. All lengths in metres (the lake must be in a metric CRS)."""
    import numpy as np

    c0, c1 = sheet_shore.centroid, lake.centroid
    origin = (c0.x, c0.y)
    base = (c1.x - c0.x, c1.y - c0.y)
    size = math.sqrt(max(lake.area, 1.0))
    tol = size / 300.0
    s_src, s_dst = sheet_shore.simplify(tol), lake.simplify(tol)
    out: Dict[str, object] = {}
    results = {}
    for mirror in (False, True):
        # coarse: every `coarse_step` degrees, centroids together
        s, a, dx, dy = _search(s_src, s_dst, mirror, np.arange(0, 360, coarse_step), [(0.0, 0.0)], origin, base)
        # fine: ±coarse_step around it, and shifts up to 4 % of the lake's size
        step = size * 0.01
        shifts = [(i * step, j * step) for i in range(-4, 5) for j in range(-4, 5)]
        s, a, dx, dy = _search(s_src, s_dst, mirror, np.arange(a - coarse_step, a + coarse_step + 0.01, 0.5), shifts,
                               origin, (dx, dy))
        step /= 4
        shifts = [(i * step, j * step) for i in range(-3, 4) for j in range(-3, 4)]
        s, a, dx, dy = _search(sheet_shore, lake, mirror, np.arange(a - 0.5, a + 0.51, 0.25), shifts, origin, (dx, dy))
        results[mirror] = (s, a, dx, dy)
    s, a, dx, dy = results[False]
    placed = _place(sheet_shore, a, dx, dy, False, origin)
    out.update(iou=round(s, 4), angle_deg=round(a % 360, 2), dx=round(dx, 2), dy=round(dy, 2),
               origin=(round(origin[0], 3), round(origin[1], 3)),
               area_ratio=round(sheet_shore.area / lake.area, 4), mirror_iou=round(results[True][0], 4))
    d = shore_distances(placed, lake)
    out.update(median_m=round(d[0], 1), p90_m=round(d[1], 1))
    return out


def shore_distances(placed, lake, n: int = 400) -> Tuple[float, float]:
    """Median and 90th-percentile distance (m) from points along the placed sheet's outer shore to the lake's shore."""
    import numpy as np
    ring = max(_polys(placed), key=lambda p: p.area).exterior
    target = lake.boundary
    ds = sorted(target.distance(ring.interpolate(k / n, normalized=True)) for k in range(n))
    return float(np.median(ds)), float(ds[int(0.9 * (n - 1))])


def _polys(g) -> List:
    return list(g.geoms) if hasattr(g, "geoms") else [g]
