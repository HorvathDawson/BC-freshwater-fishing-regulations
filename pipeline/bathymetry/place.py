"""Place a vector depth sheet on the earth: slide and turn its shoreline onto our FWA lake outline (T634 item 3).

The 2001 files are "diagrammatic - no projection": sheet metres at true size, set down anywhere and at any heading.
So the fit is RIGID — a turn and a shift, never a stretch. The scale is not fitted; the area ratio is REPORTED, so a
sheet that is not at true size (or not this lake) shows as a bad ratio instead of being stretched to fit.

    shore(contours, soundings)  the sheet's own lake outline: its shoreline arcs (FCODE GA2711…), gaps at inlets
                          closed, polygonised; the faces holding a contour or a sounding are the water. A gap longer
                          than JOIN_TRUSTED_M is first completed with the sheet's other arcs that run between its ends
                          (a shore coded as a stream: Abruzzi); `shore_report` says how each gap was closed.
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


#: T1188 v5: a straight join longer than this (m) is not trusted to follow the water. Before it is drawn, the shore is
#: completed with the sheet's OTHER arcs that run between the gap's two ends (`COMPLETE_PREFIX`): Abruzzi's
#: north-west shore is coded GA2485 (stream), and the 830 m chord the fitter drew cut its shallows off. Chosen from the
#: data (T1188 v5, the 769 joins of the 498 placed sheets): the joins run continuously from 0 to 296 m (inlet gaps,
#: under the first pass's own MAX_GAP_M); above it lie only the 29 joins (301-975 m) of the 26 sheets that close only
#: at the 1,000 m pass. So the trusted join is the first pass's: MAX_GAP_M.
JOIN_TRUSTED_M = MAX_GAP_M
#: the arcs that may complete a shore: the FCODE hydrography family (G...: streams, double-line river banks, marsh and
#: other shoreline classes), never a depth line and never the shore code itself
COMPLETE_PREFIX = "G"
#: a completion path may be at most this many times the straight join it replaces (it follows the shore, not a stream
#: up a valley and back)
COMPLETE_MAX_FACTOR = 3.0
#: digitising slack (m): a depth line this close outside the shore still counts as inside it
INSIDE_TOL_M = 2.0
#: a completed ring is used only when at most this share of the depth-line length lies outside it; a sheet whose
#: shore leaves more than this outside is flagged `depth_lines_outside_shore` (T1188 v5: 0 % on 483 of 498 placed
#: sheets, at most 0.79 % on the rest but Bruce 1.1 %, Abruzzi 7.6 % and Boy 19.4 %, the three cut by a chord)
COMPLETE_INSIDE_MAX = 0.01

Contour = Tuple[Optional[float], Optional[str], Sequence[Tuple[float, float]]]


def _is_shore(fc: Optional[str]) -> bool:
    return bool(fc) and fc.startswith(SHORE_PREFIX)


def depth_lines(contours: Iterable[Contour]) -> List:
    """the sheet's depth lines (an arc with a non-zero CONTOUR that is not shore), as LineStrings"""
    from shapely.geometry import LineString
    return [LineString(p) for cv, fc, p in contours if cv not in (None, 0) and not _is_shore(fc) and len(p) >= 2]


def outside_share(lines: Sequence, poly, tol: float = 0.0) -> Optional[float]:
    """the share of the lines' length outside `poly` (grown by `tol` m); None when there are no lines"""
    import numpy as np
    import shapely
    arr = np.asarray(list(lines), dtype=object)
    total = float(shapely.length(arr).sum()) if len(arr) else 0.0
    if total <= 0 or poly is None:
        return None
    grown = poly.buffer(tol) if tol else poly
    shapely.prepare(grown)
    inside = shapely.contains(grown, arr)               # the lines wholly inside need no cut (fast, prepared)
    held = float(shapely.length(arr[inside]).sum())
    rest = arr[~inside]
    if len(rest):
        held += float(shapely.length(shapely.intersection(rest, grown)).sum())
    return max(0.0, 1.0 - held / total)


def close_lines(lines: Sequence, max_gap: float):
    """The lines merged, their dangling ends joined to the nearest dangling end, closest pairs first, up to `max_gap`
    (m). Returns (merged parts, [(distance, end a, end b)]) with the ends as (x, y)."""
    from shapely.geometry import Point
    from shapely.ops import linemerge, unary_union
    u = unary_union(list(lines))
    merged = linemerge(u) if u.geom_type == "MultiLineString" else u
    parts = list(merged.geoms) if hasattr(merged, "geoms") else [merged]
    ends = [tuple(l.coords[k])[:2] for l in parts if not l.is_ring for k in (0, -1)]
    pairs = sorted((Point(ends[i]).distance(Point(ends[j])), i, j) for i in range(len(ends))
                   for j in range(i + 1, len(ends)))
    used, joins = set(), []
    for dist, i, j in pairs:
        if dist > max_gap:
            break
        if i in used or j in used:
            continue
        used |= {i, j}
        joins.append((dist, ends[i], ends[j]))
    return parts, joins


def _node(xy) -> Tuple[float, float]:
    return (round(float(xy[0]), 2), round(float(xy[1]), 2))


def _completion(a, b, arcs: Sequence[Tuple[str, object]], shore_nodes, limit: float):
    """The shortest run of `arcs` ((code, LineString)) from node a to node b (shortest by length; ties by node, then arc),
    through no other shore node, at most `limit` m long; None when there is none."""
    import heapq
    adj: Dict[Tuple[float, float], List[Tuple[float, Tuple[float, float], int]]] = {}
    for k, (_fc, l) in enumerate(arcs):
        u, v = _node(l.coords[0]), _node(l.coords[-1])
        if u == v:
            continue
        adj.setdefault(u, []).append((l.length, v, k))
        adj.setdefault(v, []).append((l.length, u, k))
    a, b = _node(a), _node(b)
    best = {a: 0.0}
    prev: Dict[Tuple[float, float], Tuple[Tuple[float, float], int]] = {}
    heap = [(0.0, a)]
    while heap:
        d, n = heapq.heappop(heap)
        if n == b:
            break
        if d > best.get(n, math.inf) or (n != a and n in shore_nodes):
            continue
        for w, m, k in sorted(adj.get(n, ())):
            nd = d + w
            if nd <= limit and nd < best.get(m, math.inf) - 1e-9:
                best[m], prev[m] = nd, (n, k)
                heapq.heappush(heap, (nd, m))
    if b not in prev:
        return None
    ks, n = [], b
    while n != a:
        n, k = prev[n]
        ks.append(k)
    return sorted(ks), best[b]


def _water(parts, joins, probes):
    from shapely.ops import polygonize, unary_union
    faces = list(polygonize(unary_union(list(parts) + list(joins))))
    water = [f for f in faces if any(f.contains(q) for q in probes)] if probes else faces[:1]
    w = unary_union(water) if water else None
    return w if w is not None and w.area > 0 else None


def shore_report(contours: Iterable[Contour], soundings: Sequence[Tuple[float, float, Optional[float]]] = (), *,
                 max_gap: float = MAX_GAP_M) -> Tuple[Optional[object], Dict[str, object]]:
    """The sheet's water (`shore`) and how its shore was closed: every join (`m`, `how`: "straight" or "arcs", with the
    arcs' codes and length), the longest straight join, and `depth_lines_outside_shore_pct` (the share of the depth
    lines' length more than INSIDE_TOL_M outside the water, in percent). A join longer than JOIN_TRUSTED_M is first
    completed with the sheet's other arcs between its two ends (`_completion`); the completion is kept only when the
    ring then closes and holds the depth lines (COMPLETE_INSIDE_MAX); otherwise the straight join stays and says so."""
    from shapely.geometry import LineString, Point
    contours = list(contours)
    lines = [LineString(p) for cv, fc, p in contours if _is_shore(fc) and len(p) >= 2]
    if not lines:
        return None, {"joins": []}
    parts, joins = close_lines(lines, max_gap)
    probes = [Point(x, y) for x, y, _ in soundings]
    probes += [LineString(p).interpolate(0.5, normalized=True) for cv, _fc, p in contours
               if cv not in (None, 0) and len(p) >= 2]
    deep = depth_lines(contours)
    others = [(fc, LineString(p)) for cv, fc, p in contours
              if cv in (None, 0) and fc and fc.startswith(COMPLETE_PREFIX) and not _is_shore(fc) and len(p) >= 2]
    shore_nodes = {_node(c) for l in lines for c in (l.coords[0], l.coords[-1])}
    used: Dict[int, Tuple[List[int], float]] = {}          # join index -> (the arcs completing it, their length)
    straight = [LineString([p, q]) for _d, p, q in joins]

    def build():
        segs = [l for i, l in enumerate(straight) if i not in used]
        segs += [others[k][1] for i in sorted(used) for k in used[i][0]]
        return _water(parts, segs, probes)

    water = build()
    for i in sorted(range(len(joins)), key=lambda i: (-joins[i][0], i)):
        d, p, q = joins[i]
        if d <= JOIN_TRUSTED_M or not others or not deep:      # no depth lines: nothing can check a completion
            continue
        path = _completion(p, q, others, shore_nodes - {_node(p), _node(q)}, COMPLETE_MAX_FACTOR * d)
        if path is None:
            continue
        used[i] = path
        trial = build()
        out = outside_share(deep, trial, INSIDE_TOL_M)
        if trial is None or out is None or out > COMPLETE_INSIDE_MAX:
            del used[i]                      # it does not close, or the depth lines fall outside: the chord stays
            continue
        water = trial
    rec = []
    for i, (d, _p, _q) in enumerate(joins):
        e: Dict[str, object] = {"m": round(d, 1), "how": "arcs" if i in used else "straight"}
        if i in used:
            e.update(arcs_m=round(used[i][1], 1), codes=sorted({others[k][0] for k in used[i][0]}))
        rec.append(e)
    rec.sort(key=lambda e: (-e["m"], e["how"]))
    info: Dict[str, object] = {"joins": rec,
                               "longest_straight_join_m": max((e["m"] for e in rec if e["how"] == "straight"),
                                                              default=0.0)}
    out = outside_share(deep, water, INSIDE_TOL_M) if water is not None else None
    if out is not None:
        info["depth_lines_outside_shore_pct"] = round(100 * out, 2)
    info["flags"] = shore_flags(info)
    return water, info


def shore_flags(info: Dict[str, object]) -> List[str]:
    """`shore_joined_straight` (a straight join longer than JOIN_TRUSTED_M closes the shore) and
    `depth_lines_outside_shore` (more than COMPLETE_INSIDE_MAX of the depth lines lie outside it)"""
    out = []
    if float(info.get("longest_straight_join_m") or 0) > JOIN_TRUSTED_M:
        out.append("shore_joined_straight")
    if float(info.get("depth_lines_outside_shore_pct") or 0) > 100 * COMPLETE_INSIDE_MAX:
        out.append("depth_lines_outside_shore")
    return out


def shore(contours: Iterable[Contour], soundings: Sequence[Tuple[float, float, Optional[float]]] = (), *,
          max_gap: float = MAX_GAP_M):
    """The sheet's water: the shoreline arcs, gaps at inlets closed (a long gap completed with the sheet's own arcs
    where they run between its ends, `shore_report`), polygonised; the faces that hold a depth contour or a sounding
    are the water (an island is a face that holds none, and stays a hole). None when nothing closes."""
    return shore_report(contours, soundings, max_gap=max_gap)[0]


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
