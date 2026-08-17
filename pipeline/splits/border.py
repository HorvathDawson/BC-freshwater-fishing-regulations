"""Provincial-boundary handling: split cross-border BLKs at the BC outline + flag out-of-BC pieces.

A handful of BC fishing-reg streams (Kootenay, Columbia, …) physically leave BC and re-enter —
FWA carries the full geometry, so a single BLK can dip into the US and come back (verified on the
Kootenay: blk 356570348 crosses the 49th parallel at m≈168733 and m≈431761). We treat the
out-of-BC portions like under-lake reaches BUT DIFFERENT: the geometry is **kept** (so the client
can draw it dotted) and the piece is **not** a barrier — BC regs simply don't apply there.

Mechanically this is "lakes but via splits, after the merge": generate a `border` SplitPoint at
every crossing of a BLK with the BC outline, run them through the normal `sectionizer`, then flag
each resulting piece whose representative point falls outside the outline. A curated point near a
border split (e.g. Kootenay "Idaho border") later reuses it via the sectionizer's proximity pickup.

The BC outline here is the union of the WMU polygons (they tile the province) — good enough as an
in-data boundary. A dedicated provincial-boundary layer (a future fetch) would be crisper; swap it
into ``bc_outline`` without touching the split/flag logic.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Optional

from shapely.geometry import Point
from shapely.ops import unary_union

from pipeline.splits.anchors import _points
from pipeline.models import AnchorType, BlkChain, NodeKind, SplitPoint, StreamGraph


def bc_outline(fwa):
    """The BC land outline as a shapely (Multi)Polygon, or None if unavailable.

    FAST PATH: the province never changes, so the outline is precomputed once and cached at
    ``data/bc_boundary.geojson`` (see ``pipeline.splits.bc_boundary``). A cheap file read replaces
    the ~225-WMU ``union_all`` that used to dominate the border stage.

    FALLBACK (cache missing): union the WMU polygons on the fly via ``fast_wmu_union`` — this
    simplifies each poly + micro-buffers BEFORE the union, so it's also fast (~seconds) and could run
    every build; the cache is just an even-cheaper file read. Loaded whole (≈225 rows) on purpose: a
    bbox-clipped union would expose the loaded set's cut edge as a fake "border" (``fast_wmu_union``
    simplifies internally)."""
    from pipeline.splits.bc_boundary import fast_wmu_union, load_cached_boundary

    cached = load_cached_boundary(fwa.gpkg_path)
    if cached is not None:
        return cached
    outline, _ = fast_wmu_union(fwa)
    return outline


def border_split_points(chains: list[BlkChain], outline, prof=None) -> list[SplitPoint]:
    """One `border` SplitPoint per crossing of each BLK with the outline boundary.

    Province-scale fast path: only a chain that is NOT fully inside BC can cross the boundary, so a
    single **vectorized, prepared** ``covered_by`` (shapely 2.x auto-prepares the scalar outline)
    prunes the hundreds of thousands of fully-inland chains in one C-level call. Only the handful of
    near-border candidates then pay the expensive per-geometry boundary intersection — turning a
    province-wide O(N·boundary) sweep (the old ~27 min bottleneck) into O(candidates)."""
    if outline is None:
        return []
    import numpy as np
    import shapely
    from pipeline.utils.profiling import Profiler
    prof = prof or Profiler()

    with prof.phase("  gather chain geometries"):
        valid = [c for c in chains
                 if getattr(c, "geometry", None) is not None and not c.geometry.is_empty]
    if not valid:
        return []
    with prof.phase("  covered_by prefilter (vectorized)"):
        arr = np.fromiter((c.geometry for c in valid), dtype=object, count=len(valid))
        covered = shapely.covered_by(arr, outline)  # True = fully inside BC -> cannot cross the border

    import time
    boundary = outline.boundary
    out: list[SplitPoint] = []
    _t = time.perf_counter()
    for c, cov in zip(valid, covered):
        if cov:
            continue                               # fully inland: skip the intersection entirely
        g = c.geometry
        crossings = _points(g.intersection(boundary))
        for i, p in enumerate(sorted(crossings, key=lambda p: g.project(p))):
            out.append(SplitPoint(
                split_id=f"border:{c.blk}:{i}", blk=c.blk,
                route_measure=c.mouth_measure + g.project(p), fid="",
                label="BC boundary", anchor_type=AnchorType.border))
    prof.add("  crossing intersection loop", time.perf_counter() - _t)
    return out


def _midpoint(g) -> Optional[Point]:
    """A representative interior point for any piece geometry (LineString or MultiLineString)."""
    if g is None or g.is_empty:
        return None
    if g.geom_type == "LineString":
        return g.interpolate(0.5, normalized=True)
    return g.representative_point()             # MultiLineString / other: guaranteed on the geom


def _pieces_in_polygon(graph: StreamGraph, geoms: dict, poly, blks=None, inside: bool = True):
    """Node ids of the stream pieces whose representative midpoint is INSIDE (``inside=True``) or
    OUTSIDE (``inside=False``) ``poly``. Shared core of the border/out-of-BC flag and the area
    closure flag (same midpoint + point-in-polygon test, opposite polarity). ``blks`` (optional)
    restricts the scan — only those blks can have a piece on the relevant side — avoiding a
    province-wide sweep."""
    if poly is None:
        return
    for nid, node in list(graph.nodes.items()):
        if node.kind != NodeKind.stream or (blks is not None and node.blk not in blks):
            continue
        mp = _midpoint(geoms.get(nid))
        if mp is not None and poly.contains(mp) == inside:
            yield nid


def mark_out_of_bc(graph: StreamGraph, geoms: dict, outline, blks=None) -> int:
    """Set ``out_of_bc=True`` on every stream piece (midpoint) outside the BC outline. Returns the
    count flagged. ``blks`` (optional) restricts the scan to the blks that actually crossed the
    border — only those can have an out-of-BC piece — which avoids a province-wide point-in-polygon
    sweep on a big build."""
    n = 0
    for nid in _pieces_in_polygon(graph, geoms, outline, blks, inside=False):
        graph.nodes[nid] = replace(graph.nodes[nid], out_of_bc=True)
        n += 1
    return n


def mark_inside_area(graph: StreamGraph, geoms: dict, poly, area_label: str, blks=None) -> int:
    """Append ``area_label`` to ``in_areas`` on every stream piece (midpoint) INSIDE ``poly`` —
    the inverse of ``mark_out_of_bc``, sharing ``_pieces_in_polygon``. Purely geometric (rule-
    agnostic): records which admin/park polygon a piece falls inside; Phase-5 matching maps that
    area to its closure reg. Idempotent (won't double-add). ``blks`` scopes the flag to the named
    water's WSC-descendant blks so only THAT system's in-park pieces are marked, not every stream
    inside the polygon. Returns the count flagged."""
    n = 0
    for nid in _pieces_in_polygon(graph, geoms, poly, blks, inside=True):
        node = graph.nodes[nid]
        if area_label not in node.in_areas:
            graph.nodes[nid] = replace(node, in_areas=node.in_areas + (area_label,))
            n += 1
    return n


def mark_inside_areas(graph: StreamGraph, geoms: dict, polys_by_name: dict) -> int:
    """Batch membership: flag ``in_areas`` for every stream piece whose midpoint falls inside each
    polygon, using ONE STRtree over all node midpoints. For each polygon the tree bbox-prefilters to
    the few midpoints near it, then a precise ``contains`` confirms — turning the old
    O(polygons·nodes) province-wide sweep into O(polygons·log nodes + hits). Same midpoint-containment
    semantics as ``mark_inside_area``, just vectorized. Returns the number of flags added."""
    from shapely.strtree import STRtree

    nids: list = []
    mids: list = []
    for nid, node in graph.nodes.items():
        if node.kind != NodeKind.stream:
            continue
        mp = _midpoint(geoms.get(nid))
        if mp is not None:
            nids.append(nid)
            mids.append(mp)
    if not mids or not polys_by_name:
        return 0
    tree = STRtree(mids)
    n = 0
    for name, poly in polys_by_name.items():
        if poly is None or poly.is_empty:
            continue
        for i in tree.query(poly, predicate="contains"):   # midpoints poly.contains() — bbox-prefiltered
            nid = nids[i]
            node = graph.nodes[nid]
            if name not in node.in_areas:
                graph.nodes[nid] = replace(node, in_areas=node.in_areas + (name,))
                n += 1
    return n


def apply_border(fwa, graph: StreamGraph, geoms: dict, chains: list[BlkChain],
                 fid_index: Optional[dict] = None, prof=None) -> tuple[int, int]:
    """Full border pass: outline -> split cross-border BLKs -> flag out-of-BC pieces.
    Returns (n_border_splits, n_pieces_flagged). Call BEFORE curated splits so their points can
    pick up the border boundaries. Meaningful on province-scale builds; on a small inland bbox
    no BLK reaches the border so it is a no-op.

    ``prof`` (optional Profiler) attributes the ~28-min stage to outline load / split-point search /
    graph cut / out-of-BC flag; pass one or set env ``PIPELINE_PROFILE=1``."""
    from pipeline.splits.sectionizer import split_graph_at
    from pipeline.utils.profiling import Profiler
    prof = prof or Profiler()

    with prof.phase("bc_outline (load/union)"):
        outline = bc_outline(fwa)
    if outline is None:
        return 0, 0
    with prof.phase("border_split_points"):
        pts = border_split_points(chains, outline, prof=prof)
    with prof.phase("split_graph_at"):
        if pts:
            split_graph_at(graph, geoms, pts, fid_index)
    with prof.phase("mark_out_of_bc"):
        flagged = mark_out_of_bc(graph, geoms, outline, blks={p.blk for p in pts})
    prof.report("border")
    return len(pts), flagged
