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
    """Union of ALL WMU polygons = an in-data BC land outline. Loaded whole (≈225 rows) on
    purpose: a bbox-clipped union would expose the loaded set's cut edge as a fake "border",
    so the outline must always be the true province-wide boundary. Returns a shapely
    (Multi)Polygon, or None if no WMUs. (A dedicated provincial-boundary layer is a cleaner
    future swap — replace this body without touching the split/flag logic.)"""
    if "wmu" not in getattr(fwa, "layer_names", []):
        return None
    gdf = fwa.get_layer("wmu", columns=["WILDLIFE_MGMT_UNIT_ID"])
    polys = [g for g in gdf.geometry if g is not None and not g.is_empty]
    return unary_union(polys) if polys else None


def border_split_points(chains: list[BlkChain], outline) -> list[SplitPoint]:
    """One `border` SplitPoint per crossing of each BLK with the outline boundary."""
    if outline is None:
        return []
    boundary = outline.boundary
    out: list[SplitPoint] = []
    for c in chains:
        g = getattr(c, "geometry", None)
        if g is None or g.is_empty:
            continue
        crossings = _points(g.intersection(boundary))
        for i, p in enumerate(sorted(crossings, key=lambda p: g.project(p))):
            out.append(SplitPoint(
                split_id=f"border:{c.blk}:{i}", blk=c.blk,
                route_measure=c.mouth_measure + g.project(p), fid="",
                label="BC boundary", anchor_type=AnchorType.border))
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


def apply_border(fwa, graph: StreamGraph, geoms: dict, chains: list[BlkChain],
                 fid_index: Optional[dict] = None) -> tuple[int, int]:
    """Full border pass: outline -> split cross-border BLKs -> flag out-of-BC pieces.
    Returns (n_border_splits, n_pieces_flagged). Call BEFORE curated splits so their points can
    pick up the border boundaries. Meaningful on province-scale builds; on a small inland bbox
    no BLK reaches the border so it is a no-op."""
    from pipeline.splits.sectionizer import split_graph_at
    outline = bc_outline(fwa)
    if outline is None:
        return 0, 0
    pts = border_split_points(chains, outline)
    if pts:
        split_graph_at(graph, geoms, pts, fid_index)
    flagged = mark_out_of_bc(graph, geoms, outline, blks={p.blk for p in pts})
    return len(pts), flagged
