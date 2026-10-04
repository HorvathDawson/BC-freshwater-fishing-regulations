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

from pipeline.atlas.splits.anchors import _points
from pipeline.common.models import AnchorType, BlkChain, NodeKind, SplitPoint, StreamGraph


def bc_outline(fwa):
    """The BC land outline as a shapely (Multi)Polygon, or None if unavailable.

    FAST PATH: the province never changes, so the outline is precomputed once and cached at
    ``data/bc_boundary.geojson`` (see ``pipeline.atlas.splits.bc_boundary``). A cheap file read replaces
    the ~225-WMU ``union_all`` that used to dominate the border stage.

    FALLBACK (cache missing): union the WMU polygons on the fly via ``fast_wmu_union`` — this
    simplifies each poly + micro-buffers BEFORE the union, so it's also fast (~seconds) and could run
    every build; the cache is just an even-cheaper file read. Loaded whole (≈225 rows) on purpose: a
    bbox-clipped union would expose the loaded set's cut edge as a fake "border" (``fast_wmu_union``
    simplifies internally)."""
    from pipeline.atlas.splits.bc_boundary import fast_wmu_union, load_cached_boundary

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
    from pipeline.common.utils.profiling import Profiler
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
                label="BC boundary", anchor_type=AnchorType.border, source="border"))
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
    n += _mark_measure_beyond_the_data(graph, geoms)
    return n


#: A piece has to claim this many metres of route before its missing geometry means anything —
#: below it, a zero-length piece is a cut-point artifact, not a river leaving the province.
_NO_GEOM_MIN_M = 250.0


def _mark_measure_beyond_the_data(graph: StreamGraph, geoms: dict) -> int:
    """Flag pieces whose ROUTE runs on after the province's geometry stops.

    The midpoint test cannot see this case, and it is the one that matters most on a river
    leaving the country. FWA's route measure is a property of the whole blue line, including the
    part outside British Columbia; the geometry is only what B.C. holds. Where a river crosses
    out, the last piece inherits a measure span that reaches the far end and a geometry clipped
    to the border — and a zero-length line has its midpoint exactly ON the outline, which
    `contains` answers False for, so it is marked neither inside nor out.

    The CHILLIWACK is the case: its top piece reads ``down=60407 up=82832`` — a claim on 22.4 km
    — with ``geomlen=0.0``, because the river above the reserve is in Washington. The ladder drew
    those 22 km as B.C. water with no rules on it, which reads as "fish freely" on a river that
    is not in the country.

    A piece that claims kilometres and draws nothing is not a stretch. Whatever it measures is
    somewhere the province has no line for, and that is the definition of out of B.C.
    """
    n = 0
    for nid, node in list(graph.nodes.items()):
        if node.out_of_bc:
            continue
        span = (node.up_m or 0.0) - (node.down_m or 0.0)
        if span < _NO_GEOM_MIN_M:
            continue
        g = geoms.get(nid)
        if g is not None and getattr(g, "length", 0.0) >= _NO_GEOM_MIN_M:
            continue
        graph.nodes[nid] = replace(node, out_of_bc=True)
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


_AREA_MIN_OVERLAP_M = 1.0   # a piece merely TOUCHING the polygon at a cut point overlaps by 0

#: A WATERBODY is tested by its OUTLINE, and an outline drawn along a boundary leaves a sliver on
#: the far side: Williston's Zone A part lies 91.7 m² in Region 7B, its Zone B part 312 m² in 7A,
#: against 1,415 and 305 km². A share of a lake counts when it is real water — at least 0.1 ha, or
#: a tenth of the lake (so half of a 0.06 ha pond counts). Measured on the promoted build (2026-09-25)
#: over the 303,940 noded lakes: the outline keeps 418,001 of the route's 418,047 flags, drops 46
#: shoreline slivers (the two Williston zones; Intata Reach in Tweedsmuir by 933 m², Pitt Lake in
#: Pinecone Burke by 374 m²) and adds 110 where the lake reaches into an area its route does not
#: (Shuswap Lake into Cinnemousun Narrows Park by 130 ha, Stuart Lake's marine park by 106 ha).
_WATERBODY_MIN_OVERLAP_M2 = 1_000.0
_WATERBODY_MIN_SHARE = 0.10


def _waterbody_overlap_counts(part_area: float, poly_area: float) -> bool:
    """Is `part_area` of a waterbody of `poly_area` inside an area real water, not a sliver?"""
    return part_area > 0 and part_area >= min(_WATERBODY_MIN_OVERLAP_M2,
                                              _WATERBODY_MIN_SHARE * poly_area)


def _membership_geoms(graph: StreamGraph, geoms: dict, extra: dict) -> tuple[list, list, set]:
    """(node ids, geometries, indexes that are waterbody OUTLINES) for a membership pass.

    A LAKE OR WETLAND is tested by its outline whenever `extra` has it, noded or not. A noded lake's
    sidecar geometry is its under-lake ROUTE, and a route can wander across a line the lake itself
    does not cross: the Peace's route under Williston ran along the 7A/7B line near Finlay Forks, so
    both parts were in both zones and every Zone B rule bound the Zone A lake (user ruling
    2026-09-24: a noded lake's region and area membership is its polygon's). A stream piece keeps its
    line; a waterbody with no outline keeps its route."""
    from pipeline.common.models import NodeKind

    nids: list = []
    gs: list = []
    outline: set = set()
    for nid, node in graph.nodes.items():
        g = None
        if node.kind in (NodeKind.lake, NodeKind.wetland):
            g = extra.get(nid)
            if g is not None and not g.is_empty:
                outline.add(len(nids))
            else:
                g = geoms.get(nid)
        else:
            g = geoms.get(nid)
            if g is None:
                g = extra.get(nid)
        if g is not None and not g.is_empty:
            nids.append(nid)
            gs.append(g)
    return nids, gs, outline


def mark_inside_areas(graph: StreamGraph, geoms: dict, polys_by_name: dict,
                      extra: dict | None = None) -> int:
    """Batch membership: flag ``in_areas`` for every water piece that lies inside — or reaches into —
    each polygon, using ONE STRtree over the piece geometries. Returns the number of flags added.

    Two passes per polygon, because the cheap test is not the whole answer:

      1. ``covers`` — pieces wholly inside, and the normal case (18,691 of 18,909 province-wide):
         ``area_splits`` has already cut every stream crossing the polygon, so a cut water is fully in
         or fully out. It must be ``covers``, NOT ``contains``: the cut puts the piece's ENDPOINT
         exactly on the polygon boundary, which ``contains`` rejects — 1,453 fully-inside pieces fail
         that test purely because they were cut correctly.
      2. ``intersects`` minus pass 1 — the STRADDLERS, 218 streams + 44 lakes. Cutting does not reach
         everything: ``area_splits`` cuts at first-enter/last-exit only, an ``area_boundary`` split in
         splits.json is scoped to ONE named water via ``applies_to``, and a lake is never cut at all.
         Those pieces are flagged too — "within the park" means the water in the park, and dropping a
         half-inside piece loses real regulated water. Cutting is what makes the flag precise;
         intersection is what makes it complete.

    The overlap is measured, not merely tested: two pieces cut at the boundary both *touch* the
    polygon there, so a bare ``intersects`` would flag the outside neighbour of every cut. A shared
    point has zero length and zero area and is excluded; anything with real extent inside is kept.

    Covers lake and wetland nodes as well as stream pieces. The old midpoint pass tested
    ``kind == stream`` only, so 1,750 lakes lying wholly inside a park — plus 44 straddling one — were
    invisible to every ``within(area)`` rule, though a lake inside a park is closed by the same
    regulation. A MINTED waterbody (isolated lake, marsh) has no sidecar geometry at all, so ``extra``
    supplies its FWA polygon keyed by node id; without it exactly the waters that most need an area
    closure — a pond or marsh sitting inside a park — would be the ones the pass could not see.
    Idempotent (won't double-add).
    """
    from shapely.strtree import STRtree

    extra = extra or {}
    nids, gs, outline = _membership_geoms(graph, geoms, extra)
    if not gs or not polys_by_name:
        return 0
    tree = STRtree(gs)
    n = 0

    def _flag(nid: str, name: str) -> int:
        node = graph.nodes[nid]
        if name in node.in_areas:
            return 0
        graph.nodes[nid] = replace(node, in_areas=node.in_areas + (name,))
        return 1

    for name, poly in polys_by_name.items():
        if poly is None or poly.is_empty:
            continue
        inside = set(tree.query(poly, predicate="covers"))
        for i in inside:
            n += _flag(nids[i], name)
        for i in tree.query(poly, predicate="intersects"):
            if i in inside:
                continue
            part = poly.intersection(gs[i])
            if part.is_empty:
                continue
            if i in outline:
                if _waterbody_overlap_counts(part.area, gs[i].area):
                    n += _flag(nids[i], name)
            elif part.length > _AREA_MIN_OVERLAP_M or part.area > 0:
                n += _flag(nids[i], name)
    return n


def mark_mus(graph: StreamGraph, geoms: dict, mu_polys: dict,
             extra: dict | None = None) -> int:
    """Stamp ``mus`` on every water piece from the wildlife-management-unit polygons.

    The mirror of ``mark_inside_areas``, and deliberately a separate function because the two
    mean different things. ``in_areas`` is membership of a REGULATED area — a park, a reserve,
    a watershed a rule names — and exists only where somebody wrote a regulation. ``mus`` is
    administrative geography: every square metre of BC is in a management unit whether or not
    anything there is regulated.

    That distinction is load-bearing downstream. `mus` may ride on a public tile feature (it
    leaks nothing); `in_areas` may not, because we only carry the areas that are regulated, so
    "this section is in area 7" would be a regulation fact wearing a geometry costume.

    Zone regulations ("in MU 4-5, no bait") resolve through this and nothing else. Before it
    existed, MU came from the synopsis entry, so only named REGULATED water knew its MU —
    precisely the water that does not need a zone rule, since it has its own. Every unnamed
    stream had no MU at all and no zone rule could reach it.

    ``intersects``, not ``covers``: a section is not cut at MU boundaries (they are drawn by a
    ministry and get redrawn; baking one into the geometry would re-cut the atlas on every
    revision), so a section that straddles legitimately belongs to both. Measured province-wide:
    99.14% touch exactly one MU, 16,505 touch two, 196 touch three or more, coverage is 100%.
    Where they disagree the app shows the most restrictive and says so.
    """
    from shapely.strtree import STRtree

    extra = extra or {}
    nids: list = []
    gs: list = []
    for nid, node in graph.nodes.items():
        g = geoms.get(nid)
        if g is None:
            g = extra.get(nid)
        if g is not None and not g.is_empty:
            nids.append(nid)
            gs.append(g)
    if not gs or not mu_polys:
        return 0
    tree = STRtree(gs)
    n = 0
    for mu, poly in mu_polys.items():
        if poly is None or poly.is_empty:
            continue
        for i in tree.query(poly, predicate="intersects"):
            nid = nids[i]
            node = graph.nodes[nid]
            if mu in node.mus:
                continue
            graph.nodes[nid] = replace(node, mus=node.mus + (mu,))
            n += 1
    return n


def load_mu_polys(gpkg: str, bbox=None) -> dict:
    """{"2-8": (Multi)Polygon} from the wmu layer. 225 units province-wide."""
    import geopandas as gpd
    kw: dict = {"engine": "pyogrio"}
    if bbox is not None:
        kw["bbox"] = tuple(bbox)
    g = gpd.read_file(gpkg, layer="wmu", **kw)
    out: dict = {}
    for _, row in g.iterrows():
        mu = str(row.get("WILDLIFE_MGMT_UNIT_ID") or "").strip()
        geom = row.geometry
        if mu and geom is not None and not geom.is_empty:
            out[mu] = geom.union(out[mu]) if mu in out else geom
    return out


def stamp_waterbody_membership(gpkg: str, mu_polys: dict, area_polys: dict,
                               bbox=None) -> dict:
    """MU and area membership for EVERY waterbody polygon, node or not.

    A lake becomes a graph node when a stream is routed through it, and a wetland only when
    something names it. Measured province-wide, that leaves **417,111 waterbodies with no
    node at all**: 333,468 of 333,526 wetlands, 82,201 lakes and 1,442 reservoirs. They are
    real water, they are fishable, and a zone regulation ("in MU 4-5 the trout quota is 2")
    applies to every one of them — but with no node they carry no MU, so no zone rule can
    reach them and they would silently read as unregulated.

    Returns {wbk: {"mus": [...], "areas": [...]}} keyed by WATERBODY_KEY, which is the same
    key the tile exporter joins polygons on. Written as a build artifact so membership is
    computed exactly once, in the build, like everything else.
    """
    import geopandas as gpd
    from shapely.strtree import STRtree

    keys: list[str] = []
    geoms: list = []
    for layer in ("lakes", "manmade", "wetlands"):
        kw: dict = {"engine": "pyogrio", "columns": ["WATERBODY_KEY"]}
        if bbox is not None:
            kw["bbox"] = tuple(bbox)
        try:
            gdf = gpd.read_file(gpkg, layer=layer, **kw)
        except Exception:
            continue
        for k, geom in zip(gdf["WATERBODY_KEY"], gdf.geometry):
            if k is None or geom is None or geom.is_empty:
                continue
            keys.append(str(int(k)))
            geoms.append(geom)
    if not geoms:
        return {}

    tree = STRtree(geoms)
    out: dict[str, dict[str, list[str]]] = {}

    def _add(i: int, field: str, value: str) -> None:
        rec = out.setdefault(keys[i], {"mus": [], "areas": []})
        if value not in rec[field]:
            rec[field].append(value)

    for mu, poly in mu_polys.items():
        if poly is None or poly.is_empty:
            continue
        for i in tree.query(poly, predicate="intersects"):
            _add(int(i), "mus", mu)
    for area, poly in area_polys.items():
        if poly is None or poly.is_empty:
            continue
        for i in tree.query(poly, predicate="intersects"):
            _add(int(i), "areas", area)
    return out


def apply_border(fwa, graph: StreamGraph, geoms: dict, chains: list[BlkChain],
                 fid_index: Optional[dict] = None, prof=None) -> tuple[int, int]:
    """Full border pass: outline -> split cross-border BLKs -> flag out-of-BC pieces.
    Returns (n_border_splits, n_pieces_flagged). Call BEFORE curated splits so their points can
    pick up the border boundaries. Meaningful on province-scale builds; on a small inland bbox
    no BLK reaches the border so it is a no-op.

    ``prof`` (optional Profiler) attributes the ~28-min stage to outline load / split-point search /
    graph cut / out-of-BC flag; pass one or set env ``PIPELINE_PROFILE=1``."""
    from pipeline.atlas.splits.sectionizer import split_graph_at
    from pipeline.common.utils.profiling import Profiler
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
