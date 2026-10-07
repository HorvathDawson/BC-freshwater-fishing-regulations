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

The BC outline is the EXACT union of the WMU polygons (a valid coverage: they tile the province),
the same polygons the regions are dissolved from — see ``pipeline.atlas.splits.bc_boundary``.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Optional

from shapely.geometry import Point
from shapely.ops import unary_union

from pipeline.atlas.splits.anchors import _points
from pipeline.common.models import AnchorType, BlkChain, NodeKind, SplitPoint, StreamGraph


def bc_outline(fwa):
    """The B.C. outline: the EXACT union of the `wmu` units the regions are dissolved from
    (`bc_boundary.load_outline`, cached beside the gpkg as WKB). Its edge is the regions' outer
    edge, vertex for vertex, so a river leaving the province through a region's edge is cut ONCE
    (`sectionizer` aliases the region cut onto the border cut at the same place). None without
    the layer."""
    from pipeline.atlas.splits.bc_boundary import load_outline

    return load_outline(fwa.gpkg_path)


#: THE BORDER'S CROSSING ZONE (BOUND round, 2026-10-06). FWA draws a stream on past the province
#: — tens of metres on the U.S. line, 70-90 m (p10-p50) on the Alberta 120th meridian — and then
#: stops, so a line leaving B.C. usually ENDS a little beyond the outline. Measured over the 1,402
#: blue lines crossing the exact outline (1,644 crossings): 61 cross it within 5 m of the line's own
#: end and 123 within 10 m (the U.S. 49th and the Alaska panhandle hold most: Elmer Creek's ends
#: 0.21 m past the line). Cut there, each left a stub of 0.2-4.7 m "outside B.C." that is not a
#: stretch of anything: the two datasets agree on the border only to about their 1:20,000 mapping
#: accuracy. So the border is cut by the clean-cut rule (`clean_cut.decide_chain`): crossings under
#: 10 m apart are one zone (one cut at its median when the side changes, none for a graze), and a
#: zone within 10 m of a stretch's end — the line's own end, or a lake edge (a border cut 0.86 m
#: below a lake on 359342465) — takes no side there: the end goes with the water it is attached to.
#: Real crossings are untouched: the Kootenay's two (168,733 and 431,761 m) and the Tatshenshini's
#: keep their ids (`border:{blk}:{i}`, numbered over the CUTS in measure order).
BORDER_CROSSING_ZONE_M = 10.0


#: Vertices per outline chunk in the prefilter's STRtree.
_CHUNK = 64


def outline_chunks(outline):
    """The outline's rings cut into short runs of `_CHUNK` vertices — a spatial index can then
    return only the stretch of the 54,000-vertex B.C. outline near a line."""
    import shapely
    from shapely.geometry import LineString

    out = []
    for ring in shapely.get_rings(shapely.get_parts(outline)):
        xy = list(ring.coords)
        for i in range(0, len(xy) - 1, _CHUNK):
            out.append(LineString(xy[i:i + _CHUNK + 1]))
    return out


def candidates(arr, outline):
    """Which lines can cross the outline: those meeting its edge, and those lying wholly outside.

    THE SAME SET THE OLD PREFILTER GAVE (`not covered_by(line, outline)`), minus nothing a cut can
    come from: a line that does not meet the edge lies wholly inside or wholly outside, and its first
    vertex says which. A line inside that merely touches the edge is now examined too; it has no
    crossing that changes side, so it yields no cut. FAST because the edge test runs against the
    outline in 64-vertex chunks through an STRtree (one bulk query) instead of 1.2 million
    covered_by tests against a 54,000-vertex polygon — 593 s before, seconds now; the cuts are
    unchanged (`test_border.py::test_the_fast_prefilter_finds_the_same_cuts`)."""
    import numpy as np
    import shapely
    from shapely.strtree import STRtree

    tree = STRtree(outline_chunks(outline))
    hit = np.zeros(len(arr), dtype=bool)
    q = tree.query(arr, predicate="intersects")
    hit[np.unique(q[0])] = True
    first = shapely.get_point(arr, 0)
    shapely.prepare(outline)
    outside = ~shapely.contains_xy(outline, shapely.get_x(first), shapely.get_y(first))
    return hit | outside


def border_split_points(chains: list[BlkChain], outline, prof=None,
                        inside_out: Optional[dict] = None) -> list[SplitPoint]:
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
    with prof.phase("  edge/outside prefilter (STRtree over outline chunks)"):
        arr = np.fromiter((c.geometry for c in valid), dtype=object, count=len(valid))
        covered = ~candidates(arr, outline)        # True = cannot cross the border

    import time

    from pipeline.atlas.splits.clean_cut import decide_chain
    boundary = outline.boundary
    shapely.prepare(boundary)
    out: list[SplitPoint] = []
    _t = time.perf_counter()
    for c, cov in zip(valid, covered):
        if cov:
            continue                               # fully inland: skip the intersection entirely
        g = c.geometry
        # THE BORDER IS CUT CLEAN (BOUND round, 2026-10-06): the same rule as a reserve's edge, with
        # the border's own crossing zone — see BORDER_CROSSING_ZONE_M.
        cuts, inside, _grazes = decide_chain(c, outline, boundary, BORDER_CROSSING_ZONE_M)
        if inside_out is not None:                  # the stretches in B.C. (regions cut only these)
            inside_out[c.blk] = inside
        for i, (M, *_rest) in enumerate(cuts):
            out.append(SplitPoint(
                split_id=f"border:{c.blk}:{i}", blk=c.blk,
                route_measure=M, fid="",
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
    import shapely
    shapely.prepare(poly)
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
                      extra: dict | None = None, cutter: dict | None = None,
                      straddlers: list | None = None) -> int:
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

    A CLEAN-CUT AREA (`cutter`: {area name: {blk: [(lo, hi), …]}} — the inside measure intervals
    `clean_cut.resolve_clean_cuts` decided) is NOT tested by overlap for its stream pieces: the cutter
    already said which stretches are inside, and a piece is a member exactly when it lies in one of
    them. A first_last area keeps the overlap test (both sides). A piece that straddles an interval end means a cut the cutter asked for did not happen; it
    is not flagged, it is appended to `straddlers` (area, node id) for the build's gate to refuse.
    Lakes and wetlands are never cut and keep the outline test.
    """
    from shapely.strtree import STRtree

    extra = extra or {}
    cutter = cutter or {}
    by_blk: dict[str, list[str]] = {}
    if cutter:
        for nid, node in graph.nodes.items():
            if node.kind == NodeKind.stream and node.blk:
                by_blk.setdefault(node.blk, []).append(nid)
    nids, gs, outline = _membership_geoms(graph, geoms, extra)
    if not polys_by_name or (not gs and not cutter):
        return 0
    tree = STRtree(gs) if gs else None
    n = 0

    def _flag(nid: str, name: str) -> int:
        node = graph.nodes[nid]
        if name in node.in_areas:
            return 0
        graph.nodes[nid] = replace(node, in_areas=node.in_areas + (name,))
        return 1

    from pipeline.atlas.splits.clean_cut import SAME_PLACE_M

    for name, poly in polys_by_name.items():
        if poly is None or poly.is_empty:
            continue
        if name in cutter:
            for blk, spans in sorted(cutter.get(name, {}).items()):
                for nid in by_blk.get(blk, ()):
                    node = graph.nodes[nid]
                    lo_, hi_ = node.down_m, node.up_m
                    if any(lo - SAME_PLACE_M <= lo_ and hi_ <= hi + SAME_PLACE_M for lo, hi in spans):
                        n += _flag(nid, name)
                    elif any(lo_ < e - SAME_PLACE_M and e + SAME_PLACE_M < hi_
                             for lo, hi in spans for e in (lo, hi)) and straddlers is not None:
                        straddlers.append((name, nid, lo_, hi_, tuple(spans)))
            if tree is None:
                continue
            covered = set(tree.query(poly, predicate="covers"))
            for i in tree.query(poly, predicate="intersects"):     # waterbodies, as for any area
                if graph.nodes[nids[i]].kind == NodeKind.stream:
                    continue
                if i in covered:
                    n += _flag(nids[i], name)
                    continue
                part = poly.intersection(gs[i])
                if part.is_empty:
                    continue
                if (_waterbody_overlap_counts(part.area, gs[i].area) if i in outline
                        else part.length > _AREA_MIN_OVERLAP_M or part.area > 0
                        or poly.covers(gs[i])):
                    n += _flag(nids[i], name)
            continue
        if tree is None:
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
                 fid_index: Optional[dict] = None, prof=None,
                 cuts_out: Optional[dict] = None,
                 inside_out: Optional[dict] = None) -> tuple[int, int]:
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
        pts = border_split_points(chains, outline, prof=prof, inside_out=inside_out)
    with prof.phase("split_graph_at"):
        if pts:
            split_graph_at(graph, geoms, pts, fid_index)
    if cuts_out is not None:          # {blk: [measures]} — the region cutter defers to these
        for p in pts:
            cuts_out.setdefault(p.blk, []).append(p.route_measure)
    with prof.phase("mark_out_of_bc"):
        flagged = mark_out_of_bc(graph, geoms, outline, blks={p.blk for p in pts})
    prof.report("border")
    return len(pts), flagged
