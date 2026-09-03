"""Elevation sampling from AWS Terrarium terrain tiles (free, no auth) — used to orient stream flow by
the ground truth of elevation when the source's own line direction is unreliable.

Tiles are PNGs at ``s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png``; elevation in metres
is decoded per pixel as ``R*256 + G + B/256 - 32768``. Tiles are cached on disk and in memory, so sampling
thousands of stream nodes over a municipality only fetches a handful of tiles (each ~5 km at zoom 13).
"""

from __future__ import annotations

import io
import math
import re
import urllib.request
from pathlib import Path
from typing import Optional

from collections import defaultdict, deque

from PIL import Image
from pyproj import Transformer
from shapely.geometry import LineString, MultiPoint, Point
from shapely.ops import nearest_points, snap, split, substring, unary_union
from shapely.strtree import STRtree

_TO_ALBERS = Transformer.from_crs("EPSG:4326", "EPSG:3005", always_xy=True)
_TO_LONLAT = Transformer.from_crs("EPSG:3005", "EPSG:4326", always_xy=True)

_TILE_URL = "https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png"
_DEFAULT_CACHE = Path.home() / ".cache" / "terrarium"


class ElevationSampler:
    """Sample ground elevation (metres) at lon/lat from cached Terrarium tiles. ``zoom`` 13 ≈ 19 m/px at
    latitude 49 — enough to order a whole creek's nodes by height (find the low outlet) even if adjacent
    nodes are within the DEM's noise."""

    def __init__(self, zoom: int = 13, cache_dir: Path = _DEFAULT_CACHE):
        self.zoom = zoom
        self.cache = Path(cache_dir)
        self.cache.mkdir(parents=True, exist_ok=True)
        self._imgs: dict[tuple, Image.Image] = {}

    def _frac_xy(self, lon: float, lat: float) -> tuple[float, float]:
        n = 2 ** self.zoom
        fx = (lon + 180.0) / 360.0 * n
        fy = (1.0 - math.log(math.tan(math.radians(lat)) + 1.0 / math.cos(math.radians(lat))) / math.pi) / 2.0 * n
        return fx, fy

    def _tile(self, x: int, y: int) -> Image.Image:
        if (x, y) not in self._imgs:
            p = self.cache / f"{self.zoom}_{x}_{y}.png"
            if not p.exists():
                with urllib.request.urlopen(_TILE_URL.format(z=self.zoom, x=x, y=y), timeout=60) as r:
                    p.write_bytes(r.read())
            self._imgs[(x, y)] = Image.open(p).convert("RGB")
        return self._imgs[(x, y)]

    def elevation(self, lon: float, lat: float) -> Optional[float]:
        """Elevation in metres at lon/lat, or None on a fetch failure (caller falls back)."""
        try:
            fx, fy = self._frac_xy(lon, lat)
            x, y = int(fx), int(fy)
            img = self._tile(x, y)
            px = min(img.width - 1, max(0, int((fx - x) * img.width)))
            py = min(img.height - 1, max(0, int((fy - y) * img.height)))
            r, g, b = img.getpixel((px, py))
            return r * 256 + g + b / 256.0 - 32768.0
        except Exception:
            return None


def _metres(a: tuple, b: tuple) -> float:
    """Rough planar distance (m) between two lon/lat nodes near latitude 49."""
    dx = (a[0] - b[0]) * 72000.0
    dy = (a[1] - b[1]) * 111195.0
    return (dx * dx + dy * dy) ** 0.5


def _cluster_endpoints(pieces: list, tol_m: float) -> list[int]:
    """Cluster the 2 endpoints of every piece so endpoints within ``tol_m`` are the SAME node (the source
    leaves gaps, so exact-coord matching would call almost everything a loose end). Returns a node id per
    endpoint, indexed [2*k]=start, [2*k+1]=end of piece k. Uses a grid so it's ~linear."""
    eps = [pieces[k][1][0] if e == 0 else pieces[k][1][-1]
           for k in range(len(pieces)) for e in (0, 1)]
    tl_lon, tl_lat = tol_m / 72000.0, tol_m / 111195.0
    grid = defaultdict(list)
    for idx, (lon, lat) in enumerate(eps):
        grid[(int(lon / tl_lon), int(lat / tl_lat))].append(idx)
    uf = list(range(len(eps)))
    def find(x):
        while uf[x] != x:
            uf[x] = uf[uf[x]]; x = uf[x]
        return x
    for (gx, gy), members in grid.items():
        cand = [i for dx in (-1, 0, 1) for dy in (-1, 0, 1) for i in grid.get((gx + dx, gy + dy), [])]
        for a in members:
            for b in cand:
                if a < b and _metres(eps[a], eps[b]) <= tol_m:
                    uf[find(a)] = find(b)
    return [find(i) for i in range(len(eps))]


def _clip_fwa_overshoot(pieces: list, outlets: list, tol_m: float = 25.0) -> list:
    """Trim a municipal piece that CROSSES a drainage outlet (FWA/tidal) just before an endpoint. Municipal
    lines are often drawn a few metres PAST the water they drain into; left as-is the mouth sits beyond the
    river, so its outlet connector doubles back to it (Buena Vista Creek 359 crosses the Brunette at frac
    0.996 and ends 5.8 m past it). Clip the piece to END at the crossing when the crossing lies within
    ``tol_m`` (along-line) of an endpoint — a mid-line crossing (a real culvert/overpass) is left alone."""
    if not outlets:
        return pieces
    otree = STRtree(outlets)                                      # only intersect each piece with NEARBY outlets
    out: list = []
    for p in pieces:
        g = LineString([_TO_ALBERS.transform(x, y) for x, y in p[1]])
        ends = (Point(g.coords[0]), Point(g.coords[-1]))         # CHEAP GATE: only a piece with an ENDPOINT at an
        cand = {int(j) for ep in ends for j in otree.query(ep.buffer(tol_m))}   # outlet can overshoot it — skip
        near = [outlets[j] for j in cand if any(outlets[j].distance(ep) <= tol_m for ep in ends)]  # the rest
        if not near:
            out.append(p); continue
        inter = g.intersection(unary_union(near))
        cpts = ([inter] if inter.geom_type == "Point"
                else [q for q in getattr(inter, "geoms", []) if q.geom_type == "Point"])
        lo_cut, hi_cut = 0.0, g.length                            # keep [lo_cut, hi_cut] along g
        for q in cpts:
            d = g.project(q)
            if d <= tol_m:                                        # crossing near the START -> drop the stub before it
                lo_cut = max(lo_cut, d)
            elif g.length - d <= tol_m:                           # crossing near the END -> drop the overshoot after it
                hi_cut = min(hi_cut, d)
        if lo_cut <= 0.0 and hi_cut >= g.length:
            out.append(p); continue
        sub = substring(g, lo_cut, hi_cut)                        # the piece trimmed to the river crossing
        if sub.geom_type != "LineString" or sub.length < 1.0 or len(sub.coords) < 2:
            out.append(p); continue                               # degenerate clip -> leave the piece as-is
        out.append([p[0], [list(_TO_LONLAT.transform(x, y)) for x, y in sub.coords], p[2]])
    return out


def _node_pieces(pieces: list, tol_m: float = 5.0, end_buf: float = 25.0, min_sub_m: float = 2.0) -> list:
    """Planarize T-junctions: split any piece at the INTERIOR points where ANOTHER piece's ENDPOINT
    lands on it (within ``tol_m``). A loop that drains at its apex (Kyle Creek 204.1: both its ends are
    headwaters, its low outlet is at its interior) or a trib meeting a mainstem far from its ends becomes
    a real shared node instead of a far-endpoint jump, so BFS orientation and the drawn confluence are
    correct locally. Pure geometry, name- and source-agnostic.

    Only CLEARLY-interior touches split — a landing within ``end_buf`` of either endpoint is left to the
    endpoint-clustering / endpoint-on-line logic to snap to that end (the Little Stawamus Trib 1 case: a
    mainstem touching a trib ~10 m from its mouth must join AT the mouth, not carve off a spurious stub).

    Each output sub-reach keeps its origin ``[feat_idx, coords, name]`` (feat_idx and name duplicated
    across the splits of one feature — the caller groups reaches back by feat_idx). A piece with no
    interior touch is returned UNCHANGED (same object), so unsplit features are byte-identical."""
    geoms = [LineString([_TO_ALBERS.transform(x, y) for x, y in p[1]]) for p in pieces]
    tree = STRtree(geoms)
    ends = [(Point(g.coords[0]), Point(g.coords[-1])) for g in geoms]
    out: list = []
    for k, (p, g) in enumerate(zip(pieces, geoms)):
        cuts: list = []                                            # metres-along-g of each interior touch
        for jj in tree.query(g.buffer(tol_m)):
            j = int(jj)
            if j == k:
                continue
            for pt in ends[j]:
                if g.distance(pt) > tol_m:
                    continue
                d0 = g.project(pt)                                 # distance along g (albers metres)
                if end_buf < d0 < g.length - end_buf:              # a CLEARLY-interior landing (not near an end)
                    cuts.append(d0)
        cuts.sort()
        cuts = [d for i, d in enumerate(cuts) if i == 0 or d - cuts[i - 1] > min_sub_m]   # dedupe near-coincident
        if not cuts:
            out.append(p)
            continue
        cutmp = MultiPoint([g.interpolate(d) for d in cuts])
        for sub in split(snap(g, cutmp, 0.01), cutmp).geoms:       # split g at every interior touch
            coords = [list(_TO_LONLAT.transform(x, y)) for x, y in sub.coords]
            if len(coords) >= 2:
                out.append([p[0], coords, p[2]])
    return out


def dem_flow(features: list[dict], sampler: "ElevationSampler", tol_m: float = 25.0,
             bridge_tol: float = 400.0, sink_band: float = 3.0,
             coincident_tol: float = 1.0, name_tol: float = 600.0,
             touch_tol: float = 5.0, max_bridge: float = 50.0, trust_source: bool = False,
             lakes: list = None, lake_tol: float = 800.0,
             fwa: list = None, tidal=None) -> dict[int, dict]:
    """Terrain-driven flow model for the municipal lines. Steps (the user's model):
      1. Nodes = ONLY truly-coincident endpoints (within ``coincident_tol`` ≈ a shared vertex). Every
         wider join is a VISIBLE SPANNING BRIDGE — so nothing merges by tolerance without a connector
         showing how it connected. Bridges only ever grow a component (never a within-component loop).
      2. Build components; DROP any component containing NO named piece (pure-unnamed noise). Unnamed
         tributaries of a named stream are kept (they're in that stream's component).
      3. Bridges, tightest first, spanning-only: (B) physical — endpoint↔endpoint gaps and endpoint↔line
         mid-span joins within ``tol_m``; (C) same-name gaps within ``name_tol`` (one fragmented creek);
         (D) false minima — a component whose sink is an elevated pit is bridged downhill to a lower node
         in another component within ``bridge_tol``.
      4. Sample ground elevation at every node; each component's SINK BAND = nodes within ``sink_band`` m
         of its lowest node (several equally-low outlets collapse to one sink, not an arbitrary pick).
      5. Orient every piece TOWARD its component's sink via multi-source BFS depth (the end nearer the
         sink is downstream), over piece-edges + every bridge — guarantees one sink, no dead-ends.
    Returns (out{feat:{coords,comp}}, bridge_feats[{a,b,comp}], markers[{kind,lonlat,comp,elev}])."""
    pieces = [[i, list(f["geometry"]["coordinates"]), (f.get("properties", {}).get("name") or "").strip()]
              for i, f in enumerate(features)
              if (f.get("geometry") or {}).get("type") == "LineString"
              and len(f["geometry"].get("coordinates", [])) >= 2]
    if not pieces:
        return {}, [], []
    # PLANARIZE T-junctions FIRST: split any piece where another piece's endpoint lands on its interior,
    # so a tributary-on-mainstem-body or an apex-draining loop (Kyle Creek 204.1) becomes a real shared
    # node below, not a far-endpoint jump. Sub-reaches keep their origin feat_idx (pieces[k][0]); the
    # output loop groups them back per feature (a split feature gains a ``reaches`` list). NOT under
    # trust_source: there the source's own vertex order IS the flow, so dem-raw == raw exactly and we must
    # not re-cut its pieces (noding only exists to make DEM orientation correct across a junction).
    if not trust_source:
        pieces = _clip_fwa_overshoot(pieces, (list(fwa) if fwa else []) + ([tidal] if tidal is not None else []))
        pieces = _node_pieces(pieces, touch_tol)
    # NODES = only TRULY-COINCIDENT endpoints (a shared vertex). Every wider join becomes a visible
    # spanning bridge below, so nothing merges by tolerance without a connector showing how it connected.
    node = _cluster_endpoints(pieces, coincident_tol)              # node id per endpoint [2k]=start,[2k+1]=end
    node_coord = {}
    for k in range(len(pieces)):
        node_coord.setdefault(node[2 * k], tuple(pieces[k][1][0]))
        node_coord.setdefault(node[2 * k + 1], tuple(pieces[k][1][-1]))

    geoms = [LineString([_TO_ALBERS.transform(x, y) for x, y in p[1]]) for p in pieces]
    tree = STRtree(geoms)
    ep_alb = {nd: _TO_ALBERS.transform(*xy) for nd, xy in node_coord.items()}

    # DEDUPE parallel pieces: municipal data often draws a reach twice, so two features share the same
    # node-pair (endpoints cluster to the same nodes). Two parallel edges are a loop that doubles the flow
    # arrows and leaves an unmarked pseudo-leaf; keep only the LONGEST per node-pair so the graph is a tree.
    by_pair: dict[tuple, list[int]] = defaultdict(list)
    for k in range(len(pieces)):
        a, b = node[2 * k], node[2 * k + 1]
        if a != b:
            by_pair[(min(a, b), max(a, b))].append(k)
    dup_drop: set[int] = set()
    for ks in by_pair.values():
        if len(ks) > 1:
            ks.sort(key=lambda k: (-geoms[k].length, k))          # keep the longest (then lowest index)
            dup_drop.update(ks[1:])
    # OVERLAP dedupe: a piece drawn ON TOP of a longer one (a partial duplicate that does NOT share
    # endpoints, so the node-pair pass misses it) doubles arrows and points a stub at a silent junction.
    # Drop a piece when ~all of it lies within ``overlap_tol`` of a strictly-longer piece (tie -> lower
    # index kept). Sampled coverage; a genuine close-parallel channel diverges and won't be ~fully covered.
    overlap_tol = 4.0
    for k in range(len(pieces)):
        if k in dup_drop:
            continue
        gk = geoms[k]
        for jj in tree.query(gk.buffer(overlap_tol)):
            j = int(jj)
            if j == k or j in dup_drop:
                continue
            if (geoms[j].length, -j) <= (gk.length, -k):          # only a strictly-longer (tie: lower-idx) j covers k
                continue
            cov = sum(1 for t in range(21)
                      if geoms[j].distance(gk.interpolate(t / 20.0, normalized=True)) <= overlap_tol)
            if cov >= 19:                                          # ~all of k (>=19/21 pts) lies on j -> redundant
                dup_drop.add(k); break

    node_pieces = defaultdict(set)
    for k in range(len(pieces)):
        if k in dup_drop:
            continue
        node_pieces[node[2 * k]].add(k); node_pieces[node[2 * k + 1]].add(k)

    # attachment per piece-end (within tol_m: shared node, near endpoint, or on another line) — used ONLY
    # to decide which unnamed leaves to peel (a dangling end that touches nothing is a spurious stub).
    att = [[set(), set()] for _ in pieces]
    for k in range(len(pieces)):
        for e, nd in ((0, node[2 * k]), (1, node[2 * k + 1])):
            att[k][e] |= node_pieces[nd] - {k}                     # shared-vertex neighbours
            pt = Point(ep_alb[nd])
            for j in tree.query(pt.buffer(tol_m)):
                j = int(j)
                if j != k and pt.distance(geoms[j]) <= tol_m:
                    att[k][e].add(j)

    # PEEL unnamed leaves iteratively (an unnamed piece with a dangling end; removing one exposes the next).
    kept_set = set(range(len(pieces))) - dup_drop                  # duplicates never enter the graph
    changed = True
    while changed:
        changed = False
        for k in list(kept_set):
            if pieces[k][2]:
                continue
            if not (att[k][0] & kept_set) or not (att[k][1] & kept_set):
                kept_set.discard(k); changed = True

    parent = list(range(len(pieces)))
    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]; x = parent[x]
        return x
    def union(a, b):
        parent[find(a)] = find(b)

    # coincident base-node unions (a shared vertex -> ONE component, NO bridge drawn)
    for nd, ks in node_pieces.items():
        ks = [k for k in ks if k in kept_set]
        for j in ks[1:]:
            union(ks[0], j)
    comp_named = defaultdict(bool)
    for k in kept_set:
        if pieces[k][2]:
            comp_named[find(k)] = True
    kept = [k for k in kept_set if comp_named[find(k)]]
    if not kept:
        return {}, [], []
    keptS = set(kept)

    elev: dict[int, float] = {}
    def E(nd):
        if nd not in elev:
            e = sampler.elevation(*node_coord[nd])
            elev[nd] = 1e9 if e is None else e
        return elev[nd]
    kept_nodes = {node[2 * k] for k in kept} | {node[2 * k + 1] for k in kept}
    node_pieces_kept = defaultdict(list)
    for k in kept:
        node_pieces_kept[node[2 * k]].append(k); node_pieces_kept[node[2 * k + 1]].append(k)
    def canon(name):
        return re.sub(r"[^a-z0-9]", "", (name or "").lower())
    node_names = defaultdict(set)                                   # canonical names of pieces touching a node
    for k in kept:
        cn = canon(pieces[k][2])
        if cn:
            node_names[node[2 * k]].add(cn); node_names[node[2 * k + 1]].add(cn)

    # LAKE NODES are attached AFTER the stream-stream connections below, and only a COMPONENT's sink (or
    # an outflow leaf) links to the lake — not every loose endpoint (which drew long spurious connectors).
    lake_polys = list(lakes) if lakes else []
    lake_tree = STRtree(lake_polys) if lake_polys else None
    lake_node_piece: dict[int, int] = {}       # synthetic lake-node id (negative) -> a representative piece
    lake_dir: list[tuple] = []                 # (lake_node, endpoint_node, into_lake)
    lake_conn: list[tuple] = []                # (endpoint_node, lake_node, boundary lon/lat, into_lake) — viz
    lake_feeders: set[int] = set()             # piece endpoints that DRAIN INTO a lake (never a terminus)
    fwa_geoms = list(fwa) if fwa else []       # FWA/tidal OUTLETS (Albers) a stranded sink connects to
    fwa_tree = STRtree(fwa_geoms) if fwa_geoms else None

    # LAKE HUB: an APPROVED lake that touches/nears an FWA/tidal line drains OUT to that river, so it is a
    # real drainage terminus (Burnaby Lake -> Brunette). Precompute each such lake's outflow point; a lake
    # node created for it (below) then WINS sink-selection over a DEM noise pit, and every stream in its
    # basin orients toward the lake (Still Creek -> Burnaby Lake -> Brunette). Only these passed lakes hub.
    lake_fwa_outflow: dict[int, tuple] = {}    # lake poly idx -> the FWA/tidal outflow point (Albers) it drains to
    for li, poly in enumerate(lake_polys):
        best = None
        if fwa_tree is not None:
            for j in fwa_tree.query(poly.buffer(lake_tol)):
                g = fwa_geoms[int(j)]; d = poly.distance(g)
                if d <= lake_tol and (best is None or d < best[0]):
                    best = (d, nearest_points(poly, g)[1])
        if tidal is not None:
            d = poly.distance(tidal)
            if d <= lake_tol and (best is None or d < best[0]):
                best = (d, nearest_points(poly, tidal)[1])
        if best is not None:
            lake_fwa_outflow[li] = (best[1].x, best[1].y)
    lake_outflow_node: dict[int, tuple] = {}   # lake NODE id -> its FWA/tidal outflow lon/lat (a basin terminus)

    bridges: list[tuple] = []                                      # (node, node) visible spanning connectors
    line_bridges: list[tuple] = []                                 # (endpoint node, piece j, proj lon/lat)
    loop_edges: list[tuple] = []                                    # loop-closing physical links (endpoint↔endpoint
    #                                                                gaps AND endpoint-on-body joins): kept in the
    #                                                                flow graph (so an endpoint on an already-
    #                                                                connected piece is not a phantom-leaf source),
    #                                                                but NOT unioned or drawn — and dropped when they
    #                                                                would close a triangle (see the gadj build).
    def comp_of_node(nd):
        for k in node_pieces_kept.get(nd, ()):
            return find(k)
        return None
    def any_kept_piece(nd):
        return node_pieces_kept[nd][0]

    # --- PHASE B: physical spanning bridges, tightest first, EACH VISIBLE. Endpoint↔endpoint gaps and
    #     endpoint↔line mid-span joins within tol_m; only when they join two different components. ---
    nd_list = sorted(kept_nodes)
    nd_pts = [Point(ep_alb[nd]) for nd in nd_list]
    nd_idx = {nd: i for i, nd in enumerate(nd_list)}
    nd_tree = STRtree(nd_pts)
    cand: list[tuple] = []                                          # (gap, kind, a, b)
    for i, nd in enumerate(nd_list):                               # endpoint <-> endpoint gaps
        p = nd_pts[i]
        for jj in nd_tree.query(p.buffer(max_bridge)):
            nb = nd_list[int(jj)]
            if nb <= nd:
                continue
            d = p.distance(nd_pts[nd_idx[nb]])
            if coincident_tol < d <= max_bridge:
                cand.append((d, "ee", nd, nb))
    for k in kept:                                                 # endpoint <-> line (genuine mid-span)
        for e, nd in ((0, node[2 * k]), (1, node[2 * k + 1])):
            pt = Point(ep_alb[nd])
            for jj in tree.query(pt.buffer(max_bridge)):
                j = int(jj)
                if j == k or j not in keptS:
                    continue
                d = pt.distance(geoms[j])
                fr = geoms[j].project(pt, normalized=True)
                if d <= max_bridge and 0.02 < fr < 0.98:          # on the interior of j, not near its ends
                    cand.append((d, "el", nd, j))
    for d, kind, a, b in sorted(cand, key=lambda t: t[0]):
        if kind == "ee":
            ca, cb = comp_of_node(a), comp_of_node(b)
            if ca is None or cb is None:
                continue
            if not (node_names[a] & node_names[b]) and d > touch_tol:  # a wider cross-name gap is coincidence,
                continue                                          # not one drainage; a <=touch_tol touch is a confluence
            if ca == cb:                                          # loop-closer: connect the flow graph, no draw
                loop_edges.append((a, b)); continue               # (kept only if it does not close a triangle)
            union(any_kept_piece(a), any_kept_piece(b))
            bridges.append((a, b))
        else:                                                      # endpoint-on-line
            ca = comp_of_node(a)
            if ca is None:
                continue
            if d > touch_tol and not (node_names[a] & {canon(pieces[b][2])}):
                continue                                          # a WIDER gap onto a different-named line is not
            #                                                       bridged; a <=touch_tol endpoint-on-body is a confluence
            fr = geoms[b].project(Point(ep_alb[a]), normalized=True)
            near = node[2 * b] if fr < 0.5 else node[2 * b + 1]     # attach at the NEARER end of j, so the
            if ca == find(b):                                      # loop-closer: keep the endpoint connected in
                loop_edges.append((a, near)); continue             # the flow graph, but do not union or draw
            union(any_kept_piece(a), b)
            pr = geoms[b].interpolate(fr, normalized=True)
            visible = d > coincident_tol                            # a ~0 m touch is silent (no gap to span)
            line_bridges.append((a, b, tuple(_TO_LONLAT.transform(pr.x, pr.y)), near, visible))

    # --- PHASE C: SAME-NAME gap bridges (a creek fragmented with wider gaps is ONE stream), tightest
    #     first, spanning-only, each visible. ---
    by_name = defaultdict(list)
    for k in kept:
        cn = re.sub(r"[^a-z0-9]", "", pieces[k][2].lower())
        if cn:
            by_name[cn].append(k)
    for ks in by_name.values():
        if len(ks) < 2:
            continue
        pairs = []
        for x in range(len(ks)):
            for y in range(x + 1, len(ks)):
                kx, ky = ks[x], ks[y]
                best = None
                for na in (node[2 * kx], node[2 * kx + 1]):
                    for nb in (node[2 * ky], node[2 * ky + 1]):
                        dd = _metres(node_coord[na], node_coord[nb])
                        if dd <= max_bridge and (best is None or dd < best[0]):
                            best = (dd, na, nb)
                if best:
                    pairs.append((best[0], kx, ky, best[1], best[2]))
        for dd, kx, ky, na, nb in sorted(pairs):
            if find(kx) != find(ky):
                union(kx, ky); bridges.append((na, nb))

    # NOTE: the old false-minima phase (bridge an elevated-pit component downhill to ANY nearby lower
    # component) was REMOVED — it bridged unrelated cross-name streams and produced spurious mega-
    # components with one wrong sink. Bridges now only ever join SAME-NAME pieces (Phases B/C).

    # 5. orient every piece TOWARD its component's single sink — a raindrop walk. BFS depth from the whole
    #    sink band (multi-source) over the node graph (each piece is an edge between its 2 nodes, plus
    #    endpoint-on-line links), so the end nearer the sink is downstream. This guarantees ONE sink with
    #    no spurious mid-network dead-ends (which per-piece elevation produced on noisy/flat reaches).
    # global node adjacency for the flow BFS: each piece is an edge between its 2 nodes, plus every
    # spanning bridge (node-node gap fills AND endpoint-to-line joins) so BFS propagates across them.
    gadj = defaultdict(set)
    for k in kept:
        n0, n1 = node[2 * k], node[2 * k + 1]
        gadj[n0].add(n1); gadj[n1].add(n0)
    for sn, tn in bridges:
        gadj[sn].add(tn); gadj[tn].add(sn)
    for a_nd, j, proj, near, visible in line_bridges:               # attach only to the NEARER end of j:
        gadj[a_nd].add(near); gadj[near].add(a_nd)                  # linking both ends would tie their depth
    # loop-closers keep an endpoint connected (so an endpoint on an already-connected piece is not a
    # phantom-leaf source) — BUT a link between two HEADWATERS (each higher than all its real neighbours)
    # is spurious: it hides two true source leaves and draws a false Y (Noble Creek 174/175, Hutchinson
    # 205/206 — same-name headwater tips a few tens of m apart that same-name bridging fused). The genuine
    # phantom-leaf case is a low MOUTH landing on a body (lower than its neighbour), which we still keep.
    base_nb = {nd: set(ns) for nd, ns in gadj.items()}             # neighbours from pieces + bridges only
    def _is_headwater(nd):
        ns = base_nb.get(nd)
        return bool(ns) and E(nd) > max(E(n) for n in ns)
    for u, v in loop_edges:
        if (base_nb.get(u) or set()) & (base_nb.get(v) or set()):  # closes a triangle (shared junction)
            continue
        if _is_headwater(u) and _is_headwater(v):                  # links two headwater tips
            continue
        gadj[u].add(v); gadj[v].add(u)

    # SOURCE-directed in/out degree from piece order (up=node[2k] -> down=node[2k+1]) + base feeders (a
    # mouth landing on a mainstem body). Used to pick each component's SINK (its downstream terminus).
    p_indeg = defaultdict(int); p_outdeg = defaultdict(int)
    for k in kept:
        p_outdeg[node[2 * k]] += 1; p_indeg[node[2 * k + 1]] += 1
    base_feeders = {a_nd for a_nd, j, proj, near, visible in line_bridges}

    def _near_outlet(nd):                                          # node sits AT an FWA/tidal drainage outlet
        if nd < 0:                                                 # (a real terminus, even when a trib joins
            return nd in lake_outflow_node                         # there). A lake WITH an FWA/tidal outflow IS a
        p = Point(ep_alb[nd]); r = tol_m                           # terminus; a lake without one is a pass-through
        if fwa_tree is not None:                                   # outflow), handled by the lake-node logic.
            for j in fwa_tree.query(p.buffer(r)):
                if p.distance(fwa_geoms[int(j)]) <= r:
                    return True
        if tidal is not None and p.distance(tidal) <= r:
            return True
        return False

    def _outlet_point(nd, r):                                     # nearest FWA/tidal point within r m (lonlat),
        if nd < 0:                                                # or None — a creek terminus AT the river drains
            return None                                           # to it (used to seed the multi-outlet flow BFS)
        p = Point(ep_alb[nd]); best = None
        if fwa_tree is not None:
            for j in fwa_tree.query(p.buffer(r)):
                g = fwa_geoms[int(j)]; d = p.distance(g)
                if d <= r and (best is None or d < best[0]):
                    best = (d, nearest_points(p, g)[1])
        if tidal is not None:
            d = p.distance(tidal)
            if d <= r and (best is None or d < best[0]):
                best = (d, nearest_points(p, tidal)[1])
        return None if best is None else tuple(_TO_LONLAT.transform(best[1].x, best[1].y))

    def _sink_of(nodes, adj, indeg, outdeg, feeders):
        lake_sinks = [nd for nd in nodes if nd in lake_outflow_node]  # a lake draining to a real river is THE
        if lake_sinks:                                             # basin terminus — robust to DEM noise / a
            return min(lake_sinks, key=E)                          # spurious pit far from the actual outlet
        if trust_source:                                           # sink = downstream terminus of the source
            termini = [nd for nd in nodes if outdeg[nd] == 0 and nd not in feeders]
            recv = [nd for nd in termini if indeg[nd] > 0] or termini \
                or [nd for nd in nodes if outdeg[nd] == 0] or list(nodes)
            return min(recv, key=E)
        lo = min(E(nd) for nd in nodes)                            # DEM: the flat low band (noise ~ several m)
        band = [nd for nd in nodes if E(nd) <= lo + sink_band]
        outlet = [nd for nd in band if _near_outlet(nd)]           # a node AT the drainage outlet wins even if
        if outlet:                                                 # a trib joins there (so it is not a leaf) —
            return min(outlet, key=E)                              # else the outlet reversed the whole component
        leaves = [nd for nd in band if len(adj[nd]) <= 1]          # no outlet in the band: lowest LEAF terminus
        return min(leaves, key=E) if leaves else min(band, key=E)

    # OUTLET ATTACH — AFTER the stream-stream connections. For each provisional component, connect its
    # SINK to the nearest OUTLET within lake_tol, preferring an FWA/tidal line over a lake (so an estuary
    # creek roots at the river it actually drains to, NOT a far lake that would merge unrelated creeks).
    # An FWA/tidal outlet draws a connector, no merge; a lake merges every component draining it (one lake
    # node per polygon); if the sink isn't near an outlet but a terminus leaf is at a lake, the lake FEEDS
    # it (outflow). Attaches component termini only — not every loose endpoint — so connectors stay few.
    if lake_tree is not None or fwa_tree is not None or tidal is not None:
        next_lake_id = -1
        lake_of_poly: dict[int, int] = {}

        def _nearest_lake(nd):
            if lake_tree is None:
                return None
            p = Point(ep_alb[nd]); best = None
            for li in lake_tree.query(p.buffer(lake_tol)):
                li = int(li); d = p.distance(lake_polys[li])
                if d <= lake_tol and (best is None or d < best[1]):
                    best = (li, d)
            return best

        def _nearest_fwa_tidal(nd):                                # nearest FWA/tidal OUTLET point (Albers)
            p = Point(ep_alb[nd]); best = None
            if fwa_tree is not None:
                for j in fwa_tree.query(p.buffer(lake_tol)):
                    g = fwa_geoms[int(j)]; d = p.distance(g)
                    if d <= lake_tol and (best is None or d < best[0]):
                        pr = nearest_points(p, g)[1]; best = (d, (pr.x, pr.y))
            if tidal is not None:
                d = p.distance(tidal)
                if d <= lake_tol and (best is None or d < best[0]):
                    pr = nearest_points(p, tidal)[1]; best = (d, (pr.x, pr.y))
            return best

        def _attach(nd, li, into):
            nonlocal next_lake_id
            if li not in lake_of_poly:
                ln = next_lake_id; next_lake_id -= 1; lake_of_poly[li] = ln
                cen = lake_polys[li].centroid
                node_coord[ln] = tuple(_TO_LONLAT.transform(cen.x, cen.y))
                lake_node_piece[ln] = node_pieces_kept[nd][0]
                if li in lake_fwa_outflow:                          # this lake drains OUT to an FWA/tidal river:
                    opt = lake_fwa_outflow[li]                      # it is a basin terminus (seeds the flow BFS and
                    lake_outflow_node[ln] = tuple(_TO_LONLAT.transform(*opt))  # draws a lake -> river connector)
            ln = lake_of_poly[li]
            lake_dir.append((ln, nd, into))
            if into:
                lake_feeders.add(nd)
            bpt = nearest_points(Point(ep_alb[nd]), lake_polys[li].boundary)[1]
            lake_conn.append((nd, ln, tuple(_TO_LONLAT.transform(bpt.x, bpt.y)), into))
            union(lake_node_piece[ln], node_pieces_kept[nd][0])
            gadj[ln].add(nd); gadj[nd].add(ln)

        prov = defaultdict(list)
        for k in kept:
            prov[find(k)].append(k)
        for cid, comp in prov.items():
            cnodes = {node[2 * k] for k in comp} | {node[2 * k + 1] for k in comp}
            adjc = {nd: gadj[nd] for nd in cnodes}
            sink = _sink_of(cnodes, adjc, p_indeg, p_outdeg, base_feeders)
            ot = _nearest_fwa_tidal(sink); nl = _nearest_lake(sink)
            sink_lake = None
            if ot is not None and (nl is None or ot[0] <= nl[1]):  # nearer an FWA/tidal line than a lake:
                pass                                               # do NOT merge into a lake — the multi-outlet
                #                                                    BFS below seeds this sink from that river
            elif nl is not None:
                _attach(sink, nl[0], True); sink_lake = nl[0]     # component drains INTO the lake
            # OUTFLOW: a terminus LEAF (other than the sink) touching a lake is an outlet the lake FEEDS
            # (Deer Lake Brook: mouth -> Still Ck, source -> Deer Lake), so the lake connects to its outflow
            # and the whole lake system is ONE component. Runs even when the SINK drained into a DIFFERENT
            # lake — a reach can drain into lake B at its mouth and be lake A's outflow at its source; only
            # a second link to the SAME lake (sink_lake) is skipped (that would be a redundant self-loop).
            best = None
            for nd in cnodes:
                if nd == sink or len(adjc[nd]) > 1:
                    continue
                n2 = _nearest_lake(nd)
                if n2 is not None and n2[0] != sink_lake and (best is None or n2[1] < best[2]):
                    best = (nd, n2[0], n2[1])
            if best is not None:
                _attach(best[0], best[1], False)                  # lake FEEDS this component (outflow)

        # HUB INFLOWS: beyond the one component-sink attached above, EVERY kept terminus (a degree-1 leaf
        # that is the LOW/mouth end of its piece) sitting within lake_edge_tol of a lake shoreline drains
        # INTO that lake, so an APPROVED lake visibly hubs its whole basin. The tight tolerance + the mouth
        # test keep unrelated passers-by and the lake's own outflow (a HIGH source leaf) from being pulled in.
        lake_edge_tol = 40.0
        for nd in (sorted(kept_nodes) if lake_tree is not None else ()):
            if nd in lake_feeders or len(gadj[nd]) != 1:          # already an inflow, or not a leaf terminus
                continue
            nb = next(iter(gadj[nd]))
            if E(nd) > E(nb):                                     # a HIGH leaf is an outflow source, not a mouth
                continue
            p = Point(ep_alb[nd]); best = None
            for li in lake_tree.query(p.buffer(lake_edge_tol)):
                li = int(li); d = p.distance(lake_polys[li])
                if d <= lake_edge_tol and (best is None or d < best[1]):
                    best = (li, d)
            if best is not None:
                _attach(nd, best[0], True)                        # this mouth drains INTO the lake

    comps = defaultdict(list)
    for k in kept:
        comps[find(k)].append(k)
    edge_piece: dict[tuple, int] = {}                               # node-pair -> a kept piece on that edge
    for k in kept:
        a, b = node[2 * k], node[2 * k + 1]
        edge_piece[(min(a, b), max(a, b))] = k                     # (the DOWNSTREAM tree link uses this to name
    #                                                                the piece carrying flow between two nodes)
    piece_res: dict[int, dict] = {}                                # per KEPT PIECE k -> {coords,comp,down,outlet}
    #                                                                (a feature noded into >1 reach has >1 entry;
    #                                                                grouped back into ``out`` per feat_idx below)
    markers: list[dict] = []                                        # per component: 1 sink (square), N sources
    gdepth: dict[int, int] = {}                                     # node -> BFS depth from its sink (for bridge dir)
    final_sinks: set = set()                                        # every FINAL outlet node (per component there
    #                                                                may be several — a river has many mouths)
    outlet_draw: list = []                                          # (outlet node, FWA/tidal lon/lat) to draw
    for cid, comp in comps.items():
        nodes0 = {node[2 * k] for k in comp} | {node[2 * k + 1] for k in comp}
        comp_lakes = [ln for ln, pk in lake_node_piece.items() if find(pk) == cid]
        nodes0 |= set(comp_lakes)
        adj = {nd: gadj[nd] for nd in nodes0}
        nodes = set(adj)
        lo = min(E(nd) for nd in nodes)
        band = [nd for nd in nodes if E(nd) <= lo + sink_band]      # the flat low area (DEM noise ~ several m)
        indeg = defaultdict(int); outdeg = defaultdict(int)         # SOURCE-directed flow (up=node[2k] -> down)
        for k in comp:
            outdeg[node[2 * k]] += 1; indeg[node[2 * k + 1]] += 1
        for ln, nd, into in lake_dir:                              # lake edges: inflow -> indeg[lake], outflow
            if ln in comp_lakes:                                    # -> outdeg[lake] (so a lake WITH an outflow
                (indeg if into else outdeg)[ln] += 1               # is a pass-through, not a terminus)
        feeders = base_feeders | lake_feeders                      # a mouth on a mainstem / draining into a
        # MULTI-OUTLET: a river/tidal edge has MANY tributary mouths, so seed the flow BFS from EVERY node
        # that reaches the river (a terminus leaf within outlet_seed_tol, or a lake draining to one) — each
        # creek then flows to its OWN nearest river contact instead of being reversed up into a neighbour
        # that merely shares the component (Rudolph / Ancient Grove -> Brunette, not up into Trolley Creek).
        outlet_seed_tol = 60.0
        seed_outlet: dict = {}                                     # seed node -> its FWA/tidal outlet lon/lat
        for nd in nodes:
            if nd in lake_outflow_node:                            # a lake draining to a river is a terminus
                seed_outlet[nd] = lake_outflow_node[nd]
                continue
            if nd < 0:
                continue
            op = _outlet_point(nd, outlet_seed_tol)                # a stream node sitting AT the river that is a
            if op is not None and adj[nd] and E(nd) <= min(E(v) for v in adj[nd]):  # LOCAL LOW = a drainage mouth
                seed_outlet[nd] = op                               # (NOT a high headwater that merely sits near a
                #                                                    headwater FWA reach — that reversed Squatters)
        seeds = list(seed_outlet)
        if not seeds:                                             # no creek sits AT the river: a STRANDED
            sk = _sink_of(nodes, adj, indeg, outdeg, feeders)     # component — its single lowest sink connects
            seeds = [sk]                                          # to the nearest FWA/tidal within dem's reach
            op = _outlet_point(sk, lake_tol)                      # (~lake_tol, e.g. a 300 m mouth), if any
            if op is not None:
                seed_outlet[sk] = op
        depth = {s: 0 for s in seeds}                              # MULTI-source raindrop walk: every node flows
        pnode: dict[int, Optional[int]] = {s: None for s in seeds} # to its NEAREST outlet (BFS parent = downstream)
        q = deque(seeds)
        while q:
            u = q.popleft()
            for v in adj[u]:
                if v not in depth:
                    depth[v] = depth[u] + 1; pnode[v] = u; q.append(v)
        gdepth.update(depth)
        def _down_piece(pk, m):                                     # first PIECE the flow enters downstream of
            cur = m                                                 # node m, walking the sink-rooted tree — hops
            while True:                                             # OVER bridge/lake/loop edges (a trib joins its
                pd2 = pnode.get(cur)                                # mainstem via a connector, not a shared vertex,
                if pd2 is None:                                     # so its downstream piece is across that bridge)
                    return None
                q = edge_piece.get((min(cur, pd2), max(cur, pd2)))
                if q is not None and q != pk:
                    return q
                cur = pd2
        for k in comp:
            i, c, _ = pieces[k]; a, b = node[2 * k], node[2 * k + 1]
            if trust_source:                                        # reliable source: flow = source vertex
                flip = True                                         # order (drawn upstream-first), so mouth-
            else:                                                   # first output = reversed(source)
                da, db = depth.get(a, 1 << 30), depth.get(b, 1 << 30)
                flip = da > db or (da == db and E(a) > E(b))       # nearer the sink is downstream
            mouth_nd = b if flip else a                             # mouth end = the DOWNSTREAM end (coords[0])
            carrier = _down_piece(k, mouth_nd)
            piece_res[k] = {"coords": c[::-1] if flip else c, "comp": int(cid),
                            "down": pieces[carrier][0] if carrier is not None else None,
                            "outlet": (list(seed_outlet[mouth_nd])      # a mouth AT the river carries its outlet
                                       if mouth_nd in seed_outlet else None),   # (drains to that FWA/tidal point)
                            "_md": depth.get(mouth_nd, 1 << 30)}    # mouth depth: the MOUTH reach of a split feature
        for s in seeds:                                           # one sink marker per outlet (a river has many)
            final_sinks.add(s)
            if s in seed_outlet:
                outlet_draw.append((s, seed_outlet[s]))
            markers.append({"kind": "sink", "lonlat": list(node_coord[s]), "comp": int(cid),
                            "elev": round(E(s), 1)})
        for nd in nodes:                                           # SOURCES = headwaters. trust_source: a node with
            is_src = (indeg[nd] == 0 and outdeg[nd] > 0) if trust_source \
                else (len(adj[nd]) == 1 and nd not in seed_outlet and nd not in final_sinks)  # a DEM leaf that is
            if is_src:                                             # neither a mouth-outlet nor a sink is a headwater
                markers.append({"kind": "source", "lonlat": list(node_coord[nd]),          # — even on FLAT ground
                                "comp": int(cid), "elev": round(E(nd), 1)})                 # (tidal sloughs), where no
                #                                                    leaf rises sink_band above the component low
    # GROUP kept pieces back per feature. Unsplit feature -> ONE piece -> ``out[i]`` is byte-identical to
    # before. A feature noded into >1 reach -> ``out[i]`` carries the primary (longest) reach for back-
    # compat single-arrow consumers PLUS a ``reaches`` list (each {coords,down,outlet}) so the resolver /
    # the dem-raw view see every reach (an apex-draining loop's two arms both flow to the shared node).
    out: dict[int, dict] = {}
    by_feat: dict[int, list[int]] = defaultdict(list)
    for k in piece_res:
        by_feat[pieces[k][0]].append(k)
    for i, ks in by_feat.items():
        if len(ks) == 1:
            out[i] = {kk: vv for kk, vv in piece_res[ks[0]].items() if kk != "_md"}
            continue
        # primary = the MOUTH reach (nearest the sink) so out[i]["coords"][0] stays the feature's true
        # mouth for back-compat single-arrow consumers; tie -> the longer reach.
        ks.sort(key=lambda k: (piece_res[k]["_md"], -geoms[k].length))
        prim = piece_res[ks[0]]
        out[i] = {"coords": prim["coords"], "comp": prim["comp"], "down": prim["down"],
                  "outlet": prim["outlet"],
                  "reaches": [{"coords": piece_res[k]["coords"], "down": piece_res[k]["down"],
                               "outlet": piece_res[k]["outlet"]} for k in ks]}
    INF = 1 << 30
    bridge_feats = []
    for sn, tn in bridges:                                          # order a->b DOWNSTREAM (higher depth first)
        up, dn = (sn, tn) if gdepth.get(sn, INF) >= gdepth.get(tn, INF) else (tn, sn)
        bridge_feats.append({"a": list(node_coord[up]), "b": list(node_coord[dn]),
                             "comp": int(find(node_pieces_kept[sn][0]))})
    for a_nd, j, proj, near, visible in line_bridges:
        if not visible:
            continue
        ep, pt = list(node_coord[a_nd]), list(proj)                 # arrow points downstream: the endpoint is
        if gdepth.get(a_nd, INF) >= gdepth.get(near, INF):         # upstream when it's deeper than its landing
            a_, b_ = ep, pt
        else:
            a_, b_ = pt, ep
        bridge_feats.append({"a": a_, "b": b_, "comp": int(comp_of_node(a_nd))})
    for nd, ln, bpt, into in lake_conn:                              # lake connectors: short link endpoint <->
        ep = list(node_coord[nd])                                    # lake boundary, arrow following the flow
        a_, b_ = (ep, list(bpt)) if into else (list(bpt), ep)       # inflow -> lake; outflow lake -> stream
        bridge_feats.append({"a": a_, "b": b_, "comp": int(find(lake_node_piece[ln]))})
    for nd, opt in outlet_draw:                                      # each outlet node -> its FWA/tidal point
        pk = lake_node_piece[nd] if nd < 0 else node_pieces_kept[nd][0]   # lake node vs stream node
        bridge_feats.append({"a": list(node_coord[nd]), "b": list(opt), "comp": int(find(pk))})
    return out, bridge_feats, markers


