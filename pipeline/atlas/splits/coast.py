"""T678 / T685 THE COAST CUT: where a stream's tidal water ends, decided ONCE, in the atlas's cut sequence.

Owner 2026-11-11, verbatim: "all the tidal logic and stream removal happens in the build of the graph", "the cuts used
for tidal should happen where cuts happen". Owner 2026-11-12 (RULINGS E21-E24): "we want a single rule / a consistent
definition". The order of sources (the owner's edits do not outrank the default):

  1. A DFO WRITTEN BOUNDARY (a sign, a bridge, a written line): a curated `split` row of `waters/tidal_limits.json`
     classified as DFO-written (`authority`), a split the owner made his own decision (authority `owner`), a `cut`
     row, and the owner's `whole` rows (his list of whole tidal waters: the Fraser's channels, nine sloughs). Its
     water is cut by that row, never here.
  2. OTHERWISE THE ONE COAST RULE (E21), cut at THE EDGE OF THE RECEIVING WATER, read off the map's own polygons:
     (a) a water that reaches the SEA is fresh to the shoreline (the mouth line): only a stretch the line is drawn
         over the sea itself (outside the FWA land mass) is tidal. Nearly every line ends on the coastline: no cut.
     (b) a creek that enters a TIDAL part of a river is cut at THE RIVER'S EDGE: where its line crosses out of the
         river polygon its mouth sits in (`CoastGeometry.river_at`: chosen by where the mouth IS, never by an FWA
         key). Only the piece drawn inside the river's water is tidal. A river drawn as a single line there has no
         water area: nothing on the creek is tidal. A creek entering a FRESH part is never looked at.
     FWA's own vertex for the same place (the top of the line's lowest edge-1450 run, `edge1450_tops`) is recorded
     beside it and taken as the cut when it is the same place (`SAME_PLACE_M`); it never decides.
  3. The DFO subarea polygon is MAP SHADING ONLY. It never makes a cut.

Also decided here, in the same pass:
  * A FRESH STUB under `STUB_M` at a line's top counts as tidal: the whole line is tidal (owner A).
  * A line wholly in tidal water STAYS IN THE GRAPH, marked tidal (owner B): `whole` rows, never deleted.
  * THE FRASER'S OWN CHANNELS AND ARMS below its tidal boundary are tidal for their length (owner, verbatim: "We still
    want all channels and stuff of Fraser below it's tidal boundary as tidal"): a named line on the Fraser's own
    watershed code (`FRASER_WSC`) whose outlets are all tidal. SLOUGHS ARE NOT CHANNELS: a slough, and an unnamed
    line on the Fraser's code, follows rule (b) like any creek (E24) unless the owner listed it whole (rank 1).
  * A cut is made only where the piece below it goes tidal (every outlet below the cut tidal water), a least fixpoint.
  * THE OWNER'S EXPLICIT RULINGS on single lines (`waters/shore_held.json`, every row `decided`) are kept as written.

The cut keeps the shore family's id, `shore:<blk>:<int measure>` (`SOURCE`), so every reader of a shore boundary
reads it unchanged. Pure functions of the graph and plain inputs; the build feeds them (`build.cut_shores`).
"""
from __future__ import annotations

from typing import Callable, Mapping

from pipeline.common.models import AnchorType, NodeKind, SplitPoint, StreamGraph

#: the cut family (`SplitPoint.source`, `sectionizer.AUTO_SOURCES`) and id prefix: the shore's, kept (T665 readers)
SOURCE = "shore"
LABEL = "shore"
#: the rule the record carries (coast/1, T678: the edge-1450 vertex cross-checked against polygons chosen by FWA key)
RULE = "coast/2"
#: every rule whose atlas carries the one tidal record (`built_by_coast_rule`)
RULES = ("coast/1", RULE)
#: owner A (2026-11-11): a fresh run under this many metres at a line's top counts as tidal (the in-line gap bridge too)
STUB_M = 25.0
#: the station spacing of the scan along a line for the water's edge (`water_edge`); = `clean_cut.DIP_M`: a dip out of
#: the water shorter than this between two stations is positional error and is not an edge
STEP_M = 5.0
#: the edge is found to this many metres (bisection), and a point this close to the water is in it
EDGE_TOL_M = 0.01
#: a line END this close to the sea reaches it (FWA ends a coastal line on the coastline)
REACH_M = 50.0
#: how close a line's mouth must be to a water polygon to sit in it
REACH_MOUTH_M = 1.0
#: an edge this close to the line's mouth IS the mouth (`tolerances.SLIVER_M`, the same end rule as `tidal.END_M`: a
#: cut there would make a piece the sliver gate refuses): the line enters tidal water at its end, nothing on it is tidal
END_M = 5.0
#: FWA's edge-1450 vertex within this many metres of the water's edge is the same place: the cut takes FWA's vertex
SAME_PLACE_M = 1.0
#: THE FRASER'S OWN WATERSHED CODE, as the graph carries it (`FidRow.wsc`, trimmed): owner 2026-10-29 "all items with
#: same wsc as the fraser below mission should be dropped as tidal", restated 2026-11-11 for channels and arms
FRASER_WSC = "100-000000"
_EPS = 0.01

#: POLICY (owner 2026-11-11): the coast crossing is the cut where no DFO-written boundary governs. False is the pin
A_LINE_ENTERING_TIDAL_WATER_IS_CUT_AT_ITS_COAST_CROSSING = True
#: POLICY (owner 2026-11-12, RULINGS E21/E22, T685): the cut is the EDGE of the receiving water's polygon (the sea, or
#: the river polygon the mouth sits in), never FWA's edge-1450 vertex where the two differ. False is the pin: the
#: vertex decides wherever FWA drew one
THE_CUT_IS_THE_RECEIVING_WATERS_EDGE = True
#: POLICY (owner A, 2026-11-11): a fresh stub under STUB_M at the top is tidal. False is the pin
A_FRESH_STUB_AT_THE_TOP_IS_TIDAL = True
#: POLICY (owner 2026-11-11): the Fraser's own named channels and arms below its boundary are tidal for their length
THE_FRASERS_CHANNELS_BELOW_ITS_BOUNDARY_ARE_TIDAL = True
#: POLICY (owner 2026-11-11): a cut whose piece below stays freshwater is not made. False is the pin
A_CUT_THAT_MAKES_NOTHING_TIDAL_IS_NOT_MADE = True

# the record's reasons (each row says why; none is silent)
CUT = "coast crossing"
AT_MOUTH = "the coast crossing is the line's mouth: nothing on the line is tidal"
STUB = "the coast crossing is within 25 m of the line's top: the whole line is tidal"
FRASER = "a channel or arm of the Fraser below its tidal boundary: tidal for its length"
FRESH_BELOW = "the piece below the crossing would stay freshwater: not cut"
GOVERNED = "a DFO-written boundary governs this water (rank 1)"
HELD = "the owner's ruling on this line (waters/shore_held.json)"
LAKE_BELOW = "a lake on the line below the crossing (lakes are never cut)"
# the receiving water a row names (`water`)
SEA = "sea"
RIVER = "river polygon"
NO_WATER_AREA = "the tidal water it joins is drawn as a single line there (no water area)"


#: a split row's `authority` in `waters/tidal_split_authority.json`: DFO's words name the boundary (`dfo`), the owner
#: decided it (`owner`, 2026-11-12: "Keep all as my decisions" for the ten splits not traced to DFO), or nothing does
#: (`none`: the coast crossing decides its water)
AUTHORITIES = ("dfo", "owner", "none")
#: the authorities that GOVERN a water (rank 1): a DFO-written boundary, or one the owner made his own decision
GOVERNING = ("dfo", "owner")


def load_authority(path=None) -> dict[str, str]:
    """`{split id: authority}` (`AUTHORITIES`) from `waters/tidal_split_authority.json` (one row per curated tidal split:
    who stands behind it). Missing is refused (AGENTS 37); a row without a known `authority` is refused, never read as
    "none"."""
    import json
    from pathlib import Path
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.waters.tidal_split_authority
    p = Path(path)
    if not p.exists():
        raise SystemExit(f"coast: {p} is missing — the tidal split classification (T678)")
    out = {}
    for r in json.loads(p.read_text(encoding="utf-8"))["splits"]:
        a = r.get("authority")
        if a not in AUTHORITIES:
            raise SystemExit(f"coast: {p}: split {r.get('split')!r} has authority {a!r}, not one of {AUTHORITIES}")
        out[str(r["split"])] = a
    return out


def authority(limit, table: Mapping[str, str]) -> str:
    """A tidal-limit row's authority over the cut (T678 rank 1): `dfo` (a DFO-written boundary: the cut), `owner` (the
    owner's own decision, 2026-11-12: governs exactly like a DFO-written one), `none` (not shown to be DFO's and not the
    owner's: no authority, the coast crossing decides; the split stays a cut for any rule naming it), `unclassified`
    (a split the classification does not list yet: it governs as today and is listed for the owner). `table`:
    `load_authority`. `whole` rows are the owner's whole-water calls (rank 1); `cut` rows are curated points (rank 1,
    as today); `mouth` rows carry nothing (auto-generated, 2026-10-28)."""
    lim = getattr(limit, "limit", None)
    if lim in ("whole", "cut"):
        return "dfo"
    if lim != "split":
        return "none"
    got = table.get(str(getattr(limit, "split", "")))
    return "unclassified" if got is None else got


def edge1450_tops(rows) -> dict[str, float]:
    """`{blk: measure}`: the top of each blue line's LOWEST run of edge-1450 segments, for every line whose lowest
    segment is one (FWA's water crossing: the vertex where the line enters the water it crosses). `rows`: objects
    with `blk`, `edge_type`, `down_m`, `up_m` (`FidRow`). Contiguous by measure (a gap over 0.5 m ends the run)."""
    from pipeline.common.fwa_edge_types import WATER_CROSSING
    by: dict[str, list] = {}
    for r in rows:
        if r.blk:
            by.setdefault(str(r.blk), []).append((float(r.down_m), float(r.up_m), str(r.edge_type)))
    out: dict[str, float] = {}
    for blk, rs in by.items():
        rs.sort()
        if rs[0][2] != WATER_CROSSING:
            continue
        top = rs[0][1]
        for d, u, e in rs[1:]:
            if e != WATER_CROSSING or d > top + 0.5:
                break
            top = max(top, u)
        out[blk] = top
    return out


def water_edge(geometry, mouth_m: float, top_m: float, water, step: float = STEP_M) -> float:
    """THE EDGE OF THE RECEIVING WATER on one blue line (`geometry` mouth first, route measures `mouth_m`..`top_m`): the
    route measure where the line, followed up from its mouth, first leaves `water` (a polygon: the sea around it, or
    the river polygon its mouth sits in). Stations every `step` find the first one outside; the edge between it and
    the last one inside is bisected to `EDGE_TOL_M`. `mouth_m` when there is no water or the mouth is not in it (within
    `REACH_MOUTH_M`); `top_m` when the whole line lies in it."""
    import numpy as np
    import shapely
    L = float(geometry.length)
    if L <= 0 or water is None or water.is_empty:
        return float(mouth_m)
    if float(shapely.distance(water, shapely.get_point(geometry, 0))) > REACH_MOUTH_M:
        return float(mouth_m)
    shapely.prepare(water)

    def inside(ds):
        return shapely.dwithin(water, shapely.line_interpolate_point(geometry, np.asarray(ds, dtype=float)), EDGE_TOL_M)
    scale = float(top_m - mouth_m) / L
    lo, at = 0.0, step
    while lo < L:
        ds = np.minimum(np.arange(at, at + 256 * step, step), L)
        out = np.flatnonzero(~inside(ds))
        if not len(out):
            lo = float(ds[-1])
            at = lo + step
            continue
        i = int(out[0])
        lo, hi = (float(ds[i - 1]) if i else lo), float(ds[i])
        while hi - lo > EDGE_TOL_M:
            mid = (lo + hi) / 2.0
            if bool(inside([mid])[0]):
                lo = mid
            else:
                hi = mid
        return float(mouth_m + lo * scale)
    return float(top_m)


def is_slough(name: str) -> bool:
    return "slough" in str(name or "").lower()


def coast_rule(graph: StreamGraph, *, tidal, e1450: Mapping[str, float], edge: Callable, sea_mouth=frozenset(),
               governed=frozenset(), held: Mapping[str, Mapping] | None = None, existing: Mapping[str, list] | None = None,
               leaves_at=None, fraser_wsc: str = FRASER_WSC):
    """THE COAST RULE (T678, E21 as built by T685). `tidal`: section ids tidal off the map under rank 1 (a DFO-written
    split's piece, the owner's whole waters). `edge(blk, via) -> (measure, water)`: where the line leaves the water it
    enters (`water_edge`), `via` being `SEA` (a line ending in the sea: the sea around it) or `RIVER` (a line entering
    tidal water: the river polygon its mouth sits in), and what that water is (`SEA`, `RIVER`, or `NO_WATER_AREA`,
    the measure then being the mouth). `e1450`: `edge1450_tops`, FWA's own vertex for the same place (recorded; the
    cut when within `SAME_PLACE_M`). `sea_mouth`: the blks whose mouth lies in the sea with no outlet. `governed`: blks
    of a water a rank-1 row governs (never cut here). `held`: `{blk: {keep: cut|whole|fresh, cut_m?}}` the owner's
    rulings on single lines. `existing`: `shore.standing_boundaries` (a crossing within `clean_cut.DIP_M` of one is
    that boundary). `leaves_at`: `shore.edge_leave_measures` (outlets judged where each edge leaves the line).

    Returns `(cut points, whole lines, rows)`. Whole: `{blk, name, why}` (`coast`, `stub`, `fraser`, `held`). Rows:
    one per line that enters tidal water or the sea (or is held), cut (`cut_m`) or why not (`not_cut`)."""
    from pipeline.atlas.splits import clean_cut as _cc
    held = dict(held or {})
    existing = existing or {}
    tidal = frozenset(tidal)
    lines: dict[str, list] = {}
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if n.kind == NodeKind.stream and n.blk:
            lines.setdefault(n.blk, []).append(n)
    for v in lines.values():
        v.sort(key=lambda n: (n.down_m, n.up_m))

    def name_of(ns):
        return next((n.display_name for n in ns if n.display_name), "")

    def mouth_edges(blk) -> list[int]:
        """the edges leaving the line AT ITS MOUTH (where it goes: its receiver), not a distributary higher up"""
        ns = lines[blk]
        out = []
        for n in ns:
            for i in sorted(graph.down_adj.get(n.node_id, ())):
                t = graph.nodes.get(graph.edges[i].to_node)
                if t is not None and t.blk == blk:
                    continue
                m = leaves_at(i) if leaves_at is not None else (n.down_m if n is ns[0] else None)
                if m is not None and m <= ns[0].down_m + STUB_M:
                    out.append(i)
            if leaves_at is None:
                break
        return out

    def outlet_edges(blk, at) -> list[int]:
        """the edges leaving the line below `at` (the piece below a cut there)"""
        ns = lines[blk]
        if leaves_at is None:
            return sorted(graph.down_adj.get(ns[0].node_id, ()))
        out = []
        for n in ns:
            if n.down_m >= at - _EPS:
                break
            for i in sorted(graph.down_adj.get(n.node_id, ())):
                t = graph.nodes.get(graph.edges[i].to_node)
                if t is not None and t.blk == blk:
                    continue
                m = leaves_at(i)
                if m is not None and m >= at - _EPS:
                    continue
                out.append(i)
        return out

    whole: dict[str, dict] = {}
    cand: dict[str, float] = {}          # blk -> cut measure (a whole line: its top)
    ok: set[str] = set()                 # candidates whose piece below goes tidal

    def tidal_like(i) -> bool:
        e = graph.edges[i]
        d = e.to_node
        if d in tidal:
            return True
        n = graph.nodes.get(d)
        if n is None or not n.blk:
            return False
        if n.blk in whole:
            return True
        if n.blk not in ok:
            return False
        m = getattr(e, "at_measure", None)
        if m is None or m != m:
            m = n.down_m
        return max(m, n.down_m) < cand[n.blk] - _EPS

    rows: dict[str, dict] = {}
    # THE OWNER'S RULINGS FIRST: as written, whatever the rule says
    for blk in sorted(held):
        if blk not in lines:
            raise SystemExit(f"coast: held line {blk} ({held[blk].get('name')}) has no piece in the graph")
        h = held[blk]
        ns = lines[blk]
        row = {"blk": blk, "name": name_of(ns), "mouth_m": round(ns[0].down_m, 2), "top_m": round(ns[-1].up_m, 2),
               "held": HELD, "keep": h["keep"]}
        if h["keep"] == "whole":
            whole[blk] = {"blk": blk, "name": row["name"], "why": "held"}
            row["not_cut"] = HELD
        elif h["keep"] == "cut":
            at = float(h["cut_m"])
            if not (ns[0].down_m + _EPS < at < ns[-1].up_m - _EPS):
                raise SystemExit(f"coast: held line {blk} keeps a cut at {at}, outside its line "
                                 f"({ns[0].down_m}..{ns[-1].up_m}): re-read waters/shore_held.json")
            cand[blk] = at
            row["cut_m"] = round(at, 2)
        elif h["keep"] == "fresh":
            row["not_cut"] = HELD
        else:
            raise SystemExit(f"coast: held line {blk}: keep {h['keep']!r} is not cut / whole / fresh")
        rows[blk] = row

    def upstream_blks(blk) -> set[str]:
        """the lines that flow into `blk` (their receiver may have just gone tidal)"""
        got = set()
        for n in lines.get(blk, ()):
            for i in graph.up_adj.get(n.node_id, ()):
                f = graph.nodes.get(graph.edges[i].from_node)
                if f is not None and f.blk and f.blk != blk:
                    got.add(f.blk)
        return got

    # the first lines to look at: every line ending in the sea, and every line flowing into rank-1 tidal water
    work = set(sea_mouth) & set(lines)
    for s in tidal:
        n = graph.nodes.get(s)
        for i in graph.up_adj.get(s, ()):
            f = graph.nodes.get(graph.edges[i].from_node)
            if f is not None and f.blk:
                work.add(f.blk)
    while work:
        changed: set[str] = set()
        for blk in sorted(work):
            if blk in rows or blk in governed or blk not in lines:
                continue
            ns = lines[blk]
            me = mouth_edges(blk)
            if me:
                if not all(tidal_like(i) for i in me):
                    continue
                via = "tidal water"
            elif blk in sea_mouth:
                via = "sea"
            else:
                continue
            mouth, top = ns[0].down_m, ns[-1].up_m
            name = name_of(ns)
            row = {"blk": blk, "name": name, "mouth_m": round(mouth, 2), "top_m": round(top, 2), "joins": via}
            wsc = next((n.wsc for n in ns if n.wsc), "")
            if (THE_FRASERS_CHANNELS_BELOW_ITS_BOUNDARY_ARE_TIDAL and via == "tidal water" and wsc == fraser_wsc
                    and name and not is_slough(name)):
                whole[blk] = {"blk": blk, "name": name, "why": "fraser"}
                rows[blk] = {**row, "not_cut": FRASER}
                changed.add(blk)
                continue
            if not A_LINE_ENTERING_TIDAL_WATER_IS_CUT_AT_ITS_COAST_CROSSING:
                continue
            cw, water = edge(blk, SEA if via == "sea" else RIVER)
            cw = float(cw)
            ce = e1450.get(blk)
            row.update(water=water, edge_m=round(cw, 2), edge1450_m=None if ce is None else round(float(ce), 2))
            if not THE_CUT_IS_THE_RECEIVING_WATERS_EDGE:
                c = float(mouth if ce is None else ce)
            elif ce is not None and abs(float(ce) - cw) <= SAME_PLACE_M:
                c = float(ce)                                         # the same place: FWA's own vertex on that edge
            else:
                c = cw
            c = min(c, top)
            if c - mouth <= END_M:
                rows[blk] = {**row, "not_cut": AT_MOUTH}
                continue
            if top - c < (STUB_M if A_FRESH_STUB_AT_THE_TOP_IS_TIDAL else _EPS):
                whole[blk] = {"blk": blk, "name": name, "why": "stub" if top - c > _EPS else "coast"}
                rows[blk] = {**row, "not_cut": STUB}
                changed.add(blk)
                continue
            if not any(n.down_m - _EPS <= c <= n.up_m + _EPS for n in ns):
                rows[blk] = {**row, "not_cut": LAKE_BELOW}
                continue
            at = _cc.onto_existing(c, existing.get(blk, ()), _cc.DIP_M)
            cand[blk] = at
            rows[blk] = {**row, **({"onto_existing": True} if at != c else {})}
        # the pieces below: a least fixpoint, so two braids feeding each other with no tidal outlet stay fresh
        inner = True
        while inner:
            inner = False
            for blk in sorted(cand):
                if blk in ok:
                    continue
                o = outlet_edges(blk, cand[blk])
                if (not o and blk in sea_mouth) or (o and all(tidal_like(i) for i in o)):
                    ok.add(blk)
                    changed.add(blk)
                    inner = True
        work = set()
        for blk in sorted(changed):
            work |= upstream_blks(blk)

    pts: list[SplitPoint] = []
    for blk in sorted(cand):
        r = rows[blk]
        r["below_cut_tidal"] = blk in ok
        if blk not in ok and A_CUT_THAT_MAKES_NOTHING_TIDAL_IS_NOT_MADE and "held" not in r:
            r["not_cut"] = FRESH_BELOW
            continue
        at = cand[blk]
        r["cut_m"] = round(at, 2)
        pts.append(SplitPoint(split_id=f"{SOURCE}:{blk}:{int(at)}", blk=blk, route_measure=at, fid="", label=LABEL,
                              anchor_type=AnchorType.point, source=SOURCE))
    return pts, [whole[b] for b in sorted(whole)], [rows[b] for b in sorted(rows)]


class CoastGeometry:
    """THE FWA COAST the coast cut reads (`fetch_data.py` fwa_rivers / fwa_land into `fwa_coast.gpkg`): the
    land mass (the assessment watersheds tile the land; what lies outside them is the sea) and the river polygons by
    blue line. A missing file or layer is refused (AGENTS 37): a coast with no land would read every line as sea."""

    def __init__(self, gpkg, bbox=None):
        import pyogrio
        import shapely
        from pathlib import Path
        if not Path(gpkg).exists():
            raise SystemExit(f"coast: {gpkg} is missing — `PYTHONPATH=. python data/fetch_data.py --layers fwa_rivers "
                             "fwa_land` writes it")
        land = pyogrio.read_dataframe(gpkg, layer="land", bbox=bbox, columns=[])
        riv = pyogrio.read_dataframe(gpkg, layer="rivers", bbox=bbox, columns=["BLUE_LINE_KEY"])
        if bbox is None and (land.empty or riv.empty):
            raise SystemExit(f"coast: {gpkg} holds no land or no river polygon")
        self.land = [g for g in land.geometry if g is not None and not g.is_empty]
        self.land_tree = shapely.STRtree(self.land)
        self.rivers = [g for g in riv.geometry]
        self.river_blk = [str(int(k)) if k == k and k is not None else "" for k in riv.BLUE_LINE_KEY]
        self.river_tree = shapely.STRtree(self.rivers)

    def in_sea(self, points, reach: float = REACH_M) -> list[bool]:
        """For each point: does it lie in the sea or within `reach` of it (FWA ends a coastal line ON the coastline,
        the land's edge)? A point properly inside a land polygon is not; one on an edge is checked exactly against
        the sea around it (internal watershed edges are not the coast)."""
        import numpy as np
        import shapely
        from shapely.geometry import box
        pts = np.asarray(points, dtype=object)
        inner = np.zeros(len(pts), dtype=bool)
        if len(pts):
            idx = self.land_tree.query(pts, predicate="within")
            inner[np.unique(idx[0])] = True
        out = []
        for p, inside in zip(pts, inner):
            if inside and not any(shapely.distance(self.land[i].boundary, p) <= reach
                                  for i in self.land_tree.query(p, predicate="intersects")):
                out.append(False)
                continue
            bb = (p.x - 2 * reach, p.y - 2 * reach, p.x + 2 * reach, p.y + 2 * reach)
            B = box(*bb)
            land = [shapely.clip_by_rect(self.land[i], *bb) for i in self.land_tree.query(B)]
            sea = B.difference(shapely.union_all([g for g in land if not g.is_empty])) if land else B
            out.append(not sea.is_empty and shapely.distance(sea, p) <= reach)
        return out

    def sea_around(self, geometry, pad: float = 500.0):
        """The sea around one line: its padded box less the FWA land mass."""
        import shapely
        from shapely.geometry import box
        x0, y0, x1, y1 = geometry.bounds
        bb = (x0 - pad, y0 - pad, x1 + pad, y1 + pad)
        B = box(*bb)
        land = [shapely.clip_by_rect(self.land[i], *bb) for i in self.land_tree.query(B)]
        sea = B.difference(shapely.union_all([g for g in land if not g.is_empty])) if land else B
        return shapely.make_valid(sea)

    def river_at(self, point, reach: float = REACH_MOUTH_M, own: str = ""):
        """THE RIVER'S DRAWN WATER AREA AT A POINT: the FWA river polygon(s) the point sits in (within `reach`: a mouth
        on the seam of two polygons of one river takes both). Chosen by where the point IS, never by the receiving
        river's key (T685: choosing by BLUE_LINE_KEY missed the piece a creek is drawn across the Nass, the Campbell,
        the Nitinat). The one polygon never taken is the creek's OWN drawn water (`own`: its blue line; a double-line
        creek's polygon reaches its mouth where it meets a single-line river, and is the creek, not the water it
        enters). None where the map draws no water area there (a single-line river)."""
        import shapely
        hit = [self.rivers[i] for i in sorted(self.river_tree.query(point, predicate="dwithin", distance=reach))
               if not own or self.river_blk[i] != own]
        return shapely.make_valid(shapely.union_all(hit)) if hit else None

    def edge(self, geometry, mouth_m: float, top_m: float, via: str, own: str = "") -> tuple[float, str]:
        """`coast_rule`'s `edge` for one line (`geometry` mouth first; `own` its blue line): where it leaves the water it
        enters, and what that water is."""
        import shapely
        if via == SEA:
            return water_edge(geometry, mouth_m, top_m, self.sea_around(geometry)), SEA
        water = self.river_at(shapely.get_point(geometry, 0), own=own)
        if water is None:
            return float(mouth_m), NO_WATER_AREA
        return water_edge(geometry, mouth_m, top_m, water), RIVER


# ------------------------------------------------------------------------------------------------ the records
#: the atlas sidecar listing the owner's lines (today's result kept) and the curated splits not yet classified
OWNER_RECORD = "coast_owner.json"
OWNER_ABOUT = ("T678/T685: the owner's rulings on single lines the coast cut kept as written (`waters/shore_held.json`), "
               "the ones a DFO-written boundary governs now, and the curated tidal splits not classified as "
               "DFO-written or the owner's. Written by the atlas build; read by people.")
RECORD_ABOUT = ("T678/T685: every blue line the coast cut looked at — it enters the sea or tidal water, or the owner ruled "
                "on it: cut at the edge of the water it enters (`cut_m`, `below_cut_tidal`), or why not (`not_cut`). "
                "`water` is that water (sea / river polygon / none drawn), `edge_m` its edge on the line, `edge1450_m` "
                "FWA's own vertex there (a check; the cut when within 1 m). Written by the atlas build "
                "(`atlas.splits.coast`); read by people.")
#: THE ONE TIDAL RECORD: every tidal section of this atlas with its owner. The reach, the tiles, the store and the
#: review app read it; none recomputes it (owner 2026-11-11, T678 plan 1)
TIDAL_RECORD = "tidal_sections.json"
TIDAL_ABOUT = ("T678: THE tidal sections of this atlas, one row per section with its owner: `split:<id>` (below a "
               "DFO-written tidal boundary), `water:<item>` (a whole tidal water the owner named), `inside:<blk>` (a "
               "line tidal for its length: coast, stub, Fraser channel, held), `crossing:<section>` (a piece below a "
               "line's coast cut). Lines stay in the graph, marked tidal (owner B). Written by the atlas build only; "
               "read by the reach run (which adds only the rows the book marks tidal), the tiles and the review app.")


def rank1(limits, table: Mapping[str, str]) -> list:
    """The tidal-limit rows that govern a cut (rank 1): a DFO-written split, an owner's split (2026-11-12), an
    unclassified one (as today, listed), every `whole` row, and the `cut` rows (curated points)."""
    return [r for r in limits if getattr(r, "limit", None) in ("split", "cut", "whole") and authority(r, table) != "none"]


def unclassified(limits, table: Mapping[str, str]) -> list[dict]:
    """The split rows that are neither DFO-written nor the owner's, for the owner record: `none` (no authority: the
    coast crossing decides their water) and `unclassified` (govern as today)."""
    return [{"blk": r.blk, "name": r.name, "split": r.split, "authority": authority(r, table)} for r in limits
            if getattr(r, "limit", None) == "split" and authority(r, table) not in GOVERNING]


def water_cover(graph, ids) -> dict:
    """The WATER a set of sections covers, independent of how it is cut: `{blk: [(lo, hi)]}` (merged route-measure
    intervals) for stream pieces, `{"lake:<wbk>": True}` for lakes. Two sets of sections over the same water, cut
    differently (the length cap runs after the coast cut), have equal coverage."""
    by: dict = {}
    for s in ids:
        n = graph.nodes.get(s)
        if n is None:
            by.setdefault("?missing", []).append(s)
        elif n.blk:
            by.setdefault(n.blk, []).append((round(n.down_m, 2), round(n.up_m, 2)))
        else:
            by[s] = True
    out = {}
    for k, v in by.items():
        if v is True or k == "?missing":
            out[k] = v
            continue
        merged: list = []
        for lo, hi in sorted(v):
            if merged and lo <= merged[-1][1] + 0.01:
                merged[-1][1] = max(merged[-1][1], hi)
            else:
                merged.append([lo, hi])
        out[k] = [tuple(x) for x in merged]
    return out


def tidal_owners(graph, registry, limits, id_of, whole: Mapping[str, str], ok_cuts: Mapping[str, float], *,
                 table: Mapping[str, str], strict: bool = True) -> dict[str, str]:
    """`{section: owner}`: THE TIDAL SECTIONS of a finished atlas (the record's rows). Rank 1 on the written registry
    (the reach's resolvers, the same calls as before), then every piece of a line tidal for its length, then every
    piece of a coast-cut line at or below its cut. A section claimed twice keeps the first owner."""
    from pipeline.atlas.reach import outside as _outside
    lim = rank1(limits, table)
    cut_ids = sorted({str(r.split) for r in lim if r.limit == "split"} | {str(id_of[r.blk]) for r in lim if r.limit == "cut"})
    items = sorted({str(r.item) for r in lim if r.limit == "whole"})
    out: dict[str, str] = {}
    for s, o in _outside.tidal_cut_owner(registry, graph, cut_ids, strict=strict).items():
        out.setdefault(s, o)
    for s, o in _outside.tidal_whole_owner(registry, items, strict=strict).items():
        out.setdefault(s, o)
    for nid in sorted(graph.nodes):
        n = graph.nodes[nid]
        if not n.blk:
            continue
        if n.blk in whole:
            out.setdefault(nid, f"inside:{n.blk}")
        elif n.blk in ok_cuts and n.up_m <= ok_cuts[n.blk] + _EPS:
            out.setdefault(nid, f"crossing:{nid}")
    return out


def write_record(out_dir, owners: Mapping[str, str], whole: Mapping[str, str]) -> None:
    import json
    from pathlib import Path
    rows = [{"section": s, "owner": owners[s]} for s in sorted(owners)]
    Path(out_dir, TIDAL_RECORD).write_text(json.dumps({"_about": TIDAL_ABOUT, "rule": RULE, "sections": rows,
                                                       "lines_whole": [{"blk": b, "why": whole[b]} for b in sorted(whole)]},
                                                      indent=0) + "\n", encoding="utf-8")


def read_record(build_dir) -> dict[str, str]:
    """`{section: owner}` from an atlas's tidal record. Missing is refused (AGENTS 37): an atlas without it was built
    before T678 and is not "nothing is tidal"."""
    import json
    from pathlib import Path
    p = Path(build_dir) / TIDAL_RECORD
    if not p.exists():
        raise SystemExit(f"coast: {p} is missing — only the atlas build writes it (T678); rebuild the atlas")
    return {r["section"]: r["owner"] for r in json.loads(p.read_text(encoding="utf-8"))["sections"]}


def built_by_coast_rule(build_dir) -> bool:
    """Was this atlas cut by the coast rule (its shore record names one of `RULES`)? Then it must carry `TIDAL_RECORD`
    (`read_record` refuses one without it). An atlas cut before T678 says so by its record."""
    import json
    from pathlib import Path
    from pipeline.atlas.splits.shore import RECORD
    p = Path(build_dir) / RECORD
    if not p.exists():
        return False                     # an atlas cut before T660 had no shore record: not cut by this rule
    return json.loads(p.read_text(encoding="utf-8")).get("rule") in RULES
