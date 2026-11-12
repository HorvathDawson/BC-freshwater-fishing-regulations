"""T678 THE COAST CUT: where a stream's tidal water ends, decided ONCE, in the atlas's cut sequence (owner 2026-11-11).

Owner, verbatim: "all the tidal logic and stream removal happens in the build of the graph", "the cuts used for tidal
should happen where cuts happen". The order of sources (the owner's edits do not outrank the default):

  1. A DFO WRITTEN BOUNDARY (a sign, a bridge, a written line): a curated `split` row of `waters/tidal_limits.json`
     classified as DFO-written (`authority`), a split the owner made his own decision (authority `owner`, 2026-11-12:
     the ten not traced to DFO, "Keep all as my decisions"), and the owner's `whole` rows. Its water is cut by that
     row, never here.
  2. OTHERWISE THE COAST CROSSING: where the blue line enters the sea, or a river that is itself tidal. FWA draws it:
     the top of the line's lowest run of edge 1450 ("construction line", `fwa_edge_types.WATER_CROSSING`), which is
     FWA's own vertex. It overshoots on some lines (across the Nass), so it is CROSS-CHECKED with the geometric
     crossing (`geometric_crossing`: the top of the line's last run inside the receiving water, the sea being what lies
     outside the FWA land mass, plus the river polygons of the tidal rivers). Within `AGREE_M` the 1450 vertex is the
     cut; otherwise the line is the owner's (`OWNER_*`): it keeps today's result from the curated held file
     (`waters/shore_held.json`), and the build lists it. Nothing is guessed.
  3. The DFO subarea polygon is MAP SHADING ONLY. Polygon hand edits and non-DFO splits carry no authority over a cut.

Also decided here, in the same pass:
  * A FRESH STUB under `STUB_M` at a line's top counts as tidal: the whole line is tidal (owner A).
  * A line wholly in tidal water STAYS IN THE GRAPH, marked tidal (owner B): `whole` rows, never deleted.
  * THE FRASER'S OWN CHANNELS AND ARMS below its tidal boundary are tidal for their length (owner, verbatim: "We still
    want all channels and stuff of Fraser below it's tidal boundary as tidal"): a named line on the Fraser's own
    watershed code (`FRASER_WSC`) whose outlets are all tidal. SLOUGHS ARE NOT CHANNELS ("Sloughs aren't channels.
    Sloughs could be really non tidal?"), and an unnamed Fraser line is unclear: both are the owner's, one by one.
  * A cut is made only where the piece below it goes tidal (every outlet below the cut tidal water), a least fixpoint.

The cut keeps the shore family's id, `shore:<blk>:<int measure>` (`SOURCE`), so every reader of a shore boundary
reads it unchanged. Pure functions of the graph and plain inputs; the build feeds them (`build.cut_shores`).
"""
from __future__ import annotations

from typing import Callable, Mapping

from pipeline.common.models import AnchorType, NodeKind, SplitPoint, StreamGraph

#: the cut family (`SplitPoint.source`, `sectionizer.AUTO_SOURCES`) and id prefix: the shore's, kept (T665 readers)
SOURCE = "shore"
LABEL = "shore"
#: the rule the record carries
RULE = "coast/1"
#: owner A (2026-11-11): a fresh run under this many metres at a line's top counts as tidal (the in-line gap bridge too)
STUB_M = 25.0
#: the two crossings (edge 1450 and geometric) agree when within this many metres
AGREE_M = 50.0
#: the geometric scan's station spacing
STEP_M = 10.0
#: a line END this close to the receiving water reaches it (FWA ends a coastal line on the coastline)
REACH_M = 50.0
#: a coast crossing this close to the line's mouth is the mouth: the line enters tidal water there, nothing on it is
REACH_MOUTH_M = 1.0
#: THE FRASER'S OWN WATERSHED CODE, as the graph carries it (`FidRow.wsc`, trimmed): owner 2026-10-29 "all items with
#: same wsc as the fraser below mission should be dropped as tidal", restated 2026-11-11 for channels and arms
FRASER_WSC = "100-000000"
_EPS = 0.01

#: POLICY (owner 2026-11-11): the coast crossing is the cut where no DFO-written boundary governs. False is the pin
A_LINE_ENTERING_TIDAL_WATER_IS_CUT_AT_ITS_COAST_CROSSING = True
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
HELD = "the owner's: keeps today's result (waters/shore_held.json)"
LAKE_BELOW = "a lake on the line below the crossing (lakes are never cut)"
#: why a line is the owner's (the build lists it, `owner_rows`; the held file keeps today's result)
OWNER_DISAGREE = "the 1450 crossing and the geometric crossing disagree by more than 50 m"
OWNER_NO_GEOMETRY = "the geometric method finds no tidal water at the line's end"
OWNER_SLOUGH = "a slough on the Fraser's code joining the tidal Fraser (sloughs are not channels: the owner's, one by one)"
OWNER_ESTUARY = "an estuary drawn as an FWA river polygon: the polygon reading and the coast reading disagree"
OWNER_UNNAMED_FRASER = "an unnamed line on the Fraser's code joining the tidal Fraser (unclear: the owner's, one by one)"


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


def geometric_crossing(geometry, mouth_m: float, top_m: float, target, step: float = STEP_M):
    """THE GEOMETRIC COAST CROSSING of one blue line (`geometry` mouth first, route measures `mouth_m`..`top_m`):
    the top of the line's last run of stations inside `target` (the receiving tidal water) that reaches its mouth.
    Returns `(measure, end_gap)`; measure None when the mouth is more than `REACH_M` from `target` (the line does not
    reach tidal water: the method fails, never guesses)."""
    import numpy as np
    import shapely
    L = float(geometry.length)
    if L <= 0 or target is None or target.is_empty:
        return None, float("inf")
    gap = float(shapely.distance(target, shapely.get_point(geometry, 0)))
    if gap > REACH_M:
        return None, gap
    span = float(top_m - mouth_m)
    ds = np.arange(0.0, L, step)
    if not len(ds) or ds[-1] < L:
        ds = np.append(ds, L)
    pts = shapely.line_interpolate_point(geometry, ds)
    shapely.prepare(target)
    inside = shapely.contains(target, pts) | (shapely.distance(target, pts) <= 1.0)
    inside[0] = True
    i = 0
    while i + 1 < len(inside) and inside[i + 1]:
        i += 1
    return float(mouth_m + ds[i] * (span / L)), gap


def is_slough(name: str) -> bool:
    return "slough" in str(name or "").lower()


def coast_rule(graph: StreamGraph, *, tidal, e1450: Mapping[str, float], geo: Callable, sea_mouth=frozenset(),
               governed=frozenset(), held: Mapping[str, Mapping] | None = None, existing: Mapping[str, list] | None = None,
               leaves_at=None, fraser_wsc: str = FRASER_WSC):
    """THE T678 RULE. `tidal`: section ids tidal off the map under rank 1 (a DFO-written split's piece, the owner's
    whole waters). `e1450`: `edge1450_tops`. `geo(blk) -> (measure | None, gap)`: the geometric crossing. `sea_mouth`:
    the blks whose mouth lies in the sea with no outlet. `governed`: blks of a water a rank-1 row governs (never cut
    here). `held`: `{blk: {keep: cut|whole|fresh, cut_m?}}` the owner's lines (today's result, kept). `existing`:
    `shore.standing_boundaries` (a crossing within `clean_cut.DIP_M` of one is that boundary). `leaves_at`:
    `shore.edge_leave_measures` (outlets judged where each edge leaves the line).

    Returns `(cut points, whole lines, rows, owner)`. Whole: `{blk, name, why}` (`coast`, `stub`, `fraser`, `held`).
    Rows: one per line that enters tidal water (or is held), cut (`cut_m`) or why not (`not_cut`). Owner: one row per
    line the method cannot decide and the held file does not hold (the build refuses those: `missing_held`)."""
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
    owner: dict[str, dict] = {}
    # HELD LINES FIRST: today's result, whatever the method says
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
    seen_owner: set[str] = set()
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
            owner.pop(blk, None)
            mouth, top = ns[0].down_m, ns[-1].up_m
            name = name_of(ns)
            row = {"blk": blk, "name": name, "mouth_m": round(mouth, 2), "top_m": round(top, 2), "joins": via}
            wsc = next((n.wsc for n in ns if n.wsc), "")
            if THE_FRASERS_CHANNELS_BELOW_ITS_BOUNDARY_ARE_TIDAL and via == "tidal water" and wsc == fraser_wsc:
                if is_slough(name):
                    owner[blk] = {**row, "why": OWNER_SLOUGH}
                elif not name:
                    owner[blk] = {**row, "why": OWNER_UNNAMED_FRASER}
                else:
                    whole[blk] = {"blk": blk, "name": name, "why": "fraser"}
                    rows[blk] = {**row, "not_cut": FRASER}
                    changed.add(blk)
                continue
            if not A_LINE_ENTERING_TIDAL_WATER_IS_CUT_AT_ITS_COAST_CROSSING:
                continue
            ce = float(e1450.get(blk, mouth))
            cg, gap = geo(blk)
            row.update(edge1450_m=round(ce, 2), geometric_m=None if cg is None else round(cg, 2),
                       end_gap_m=round(gap, 1) if gap == gap and gap != float("inf") else None)
            if cg is None:
                owner[blk] = {**row, "why": OWNER_NO_GEOMETRY}
                continue
            if abs(ce - cg) > AGREE_M:
                owner[blk] = {**row, "why": OWNER_DISAGREE}
                continue
            c = min(ce, top)
            if c - mouth <= REACH_MOUTH_M:
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
        work |= {b for b in owner if b not in seen_owner}            # an owner line may resolve once its receiver does
        seen_owner |= set(owner)

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
    return pts, [whole[b] for b in sorted(whole)], [rows[b] for b in sorted(rows)], [owner[b] for b in sorted(owner)]


def missing_held(owner_rows, held) -> list[dict]:
    """The owner's lines the held file does not hold: the build refuses them (today's result must be written down
    before the build can keep it: `python -m pipeline.atlas.splits.coast_held`)."""
    return [r for r in owner_rows if r["blk"] not in held]


class CoastGeometry:
    """THE FWA COAST the geometric crossing reads (`fetch_data.py` fwa_rivers / fwa_land into `fwa_coast.gpkg`): the
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

    def touches_river(self, geometry) -> bool:
        """Does the line meet an FWA river polygon (an estuary or river drawn double-line)?"""
        return bool(len(self.river_tree.query(geometry, predicate="intersects")))

    def target(self, geometry, tidal_blks, own_blk: str = "", pad: float = 500.0):
        """The receiving tidal water around one line: the sea (the box less the land) and the river polygons of the
        tidal rivers (`tidal_blks`), never the line's own river polygon."""
        import shapely
        from shapely.geometry import box
        x0, y0, x1, y1 = geometry.bounds
        bb = (x0 - pad, y0 - pad, x1 + pad, y1 + pad)
        B = box(*bb)
        land = [shapely.clip_by_rect(self.land[i], *bb) for i in self.land_tree.query(B)]
        sea = B.difference(shapely.union_all([g for g in land if not g.is_empty])) if land else B
        recv = [shapely.clip_by_rect(self.rivers[i], *bb) for i in self.river_tree.query(B)
                if self.river_blk[i] in tidal_blks and self.river_blk[i] != own_blk]
        return shapely.make_valid(shapely.union_all([sea] + [g for g in recv if not g.is_empty]))


# ------------------------------------------------------------------------------------------------ the records
#: the atlas sidecar listing the owner's lines (today's result kept) and the curated splits not yet classified
OWNER_RECORD = "coast_owner.json"
OWNER_ABOUT = ("T678: the lines the coast cut leaves to the owner, each kept as `waters/shore_held.json` says (today's "
               "result until the owner decides), with where it is and what each method says; and the curated tidal "
               "splits not yet classified as DFO-written or not (they govern as today until they are). Written by the "
               "atlas build; read by people and the held-rivers page.")
RECORD_ABOUT = ("T678: every blue line the coast cut looked at — it enters the sea or tidal water, or the owner holds it: "
                "cut at its coast crossing (`cut_m`, `below_cut_tidal`), or why not (`not_cut`); `edge1450_m` / "
                "`geometric_m` are the two crossings. Written by the atlas build (`atlas.splits.coast`); read by people.")
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


def coverage(graph, ids) -> dict:
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
    """Was this atlas cut by the coast rule (its shore record names `RULE`)? Then it must carry `TIDAL_RECORD`
    (`read_record` refuses one without it). An atlas cut before T678 says so by its record."""
    import json
    from pathlib import Path
    from pipeline.atlas.splits.shore import RECORD
    p = Path(build_dir) / RECORD
    if not p.exists():
        return False                     # an atlas cut before T660 had no shore record: not cut by this rule
    return json.loads(p.read_text(encoding="utf-8")).get("rule") == RULE
