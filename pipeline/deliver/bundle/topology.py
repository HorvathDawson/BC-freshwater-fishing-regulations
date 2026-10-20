"""PART TOPOLOGY AND CAMERA HINTS (P09, MINIMAL-PACKAGING §10): everything the regs map navigation needs, WITHOUT geometry.

The app draws lines and polygons from the tiles (feature id = sid). The bundle carries only what the tiles cannot say:

  part_geo    (ord, part_ix): `rank` (mouth first within the water), the bbox (lon/lat), and the midpoint: `mid_sid`
              and its point, where "Conditions here" opens (owner Q7).
  part_mark   (ord, mark_ix): every place two parts of a water meet (within TOUCH_M), with the split's name there
              (or NULL: the app shows no text), the downstream part and the upstream parts. Side channels and lake
              splits make the parts a graph, not a list.
  water_exit  (ord): the water this one flows into (`item_down`), and the receiving water's part it enters.

THE RULES (stated, pinned in test_part_geo.py), moved here from `pipeline/tools/water_map_export.py`, which now imports
`order_parts` and `split_label` from this module (one implementation):
  rank      by the part's km from the mouth (the highest `km_from` of its main-stem runs, else of its side channels;
            `answers.display.part_km`), mouth first; parts with no km go last, in part order.
  midpoint  a stream part: the point at half its main-stem length, walked mouth first by route measure
            (`section_span`), or by summed section length where a section has no span; `mid_sid` is the section
            holding it, and the point is that section's own midpoint (within one section of the exact point). A lake
            part: `rep_sid` and the polygon's representative point.
  marks     two parts closer than TOUCH_M; the point is the middle of their nearest points; the down part is the lower
            rank. Pairs meeting at one point with one down part are one mark with several up parts.
  exit      `item_down` (from the graph). Where a water drains into several, the receiver whose part lies nearest
            this water's mouth part (rank 0); `down_part` is that part.

Geometry is the atlas's (`geometries.pkl`, EPSG:3005 metres), kept for the NAMED sections only, from the moment it is
loaded until these tables are written. Never key on `ord` across bundles (it is the insertion order).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple

TOUCH_M = 60.0

Geometry = object      # shapely geometry, EPSG:3005


def keep_named(db: sqlite3.Connection, build_dir, geoms: dict) -> Dict[int, Geometry]:
    """{sid: geometry} for every section a named water owns (`item_section`), keyed by the handle index."""
    from pipeline.common.section_handles import id_at, read as read_handles
    ids, _ = read_handles(build_dir)
    want = sorted({s for (s,) in db.execute("SELECT sid FROM item_section")})
    handle = {sid: id_at(ids, sid) for sid in want}          # handles are 1-based: id_at, never ids[sid]
    out = {sid: geoms[handle[sid]] for sid in want if handle[sid] in geoms}
    # A LAKE'S outline is not in geometries.pkl (its line there is the route through it): the atlas keeps the polygons
    # in waterbody_polys.pkl, keyed "lake:<wbk>" like the section handle.
    missing = [sid for sid in want if sid not in out]
    polys_p = Path(build_dir) / "waterbody_polys.pkl"
    if missing and polys_p.exists():
        import pickle
        with polys_p.open("rb") as fh:
            polys = pickle.load(fh)
        out.update({sid: polys[handle[sid]] for sid in missing if handle[sid] in polys})
    return out


# ---- the pure rules (shared with water_map_export) ----------------------------------------------------------------

def part_km(runs: Sequence[dict]) -> Optional[float]:
    """The highest `km_from` of the main-stem runs, else of the side channels, else None."""
    def km(rs):
        return [r["km_from"] for r in rs if r.get("km_from") is not None]
    m = km([r for r in runs if not r.get("branch")])
    b = km([r for r in runs if r.get("branch")])
    return max(m) if m else max(b) if b else None


def order_parts(parts: Sequence[dict]) -> List[dict]:
    """Within one water: by km, mouth up; parts with no km (side channels, lake parts) last, in part order (`v`)."""
    with_km = sorted((p for p in parts if p.get("km") is not None), key=lambda p: (p["km"], p["v"]))
    return with_km + [p for p in parts if p.get("km") is None]


def split_label(down: dict, up: dict, splits: Dict[str, dict]) -> Optional[str]:
    """The split where `up` (upstream) meets `down`: the downstream part's run start, else the upstream part's run end,
    when it has a name. A branch (`km` None: a side channel, a lake part) is named only by its own runs' ends: the main
    stem's next split is not where it leaves."""
    ends = ((down, "from"), (up, "to")) if up.get("km") is not None else ((up, "to"), (up, "from"))
    for p, end in ends:
        for r in p.get("runs") or []:
            s = splits.get(r.get(end) or "")
            if s and s.get("name"):
                return s["name"]
    return None


def stem_midpoint(stem: Sequence[Tuple[int, Optional[float]]]) -> int:
    """The sid holding half the main stem's length. `stem` is (sid, length) mouth first."""
    total = sum(n or 0.0 for _, n in stem)
    acc = 0.0
    for sid, n in stem:
        acc += n or 0.0
        if acc >= total / 2:
            return sid
    return stem[-1][0]


def point_on(g):
    """A point ON a section: a line's midpoint (the longest piece of a multi-line), else a polygon's representative
    point (a river drawn as a polygon on its stem)."""
    t = g.geom_type
    if t == "MultiLineString":
        g, t = max(g.geoms, key=lambda x: x.length), "LineString"
    if t in ("LineString", "LinearRing"):
        return g.interpolate(0.5, normalized=True)
    return g.representative_point()


def merge_marks(raw: Sequence[dict]) -> List[dict]:
    """One mark per (point, down part), with every up part meeting there."""
    by: Dict[tuple, dict] = {}
    for m in raw:
        k = (round(m["lon"], 6), round(m["lat"], 6), m["down"])
        if k in by:
            by[k]["up"] = sorted(set(by[k]["up"]) | {m["up"]})
            by[k]["label"] = by[k]["label"] or m["label"]
        else:
            by[k] = {"lon": m["lon"], "lat": m["lat"], "down": m["down"], "up": [m["up"]], "label": m["label"]}
    return sorted(by.values(), key=lambda m: (m["down"], m["up"], m["lon"], m["lat"]))


# ---- the tables -------------------------------------------------------------------------------------------------

def write(db: sqlite3.Connection, geo: Dict[int, Geometry], cov) -> None:
    """Write `part_geo`, `part_mark`, `water_exit` from the bundle's own parts, spans, splits and `item_down`."""
    from pyproj import Transformer
    from shapely.ops import nearest_points, unary_union

    from pipeline.deliver.bundle import spans as SP
    to_ll = Transformer.from_crs(3005, 4326, always_xy=True)

    kind = dict(db.execute("SELECT ord, kind FROM item"))
    name = dict(db.execute("SELECT ord, name FROM item"))
    rep = {(o, p): r for o, p, r in db.execute("SELECT ord, part_ix, rep_sid FROM part")}
    sids: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    for o, s, p in db.execute("SELECT ord, sid, part_ix FROM part_section ORDER BY ord, part_ix, sid"):
        sids[(o, p)].append(s)
    span = {s: (a, b, lo, hi, off, shape) for s, a, b, lo, hi, off, shape in db.execute(
        "SELECT s.sid, s.lo_m, s.hi_m, a.token, b.token, s.off_stem, s.shape FROM section_span s "
        "JOIN part_section ps ON ps.sid = s.sid JOIN span_end a ON a.eid = s.lo JOIN span_end b ON b.eid = s.hi")}
    touch: Dict[int, set] = defaultdict(set)
    for a, b in db.execute("SELECT a, b FROM section_touch"):
        touch[a].add(b)
        touch[b].add(a)
    splits = {sid: {"name": n} for sid, n in db.execute("SELECT split_id, name FROM split")}

    def is_polygon_part(o: int, ss: List[int]) -> bool:
        if kind.get(o) != "stream":
            return True
        rows = [span.get(s) for s in ss]
        return bool(rows) and all(r is not None and r[5] == 1 and r[0] is None for r in rows)

    by_ord: Dict[int, List[dict]] = defaultdict(list)
    for (o, p), ss in sorted(sids.items()):
        poly = is_polygon_part(o, ss)
        runs = [] if poly or any(s not in span for s in ss) else SP.compose_runs(span, touch, ss)
        by_ord[o].append({"v": p, "sids": ss, "km": None if poly else part_km(runs), "runs": runs, "poly": poly})

    part_rows, mark_rows, shapes = [], [], {}
    no_measure = 0
    for o, parts in sorted(by_ord.items()):
        ranked = order_parts(parts)
        for rank, p in enumerate(ranked):
            gs = [geo[s] for s in p["sids"] if s in geo]
            if not gs:
                raise SystemExit(f"part_geo: water ord {o} part {p['v']} has no geometry in this atlas")
            g = unary_union(gs)
            shapes[(o, p["v"])] = g
            if p["poly"]:
                mid = rep[(o, p["v"])]
                pt = (geo.get(mid) or g).representative_point()
            else:
                # only sections the atlas drew can hold the point (a few have no shape in geometries.pkl)
                stem = sorted((s for s in p["sids"] if s in span and s in geo and not span[s][4]
                               and span[s][0] is not None), key=lambda s: (span[s][0], s))
                if stem:
                    mid = stem_midpoint([(s, span[s][1] - span[s][0]) for s in stem])
                else:                                   # no measures: summed section length, part order
                    no_measure += 1
                    mid = stem_midpoint([(s, geo[s].length) for s in p["sids"] if s in geo])
                pt = point_on(geo[mid])
            lon, lat = to_ll.transform(pt.x, pt.y)
            w, s_, e, n = to_ll.transform_bounds(*g.bounds)
            part_rows.append((o, p["v"], rank, mid, round(lon, 6), round(lat, 6),
                              round(w, 6), round(s_, 6), round(e, 6), round(n, 6)))
        raw = []
        for i in range(len(ranked)):
            for j in range(i + 1, len(ranked)):
                down, up = ranked[i], ranked[j]
                a, b = shapes[(o, down["v"])], shapes[(o, up["v"])]
                if a.distance(b) > TOUCH_M:
                    continue
                pa, pb = nearest_points(a, b)
                lon, lat = to_ll.transform((pa.x + pb.x) / 2, (pa.y + pb.y) / 2)
                raw.append({"lon": round(lon, 6), "lat": round(lat, 6), "down": down["v"], "up": up["v"],
                            "label": split_label(down, up, splits)})
        for k, m in enumerate(merge_marks(raw)):
            mark_rows.append((o, k, m["lon"], m["lat"], m["label"], m["down"], json.dumps(m["up"])))

    mouth = _mouths(part_rows)
    shapes_of: Dict[int, List[Tuple[int, Geometry]]] = defaultdict(list)
    for (o, p), g in sorted(shapes.items(), key=lambda kv: kv[0]):
        shapes_of[o].append((p, g))
    exits = []
    downs: Dict[int, List[int]] = defaultdict(list)
    for a, b in db.execute("SELECT ord, down_ord FROM item_down ORDER BY ord, down_ord"):
        downs[a].append(b)
    for o, cands in sorted(downs.items()):
        if o not in mouth:
            continue
        here = shapes[(o, mouth[o])]
        best = None
        for d in cands:
            for p, g in shapes_of.get(d, ()):
                cand = (here.distance(g), d, p)
                if best is None or cand < best:
                    best = cand
        exits.append((o, cands[0], None, name.get(cands[0])) if best is None
                     else (o, best[1], best[2], name.get(best[1])))

    db.executemany("INSERT INTO part_geo (ord, part_ix, rank, mid_sid, mid_lon, mid_lat, w, s, e, n) "
                   "VALUES (?,?,?,?,?,?,?,?,?,?)", part_rows)
    db.executemany("INSERT INTO part_mark (ord, mark_ix, lon, lat, label, down_part, up_parts) VALUES (?,?,?,?,?,?,?)",
                   mark_rows)
    db.executemany("INSERT INTO water_exit (ord, down_ord, down_part, label) VALUES (?,?,?,?)", exits)
    cov.filled("part_geo", len(part_rows))
    cov.filled("part_mark", len(mark_rows))
    cov.filled("water_exit", len(exits))
    print(f"  part topology: {len(part_rows):,} parts, {len(mark_rows):,} marks, {len(exits):,} exits "
          f"({no_measure:,} stream parts placed by section length: no route measure)")


def _mouths(part_rows: Sequence[tuple]) -> Dict[int, int]:
    """{ord: the part_ix of rank 0}."""
    return {o: p for o, p, rank, *_ in part_rows if rank == 0}
