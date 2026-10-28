"""PART TOPOLOGY AND CAMERA HINTS (P09, MINIMAL-PACKAGING §10): everything the regs map navigation needs, WITHOUT geometry.

The app draws lines and polygons from the tiles (feature id = sid). The bundle carries only what the tiles cannot say:

  part_geo    (ord, part_ix): `rank` (mouth first within the water), the bbox (lon/lat), and the midpoint: `mid_sid`
              and its point, where "Conditions here" opens (owner Q7).
  part_mark   (ord, mark_ix): every PLACE two parts of a water meet (within TOUCH_M; T204: one mark per meeting
              place, not per pair of parts), with the split's name there (or NULL: the app shows no text), the
              downstream part and the upstream parts, and — where a part has several stretches — which stretch of each
              (`down_stretch`, `up_stretches`, NULL when every one is stretch 0). Side channels and lake splits make
              the parts a graph, not a list.
  part_stretch (ord, part_ix, stretch_ix) (T204): a part's STRETCHES — its runs that no other part of the same water
              separates (a lake or an unnamed piece between two runs does not) — each with its rank among the water's
              stretches (mouth first), midpoint, bbox and section ids; written for every part of a water that has a
              part of two or more stretches (Kanaka Creek's part below the 112 Avenue bridge AND above Cliff Park
              Falls), none for any other water: there a part is its one stretch.
  water_exit  (ord): the water this one flows into (`item_down`), and the receiving water's part it enters.

THE RULES (stated, pinned in test_part_geo.py), moved here from `pipeline/tools/audit/water_map_export.py`, which now imports
`order_parts` and `split_label` from this module (one implementation):
  rank      by the part's km from the mouth (the highest `km_from` of its main-stem runs, else of its side channels;
            `answers.display.part_km`), mouth first; parts with no km go last, in part order.
  midpoint  a stream part: the point at half its main-stem length, walked mouth first by route measure
            (`section_span`), or by summed section length where a section has no span; `mid_sid` is the section
            holding it, and the point is that section's own midpoint (within one section of the exact point). A lake
            part: `rep_sid` and the polygon's representative point.
  stretch   a part's main-stem runs, mouth up, merged unless another part of the water has a main-stem section in the
            gap between them; a side-channel run joins the stretch it re-enters within (else the nearest); a part with
            no main-stem run (side channels only, a lake) is one stretch (`stretches`).
  marks     two STRETCHES of different parts closer than TOUCH_M; the point is the middle of their nearest points; the
            down stretch is the lower rank. Stretches meeting at one point with one down stretch are one mark with
            several up stretches. A water whose parts are each one stretch gets exactly the per-pair marks it had.
  exit      `item_down` (from the graph), less every receiver that itself flows into this water (`exit_receivers`,
            T59: the Fraser "flowed into" Annacis Channel, its own side channel; a creek into the headwater lake it
            runs out of). Where a water drains into several, the receiver whose part lies nearest this water's mouth
            part (rank 0); `down_part` is that part. None left (the Fraser, a coastal creek): no exit row.

Geometry is the atlas's (`geometries.pkl`, EPSG:3005 metres), kept for the NAMED sections only, from the moment it is
loaded until these tables are written. Never key on `ord` across bundles (it is the insertion order).
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple

from pipeline.common.tolerances import TOUCH_M     # two parts closer than this meet

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


def stretches(runs: Sequence[dict], span: Dict[int, tuple], other: Sequence[Tuple[float, float]]) -> List[dict]:
    """T204: a part's STRETCHES, mouth first: [{"lo", "hi" (stem metres, None for no main stem), "sids"}]. `runs` are
    `spans.compose_runs`'s; `span[sid]` is (lo_m, hi_m, lo, hi, off_stem, shape); `other` the (lo_m, hi_m) of every
    main-stem section of the water's OTHER parts. Two main-stem runs of the part are one stretch unless one of
    `other` lies in the gap between them (what separates Kanaka Creek's mouth run from its run above Cliff Park Falls
    is the part between them; a lake on the creek's course, or an unnamed piece, is not another part). A side-channel
    run joins the stretch whose stem range holds the measure it re-enters at, else the nearest, else the first."""
    stem = [r for r in runs if not r.get("branch")]
    side = [r for r in runs if r.get("branch")]
    if not stem:
        sids = sorted(s for r in runs for s in r["sids"])
        return [{"lo": None, "hi": None, "sids": sids}] if sids else []

    def ext(r):
        ms = [s for s in r["sids"] if not span[s][4] and span[s][0] is not None]
        return min(span[s][0] for s in ms), max(span[s][1] for s in ms)
    out: List[dict] = []
    for lo, hi, sids in sorted((ext(r) + (r["sids"],) for r in stem), key=lambda c: (c[0], c[1], c[2][0])):
        if out and not any(a < lo and b > out[-1]["hi"] for a, b in other):
            out[-1]["hi"] = max(out[-1]["hi"], hi)
            out[-1]["sids"] += sids
        else:
            out.append({"lo": lo, "hi": hi, "sids": list(sids)})
    for r in side:
        m = None if r.get("km_from") is None else r["km_from"] * 1000

        def off(i: int) -> tuple:
            s = out[i]
            return (0.0 if s["lo"] <= m <= s["hi"] else min(abs(m - s["lo"]), abs(m - s["hi"])), i)
        out[0 if m is None else min(range(len(out)), key=off)]["sids"] += r["sids"]
    for s in out:
        s["sids"] = sorted(set(s["sids"]))
    return out


def cut_between(x: int, y: int, span: Dict[int, tuple], splits: Dict[str, dict]) -> Optional[str]:
    """T228: the NAMED SPLIT two touching sections meet at — the span end token they share, when it is a split with a
    name — else None."""
    a, b = span.get(x), span.get(y)
    if not a or not b:
        return None
    for t in sorted({a[2], a[3]} & {b[2], b[3]} - {None}):
        hit = splits.get(t)
        if hit and hit.get("name"):
            return t
    return None


def by_chain(st: Sequence[dict], chain: Dict[int, int], touch: Dict[int, set], span: Dict[int, tuple],
             splits: Optional[Dict[str, dict]] = None, gaps: Optional[List[Tuple[int, int]]] = None) -> List[dict]:
    """T228 (owner 2026-10-28: "sections are bounded by extents (named splits); salmon DFO places' extents are splits
    too; adjacent sections merge only when both standard and salmon rules match; every boundary is a named split"):
    each stretch of `st` cut where the DFO salmon chain changes (`salmon_section.chain`, 0 for a section in none) AND a
    named split stands there (`cut_between`). The connected runs (`section_touch`) of one chain are the candidate
    pieces; two that touch only where no named split stands are ONE piece — a salmon difference at a point with no split
    is a DATA GAP (a DFO place's edge not placed as a split), never cut or named: each such touching pair is appended to
    `gaps` (down-or-up order as met) for the build's diagnostic and the review's DFO queue. Each piece mouth first, its
    `hi` the highest main-stem measure it holds (None: no main stem), its `chain` the one chain or a tuple of them.
    Where two pieces read the same salmon TODAY the app joins them again (`@app/core` `mergeNav`). A stretch left whole
    is returned as it was."""
    splits = splits or {}
    out: List[dict] = []
    for s in st:
        if len({chain.get(x, 0) for x in s["sids"]}) <= 1:
            out.append(s)
            continue
        here = set(s["sids"])
        left, comps = set(s["sids"]), []
        piece_of: Dict[int, int] = {}
        for x in s["sids"]:
            if x not in left:
                continue
            c, comp, todo = chain.get(x, 0), [], [x]
            left.discard(x)
            while todo:
                y = todo.pop()
                comp.append(y)
                for z in touch.get(y, ()):
                    if z in left and chain.get(z, 0) == c:
                        left.discard(z)
                        todo.append(z)
            for y in comp:
                piece_of[y] = len(comps)
            comps.append(comp)
        # two pieces join unless a named split stands where they touch
        root = list(range(len(comps)))

        def find(i: int) -> int:
            while root[i] != i:
                root[i] = root[root[i]]
                i = root[i]
            return i
        pairs: Dict[Tuple[int, int], List[Tuple[int, int]]] = defaultdict(list)
        for x in sorted(here):
            for y in sorted(touch.get(x, ())):
                if y in here and piece_of[x] < piece_of[y]:
                    pairs[(piece_of[x], piece_of[y])].append((x, y))
        for (i, j), xs in sorted(pairs.items()):
            if not any(cut_between(x, y, span, splits) for x, y in xs):
                if gaps is not None:
                    gaps += xs
                root[find(j)] = find(i)
        joined: Dict[int, List[int]] = defaultdict(list)
        for k, comp in enumerate(comps):
            joined[find(k)] += comp
        pieces = []
        for comp in joined.values():
            cs = sorted({chain.get(y, 0) for y in comp})
            stem = [span[y] for y in comp if y in span and not span[y][4] and span[y][0] is not None]
            pieces.append({"lo": min(r[0] for r in stem) if stem else None,
                           "hi": max(r[1] for r in stem) if stem else None, "sids": sorted(comp),
                           "chain": cs[0] if len(cs) == 1 else tuple(cs)})
        pieces.sort(key=lambda q: (q["hi"] is None, q["lo"] if q["lo"] is not None else 0, q["sids"][0]))
        # a piece with no main stem (a side channel the graph does not join: no `section_touch`) goes with the stretch's
        # first main-stem piece of its chain — as `stretches` placed it by measure; with none of its chain it stays
        stem_of = {}
        for q in pieces:
            if q["hi"] is not None:
                stem_of.setdefault(q["chain"], q)
        kept = []
        for q in pieces:
            home = stem_of.get(q["chain"]) if q["hi"] is None else None
            if home is not None:
                home["sids"] = sorted(home["sids"] + q["sids"])
            else:
                kept.append(q)
        out += kept if len(kept) > 1 else [{**s, "sids": sorted(s["sids"])}]
    return out


def chain_label(dn: dict, up: dict, span: Dict[int, tuple], splits: Dict[str, dict],
                touch: Optional[Dict[int, set]] = None) -> Optional[dict]:
    """T228: the NAMED SPLIT where two pieces of ONE part meet (`by_chain` only cuts at one): the split two of their
    touching sections share, else the upper piece's lowest main-stem end when it is a named split; else None — no
    name is ever invented (owner 2026-10-28: every boundary is a named split)."""
    a = set(dn["sids"])
    for y in sorted(up["sids"]):
        for x in sorted((touch or {}).get(y, ())):
            if x in a:
                t = cut_between(x, y, span, splits)
                if t:
                    return splits[t]
    stem = sorted((span[s][0], s) for s in up["sids"] if s in span and not span[s][4] and span[s][0] is not None)
    if stem:
        hit = splits.get(span[stem[0][1]][2] or "")
        if hit and hit.get("name"):
            return hit
    return None


def split_at(down: dict, up: dict, splits: Dict[str, dict]) -> Optional[dict]:
    """The split where `up` (upstream) meets `down`: the downstream part's run start, else the upstream part's run end,
    when it has a name. A branch (`km` None: a side channel, a lake part) is named only by its own runs' ends: the main
    stem's next split is not where it leaves. Its `splits` row ({"name", "confidence"?}), or None."""
    ends = ((down, "from"), (up, "to")) if up.get("km") is not None else ((up, "to"), (up, "from"))
    for p, end in ends:
        for r in p.get("runs") or []:
            s = splits.get(r.get(end) or "")
            if s and s.get("name"):
                return s
    return None


def split_label(down: dict, up: dict, splits: Dict[str, dict]) -> Optional[str]:
    """The NAME of the split where `up` meets `down` (`split_at`), or None."""
    s = split_at(down, up, splits)
    return s["name"] if s else None


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
    """One mark per (point, down part, down stretch), with every up part meeting there. A raw mark's `up` is a part, its
    `up_k` / `down_k` (T204) the stretches (absent: 0); the merged `up` lists the parts and `up_k` their stretches, in
    the same order, sorted by (part, stretch)."""
    by: Dict[tuple, dict] = {}
    for m in raw:
        k = (round(m["lon"], 6), round(m["lat"], 6), m["down"], m.get("down_k", 0))
        u = (m["up"], m.get("up_k", 0))
        if k in by:
            by[k]["ups"] = sorted(set(by[k]["ups"]) | {u})
            if not by[k]["label"] and m["label"]:
                by[k]["label"] = m["label"]
                if m.get("confidence"):                  # T207: the named split's, with its name
                    by[k]["confidence"] = m["confidence"]
        else:
            by[k] = {"lon": m["lon"], "lat": m["lat"], "down": m["down"], "down_k": m.get("down_k", 0), "ups": [u],
                     "label": m["label"], **({"confidence": m["confidence"]} if m.get("confidence") else {})}
    out = []
    for m in sorted(by.values(), key=lambda m: (m["down"], m["down_k"], m["ups"], m["lon"], m["lat"])):
        ups = m.pop("ups")
        m["up"], m["up_k"] = [u for u, _ in ups], [k for _, k in ups]
        out.append(m)
    return out


#: a way down through unnamed pieces longer than this is the sea or the edge of the atlas (as `nearer.MAX_STEPS`)
MAX_STEPS = 200_000


def mouth_downs(graph, ids: Sequence[str], secs: Dict[int, set]) -> Dict[int, set]:
    """WHERE A WATER'S MOUTH FLOWS (T59: the Fraser's exit named Annacis Channel). {ord: the named waters first met
    going down from the water's MOUTH} for every water with a main line. The mouth is the lowest piece of the water's
    main line (`lines.line_ends`: the FWA blue line carrying most of its length, the stem the spans measure along).
    Its edges out that LEAVE the water are followed (each down the mainstem edge first, `nearer._next`, through unnamed
    pieces) to the first piece another water owns; only when every way out of the mouth comes back into the water's own pieces (a braid, a lower
    line of its own, an unnamed lake on its course) are those pieces' edges tried next, nearest first. An empty set:
    the mouth reaches the sea, the atlas's edge, a loop or only itself. A water with no main line (a lake) is absent.
    It is used only as evidence that a water flows INTO another (`exit_receivers`), never to pick an exit. `secs` is
    {ord: section handles} (`ids[h-1]` names handle `h`)."""
    from pipeline.deliver.bundle.continuation import line_of
    from pipeline.deliver.bundle.lines import handle_lengths, line_ends
    from pipeline.deliver.bundle.nearer import _next

    def line(h: int):
        return line_of(ids[h - 1]) if 0 < h <= len(ids) else None

    owner: Dict[str, set] = defaultdict(set)
    for o, ss in secs.items():
        for h in ss:
            if 0 < h <= len(ids):
                owner[ids[h - 1]].add(o)

    def below(o: int, v: Optional[str]) -> Tuple[set, Optional[str]]:
        """(the named waters first met down from `v`, or the piece of `o` itself the way comes back into)."""
        seen: set = set()
        while v is not None and v not in seen and len(seen) < MAX_STEPS:
            if v in owner:
                return (set(), v) if o in owner[v] else (set(owner[v]), None)
            seen.add(v)
            v = _next(graph, v)
        return set(), None

    length = handle_lengths(graph.nodes, ids, {h for ss in secs.values() for h in ss})
    out: Dict[int, set] = {}
    for o, ss in sorted(secs.items()):
        got = line_ends(ss, line, length)
        if got is None:
            continue
        frontier = [ids[got[1] - 1]]
        done, got, left = set(frontier), set(), False
        while frontier and not left:
            nxt = []
            for v in frontier:
                for ix in sorted(graph.down_adj.get(v, ())):
                    t = graph.edges[ix].to_node
                    if t not in graph.nodes:
                        continue
                    back = t if o in owner.get(t, ()) else None
                    if back is None:
                        named, back = below(o, t)
                        if back is None:                 # it left the water: named, the sea or the atlas's edge
                            left = True
                            got |= named
                    if back is not None and back not in done:   # still the water (a braid, a lake on its course)
                        done.add(back)
                        nxt.append(back)
            frontier = sorted(nxt)
        out[o] = got
    return out


def exit_receivers(o: int, cands: Sequence[int], mouths: Dict[int, set], downs: Dict[int, Sequence[int]]) -> List[int]:
    """The waters water `o` may EXIT into (T59): of its `item_down` receivers `cands`, every one that does not itself
    flow into `o`, directly or down a chain of waters — a side channel whose mouth is back on `o` (Annacis Channel on
    the Fraser: `mouths`, from `mouth_downs`), one that reaches it through another (the upper Jeperson Side Channel
    through the lower), or a water with no main line (a lake) that `item_down` says drains into `o` (Moose Lake on
    the Fraser, a lake the creek runs out of). Flow has no cycles, so a water that flows into `o` is never where `o`
    goes. Empty for the Fraser: its mouth is the sea. A LAKE (no main line: absent from `mouths`) keeps every
    receiver: a creek running through it is both its inflow and its outlet, and a creek's main line (`lines.main_line`,
    the line carrying most of its length) can be its upper line, so the creek's "mouth" is no evidence against the
    lake's outlet (Adsit Lake, Crooked Lake)."""
    if o not in mouths:
        return list(cands)

    def into_o(d: int) -> bool:
        if d in mouths[o]:                      # o's own mouth reaches d: evidence for d, whatever d's says back
            return False
        seen, stack = {d}, [d]
        while stack:
            x = stack.pop()
            nxt = mouths[x] if x in mouths else downs.get(x, ())
            if o in nxt:
                return True
            for y in sorted(nxt):
                if y not in seen:
                    seen.add(y)
                    stack.append(y)
        return False
    return [d for d in cands if not into_o(d)]


# ---- the tables -------------------------------------------------------------------------------------------------

def write(db: sqlite3.Connection, geo: Dict[int, Geometry], cov,
          mouths: Optional[Dict[int, set]] = None,
          dfo_places: Optional[Tuple[Dict[str, str], List[List[str]]]] = None) -> None:
    """Write `part_geo`, `part_mark`, `water_exit` from the bundle's own parts, spans, splits and `item_down`, the exit
    never a water that flows into this one (`exit_receivers`, with `mouths` from `mouth_downs` while the graph was
    loaded)."""
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
    splits = {sid: {"name": n, "confidence": c}
              for sid, n, c in db.execute("SELECT split_id, name, confidence FROM split")}
    # T228: each section's DFO salmon chain (written before this by `salmon.write_bundle`; a bundle without it: none)
    chain: Dict[int, int] = dict(db.execute("SELECT sid, chain FROM salmon_section")) if db.execute(
        "SELECT 1 FROM sqlite_master WHERE name = 'salmon_section'").fetchone() else {}

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

    # T204: every main-stem section of the water, by part — what separates a part's runs into stretches
    stem_of: Dict[int, List[Tuple[int, float, float]]] = defaultdict(list)
    for (o, p), ss in sids.items():
        stem_of[o] += [(p, span[s][0], span[s][1]) for s in ss
                       if s in span and not span[s][4] and span[s][0] is not None and span[s][1] is not None]

    def midpoint(ss: List[int], poly: bool, g, rep_sid: Optional[int]) -> Tuple[int, object]:
        """(mid_sid, point) of a part or a stretch (the rules in the module docstring)."""
        nonlocal no_measure
        if poly:
            mid = rep_sid if rep_sid in ss else ss[0]
            return mid, (geo.get(mid) or g).representative_point()
        # only sections the atlas drew can hold the point (a few have no shape in geometries.pkl)
        stem = sorted((s for s in ss if s in span and s in geo and not span[s][4]
                       and span[s][0] is not None), key=lambda s: (span[s][0], s))
        if stem:
            mid = stem_midpoint([(s, span[s][1] - span[s][0]) for s in stem])
        else:                                   # no measures: summed section length, part order
            no_measure += 1
            mid = stem_midpoint([(s, geo[s].length) for s in ss if s in geo])
        return mid, point_on(geo[mid])

    def box(g) -> tuple:
        w, s_, e, n = to_ll.transform_bounds(*g.bounds)
        return round(w, 6), round(s_, 6), round(e, 6), round(n, 6)

    def ll(pt) -> tuple:
        lon, lat = to_ll.transform(pt.x, pt.y)
        return round(lon, 6), round(lat, 6)

    part_rows, mark_rows, stretch_rows, shapes = [], [], [], {}
    gap_rows: List[tuple] = []
    no_measure = 0
    for o, parts in sorted(by_ord.items()):
        gap_pairs: List[Tuple[int, int]] = []
        ranked = order_parts(parts)
        for rank, p in enumerate(ranked):
            gs = [geo[s] for s in p["sids"] if s in geo]
            if not gs:
                raise SystemExit(f"part_geo: water ord {o} part {p['v']} has no geometry in this atlas")
            g = unary_union(gs)
            shapes[(o, p["v"])] = g
            mid, pt = midpoint(p["sids"], p["poly"], g, rep[(o, p["v"])])
            part_rows.append((o, p["v"], rank, mid, *ll(pt), *box(g)))
        # T204: the stretches of every part, then their order in the water (mouth first: as `order_parts`, by km, then
        # part, then stretch; no km last). A part of one stretch is the part itself (its km, its shape).
        nodes = []
        for p in ranked:
            other = [(a, b) for v, a, b in stem_of[o] if v != p["v"]]
            st = [{"lo": None, "hi": None, "sids": p["sids"]}] if p["poly"] or not p["runs"] \
                else by_chain(stretches(p["runs"], span, other), chain, touch, span, splits, gap_pairs)
            for k, s in enumerate(st):
                km = p["km"] if len(st) == 1 else (None if s["hi"] is None else round(s["hi"] / 1000, 2))
                nodes.append({"part": p, "v": p["v"], "k": k, "n": len(st), "sids": s["sids"], "km": km,
                              "chain": s.get("chain", chain.get(s["sids"][0], 0) if s["sids"] else 0)})
        # T228: every salmon difference with no named split under it — one row per (water, the two chains), at the
        # first place they touch, the lower section first
        seen_gap = set()
        for x, y in gap_pairs:
            dx, uy = (x, y) if (span[x][0] if span.get(x) and span[x][0] is not None else 0) <= \
                (span[y][0] if span.get(y) and span[y][0] is not None else 0) else (y, x)
            k = (chain.get(dx, 0), chain.get(uy, 0))
            if k in seen_gap or dx not in geo or uy not in geo:
                continue
            seen_gap.add(k)
            pa, pb = nearest_points(geo[dx], geo[uy])
            lon, lat = to_ll.transform((pa.x + pb.x) / 2, (pa.y + pb.y) / 2)
            gap_rows.append((o, dx, uy, round(lon, 6), round(lat, 6), k[0], k[1]))
        nodes = (sorted((x for x in nodes if x["km"] is not None), key=lambda x: (x["km"], x["v"], x["k"]))
                 + [x for x in nodes if x["km"] is None])
        multi = any(x["n"] > 1 for x in nodes)
        for x in nodes:
            if x["n"] == 1:
                x["g"] = shapes[(o, x["v"])]
            else:
                gs = [geo[s] for s in x["sids"] if s in geo]
                if not gs:
                    raise SystemExit(f"part_stretch: water ord {o} part {x['v']} stretch {x['k']} has no geometry")
                x["g"] = unary_union(gs)
        if multi:
            for srank, x in enumerate(nodes):
                if x["n"] == 1:          # the part's own row, again: its midpoint and box are the part's
                    pr = next(r for r in part_rows if r[0] == o and r[1] == x["v"])
                    mid, lon, lat, bb = pr[3], pr[4], pr[5], pr[6:10]
                else:
                    mid, pt = midpoint(x["sids"], x["part"]["poly"], x["g"], rep[(o, x["v"])])
                    (lon, lat), bb = ll(pt), box(x["g"])
                stretch_rows.append((o, x["v"], x["k"], srank, mid, lon, lat, *bb, json.dumps(x["sids"])))
        raw = []
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                dn, up = nodes[i], nodes[j]
                same = dn["v"] == up["v"]
                # T228: two pieces of one part meet only at a named split on a salmon chain boundary; any other pair
                # of one part is not adjacent (or is a stretch apart, T204)
                if same and (dn.get("chain") == up.get("chain")
                             or chain_label(dn, up, span, splits, touch) is None):
                    continue
                a, b = dn["g"], up["g"]
                if a.distance(b) > TOUCH_M:
                    continue
                pa, pb = nearest_points(a, b)
                lon, lat = to_ll.transform((pa.x + pb.x) / 2, (pa.y + pb.y) / 2)
                at = chain_label(dn, up, span, splits, touch) if same \
                    else split_at(dn["part"], up["part"], splits) if dn["n"] == 1 and up["n"] == 1 \
                    else split_at(_runs_of(dn), _runs_of(up), splits)
                raw.append({"lon": round(lon, 6), "lat": round(lat, 6), "down": dn["v"], "up": up["v"],
                            "down_k": dn["k"], "up_k": up["k"],
                            "label": at["name"] if at else None, "confidence": (at or {}).get("confidence")})
        for k, m in enumerate(merge_marks(raw)):
            ks = m["up_k"] if any(m["up_k"]) or m["down_k"] else None
            mark_rows.append((o, k, m["lon"], m["lat"], m["label"], m["down"], json.dumps(m["up"]),
                              m.get("confidence"), m["down_k"] if ks is not None else None,
                              json.dumps(ks) if ks is not None else None))

    mouth = _mouths(part_rows)
    shapes_of: Dict[int, List[Tuple[int, Geometry]]] = defaultdict(list)
    for (o, p), g in sorted(shapes.items(), key=lambda kv: kv[0]):
        shapes_of[o].append((p, g))
    exits = []
    downs: Dict[int, List[int]] = defaultdict(list)
    for a, b in db.execute("SELECT ord, down_ord FROM item_down ORDER BY ord, down_ord"):
        downs[a].append(b)
    for o, cands in sorted(downs.items()):
        if mouths is not None:
            cands = exit_receivers(o, cands, mouths, downs)
        if o not in mouth or not cands:
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
    db.executemany("INSERT INTO part_mark (ord, mark_ix, lon, lat, label, down_part, up_parts, confidence, "
                   "down_stretch, up_stretches) VALUES (?,?,?,?,?,?,?,?,?,?)", mark_rows)
    db.executemany("INSERT INTO part_stretch (ord, part_ix, stretch_ix, rank, mid_sid, mid_lon, mid_lat, w, s, e, n, "
                   "sids) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", stretch_rows)
    db.executemany("INSERT INTO water_exit (ord, down_ord, down_part, label) VALUES (?,?,?,?)", exits)
    # T228 DIAGNOSTIC: a salmon difference where no named split stands — a DFO place's edge not yet placed as a split.
    # Not cut, not named: the pieces stay one (the salmon bar shows its "see Salmon below" days) until the edge is placed
    # (`pipeline.regs.dfo_salmon.edge_gaps` puts each on the review's DFO queue).
    db.executemany("INSERT INTO salmon_edge_gap (ord, down_sid, up_sid, lon, lat, down_chain, up_chain) "
                   "VALUES (?,?,?,?,?,?,?)", gap_rows)
    cov.filled("salmon_edge_gap", len(gap_rows))
    if gap_rows:
        words, places = dfo_places or ({}, [])
        print(f"  salmon edge gaps: {len(gap_rows)} salmon difference(s) with no named split (DFO place edges to "
              f"place as splits — not cut):")
        for o, _d, _u, lon, lat, ca, cb in gap_rows:
            a = places[ca] if ca < len(places) else []
            b = places[cb] if cb < len(places) else []
            only = [p for p in a if p not in b] + [p for p in b if p not in a]
            print(f"    {name.get(o)} at {lon},{lat}: {' | '.join(f'{p} ({words.get(p, '')})' for p in only[:2])}")
    cov.filled("part_geo", len(part_rows))
    cov.filled("part_mark", len(mark_rows))
    cov.filled("part_stretch", len(stretch_rows))
    cov.filled("water_exit", len(exits))
    print(f"  part topology: {len(part_rows):,} parts, {len(mark_rows):,} marks, {len(stretch_rows):,} stretch rows, "
          f"{len(exits):,} exits "
          f"({no_measure:,} stream parts placed by section length: no route measure)")


def _runs_of(x: dict) -> dict:
    """A stretch as `split_at` reads a part: its km and the runs (of its part) holding its sections."""
    ss = set(x["sids"])
    return {"km": x["km"], "runs": [r for r in x["part"].get("runs") or [] if ss & set(r["sids"])]}


def _mouths(part_rows: Sequence[tuple]) -> Dict[int, int]:
    """{ord: the part_ix of rank 0}."""
    return {o: p for o, p, rank, *_ in part_rows if rank == 0}
