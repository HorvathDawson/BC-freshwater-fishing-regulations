"""PART TOPOLOGY AND CAMERA HINTS (P09, MINIMAL-PACKAGING §10): everything the regs map navigation needs, WITHOUT geometry.

The app draws lines and polygons from the tiles (feature id = sid). The bundle carries only what the tiles cannot say:

  part_geo    (ord, part_ix): the bbox (lon/lat), and the midpoint: `mid_sid` and its point, where "Conditions here"
              opens (owner Q7). part_ix IS river order (T517, `derived.part_rows`; asserted here).
  part_mark   (ord, mark_ix): every PLACE two parts of a water meet (within TOUCH_M; T204: one mark per meeting
              place, not per pair of parts), with the split's name there (or NULL: the app shows no text), the
              downstream part and the upstream parts. Side channels and lake splits make the parts a graph, not a
              list. T517: two neighbouring pieces of one run across a lake meet at a mark too (the lake).
  (T517: a part IS one connected run — `derived.part_rows` cuts the rule-set groups into their runs, the DFO salmon
              chain in the key — so there is no `part_stretch`; T204's `stretches` is what decides a run, there.)
  water_exit  (ord): the water this one flows into (`item_down`), and the receiving water's part it enters.

THE RULES (stated, pinned in test_part_geo.py), moved here from `pipeline/tools/audit/water_map_export.py`, which now imports
`order_parts` and `split_label` from this module (one implementation):
  rank      by the part's km from the mouth (the highest `km_from` of its main-stem runs, else of its side channels;
            `answers.display.part_km`), mouth first; a side-channel part with no km right after the main-stem part it
            runs beside (`side_host`, T710); the other parts with no km last, in part order.
  midpoint  a stream part: the point at half its main-stem length, walked mouth first by route measure
            (`section_span`), or by summed section length where a section has no span; `mid_sid` is the section
            holding it, and the point is that section's own midpoint (within one section of the exact point). A lake
            part: `rep_sid` and the polygon's representative point.
  stretch   (used by `derived.part_rows`) a group's main-stem runs, mouth up, merged unless another group of the water
            has a main-stem section in the gap between them; a side-channel run joins the stretch it re-enters within
            (else the nearest); a group with no main-stem run (side channels only, a lake) is one stretch.
  marks     two parts closer than TOUCH_M; the point is the middle of their nearest points; the down part is the lower
            index. Parts meeting at one point with one down part are one mark with several up parts. Its words are
            `answers.display.end_words` of the end token the two parts share (`mark_words`), the same words their
            names say there (asserted).
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


# end tokens every water has: they say nothing about which main-stem part a side channel runs beside
_COMMON_ENDS = frozenset({"mouth", "source", "lake_inlet", "lake_outlet"})


def _run_ends(p: dict, branch: bool) -> set:
    return {t for r in p.get("runs") or () if bool(r.get("branch")) == branch
            for t in (r.get("from"), r.get("to"))} - {None} - _COMMON_ENDS


def is_side_part(p: dict) -> bool:
    """A part made only of side-channel runs (no main-stem run, not a polygon)."""
    runs = p.get("runs") or []
    return bool(runs) and not p.get("poly") and all(r.get("branch") for r in runs)


def side_host(p: dict, mains: Sequence[dict]) -> Optional[dict]:
    """T710 (owner 2026-11-13, "List it where it sits"): the main-stem part a km-less side-channel part runs beside —
    the one whose main runs share the most end names with its runs, else the first it touches (`touches`: the `v` of
    the parts any of its sections borders); `mains` in river order, the first wins a tie. None: neither."""
    mine = _run_ends(p, True)
    score = [(len(mine & _run_ends(m, False)), -i) for i, m in enumerate(mains)]
    if score and max(score)[0] > 0:
        return mains[-max(score)[1]]
    near = set(p.get("touches") or ())
    return next((m for m in mains if m["v"] in near), None)


def order_parts(parts: Sequence[dict]) -> List[dict]:
    """Within one water: by km, mouth up. A side-channel part with no km goes right after the main-stem part it runs
    beside (`side_host`, T710), after any side part already there; the other parts with no km (lake parts, a side
    part with no host) last — both in part order (`v`, the input order)."""
    with_km = sorted((p for p in parts if p.get("km") is not None), key=lambda p: (p["km"], p["v"]))
    mains = [p for p in with_km if any(not r.get("branch") for r in p.get("runs") or ())]
    beside: Dict[int, List[dict]] = defaultdict(list)
    last: List[dict] = []
    for p in parts:
        if p.get("km") is not None:
            continue
        host = side_host(p, mains) if is_side_part(p) else None
        (beside[id(host)] if host is not None else last).append(p)
    return [x for p in with_km for x in [p] + beside.get(id(p), [])] + last


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
    """One mark per (point, down part), with every up part meeting there, sorted."""
    by: Dict[tuple, dict] = {}
    for m in raw:
        k = (round(m["lon"], 6), round(m["lat"], 6), m["down"])
        if k in by:
            by[k]["up"] = sorted(set(by[k]["up"]) | {m["up"]})
            if not by[k]["label"] and m["label"]:
                by[k]["label"], by[k]["token"] = m["label"], m.get("token")
                if m.get("confidence"):                  # T207: the named split's, with its name
                    by[k]["confidence"] = m["confidence"]
        else:
            by[k] = {"lon": m["lon"], "lat": m["lat"], "down": m["down"], "up": [m["up"]],
                     "label": m["label"], "token": m.get("token"),
                     **({"confidence": m["confidence"]} if m.get("confidence") else {}),
                     **({"alt": m["alt"]} if m.get("alt") else {})}
    return sorted(by.values(), key=lambda m: (m["down"], m["up"], m["lon"], m["lat"]))


def _stem_ends(p: dict) -> Tuple[Optional[str], Optional[str]]:
    """(the token at a part's lowest main-stem end, the one at its highest), or (None, None) with no main stem."""
    main = [r for r in p.get("runs") or [] if not r.get("branch") and r.get("km_from") is not None]
    if not main:
        return None, None
    return min(main, key=lambda r: r["km_from"]).get("to"), max(main, key=lambda r: r["km_from"]).get("from")


def mark_words(down: dict, up: dict, doc: dict, splits: Dict[str, dict]) -> Tuple[Optional[str], Optional[str],
                                                                                 Optional[str]]:
    """T517: (a mark's words, its confidence, the END TOKEN it stands at — what the release ships, `build.mark_token`)
    — `mark_alt` gives the other names it is known by. Where the lower part's top end IS the upper part's bottom end (one
    token), the mark says that end in `end_words` — the same words the two parts' names say there (asserted). Else an
    end token the two parts' runs share; else the named split `split_at` finds; else (None, None)."""
    from pipeline.deliver.answers.display.part import _named_token, end_variants, end_words, run_ends
    natural = {None, "", "mouth", "source"}
    top, bottom = _stem_ends(down)[1], _stem_ends(up)[0]
    tok = top if top == bottom and top not in natural else None
    if tok is None and (top or "").startswith("lake_outlet") and (bottom or "").startswith("lake_inlet"):
        # T517 review finding 10: two parts that meet across a lake (the lower one's top its outlet, the upper one's
        # bottom its inlet) meet AT the lake: the mark says it, as the upper part's name does
        return end_words(doc, bottom, "from"), None, bottom
    if tok is None:
        ends = lambda p: {e for r in p.get("runs") or [] for e in (r.get("to"), r.get("from"))}  # noqa: E731
        common = sorted((ends(down) & ends(up)) - natural)
        tok = common[0] if common else None
    if tok is None:
        s = split_at(down, up, splits)
        sid = next((k for k, v in splits.items() if v is s), None) if s else None
        return (s["name"], s.get("confidence"), sid) if s else (None, None, None)
    words = end_words(doc, tok, "from")
    if tok == top == bottom:
        a, b = run_ends(doc, down), run_ends(doc, up)
        if a and b and (a[1] != words or b[0] != end_words(doc, tok, "to")):
            raise SystemExit(f"part_mark: {tok!r} reads {words!r} on the mark but {a and a[1]!r} / {b and b[0]!r} "
                             f"on the parts it joins — one function names both (T517)")
    return words, (splits.get(_named_token(doc, tok) or "") or {}).get("confidence"), tok


def mark_alt(down: dict, up: dict, doc: dict) -> Optional[List[str]]:
    """T517: the OTHER names of the place a mark stands at (a gauge cut where a regulation's boundary stands: the
    gauge's own name), kept for search and the alias list — None when it has one name."""
    from pipeline.deliver.answers.display.part import end_variants
    top, bottom = _stem_ends(down)[1], _stem_ends(up)[0]
    if top is None or top != bottom or top in ("mouth", "source"):
        return None
    alt = end_variants(doc, top, "from")[1:]
    return alt or None


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


def code_top(codes: Dict[int, str], size: Dict[int, int]) -> Dict[str, int]:
    """T519 {watershed code: the size of its biggest water} — which water is its code's main river."""
    top: Dict[str, int] = {}
    for x, c in codes.items():
        top[c] = max(top.get(c, 0), size.get(x, 0))
    return top


def exit_receivers(o: int, cands: Sequence[int], mouths: Dict[int, set], downs: Dict[int, Sequence[int]],
                   size: Optional[Dict[int, int]] = None, codes: Optional[Dict[int, str]] = None,
                   streams: Optional[set] = None, top: Optional[Dict[str, int]] = None) -> List[int]:
    """The waters water `o` may EXIT into (T59): of its `item_down` receivers `cands`, every one that does not itself
    flow into `o`, directly or down a chain of waters — a side channel whose mouth is back on `o` (Annacis Channel on
    the Fraser: `mouths`, from `mouth_downs`), one that reaches it through another (the upper Jeperson Side Channel
    through the lower), or a water with no main line (a lake) that `item_down` says drains into `o` (Moose Lake on
    the Fraser, a lake the creek runs out of). Flow has no cycles, so a water that flows into `o` is never where `o`
    goes. Empty for the Fraser: its mouth is the sea. A LAKE (no main line: absent from `mouths`) keeps every
    receiver: a creek running through it is both its inflow and its outlet, and a creek's main line (`lines.main_line`,
    the line carrying most of its length) can be its upper line, so the creek's "mouth" is no evidence against the
    lake's outlet (Adsit Lake, Crooked Lake).
    T519: when o's own mouth reaches d AND d flows back into o (a braid: each mouth walk meets the other), the SMALLER
    water (`size`, its sections) is the side channel and never the bigger one's exit — since T497 named the delta's
    connectors, the Fraser's mouth walk met Morey Channel, its own side channel. Without `size` o's mouth wins."""
    if o not in mouths:
        return list(cands)

    def flows_into_o(d: int) -> bool:
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

    def channel_of_o(d: int) -> bool:
        # T519: FWA gives every channel of a river the river's watershed code. A SMALLER water whose main line is on
        # o's code is a channel of o (Morey Channel on the Fraser, since T497 named the delta's connectors), never
        # where o goes; a river of its own code (the Courtenay below the Puntledge) is
        if codes is None or size is None or codes.get(d) is None or codes.get(d) != codes.get(o):
            return False
        if streams is not None and d not in streams:
            return False                        # a lake on the river carries its code: where the river goes, if any
        # o must be its code's MAIN river (the biggest water on it): two side channels of one river (the upper
        # Jeperson into the lower) keep their exit
        tops = top if top is not None else code_top(codes, size)
        return size.get(o, 0) == tops.get(codes[o], 0) and size.get(d, 0) < size.get(o, 0)

    def into_o(d: int) -> bool:
        if channel_of_o(d):
            return True
        if d in mouths[o]:                      # o's own mouth reaches d: evidence for d, unless d is o's side channel
            return size is not None and size.get(d, 0) < size.get(o, 0) and flows_into_o(d)
        return flows_into_o(d)
    return [d for d in cands if not into_o(d)]


# ---- the tables -------------------------------------------------------------------------------------------------

def main_codes(graph, ids: Sequence[str], secs: Dict[int, set]) -> Dict[int, str]:
    """T519: {ord: the FWA watershed code carrying most of the water's stream length} — a channel of a river shares
    the river's code, so this is what says a small water is the big one's own channel."""
    from pipeline.deliver.bundle.lines import handle_lengths
    length = handle_lengths(graph.nodes, ids, {h for ss in secs.values() for h in ss})
    out: Dict[int, str] = {}
    for o, ss in secs.items():
        by: Dict[str, float] = defaultdict(float)
        for h in ss:
            n = graph.nodes.get(ids[h - 1]) if 0 < h <= len(ids) else None
            if n is not None and getattr(n, "wsc", ""):
                by[n.wsc] += length.get(h, 0.0) or 0.0
        if by:
            out[o] = max(sorted(by), key=lambda w: by[w])
    return out


def write(db: sqlite3.Connection, geo: Dict[int, Geometry], cov,
          mouths: Optional[Dict[int, set]] = None, codes: Optional[Dict[int, str]] = None,
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
    # T517: a gauge cut a part ends at is said by its nearest named feature — written BEFORE the splits are read for
    # the marks' words, which name it
    near_rows = gauge_near(db, geo, span, sids, kind)
    db.executemany("INSERT INTO split_near (split_id, token, m) VALUES (?,?,?)", near_rows)
    cov.filled("split_near", len(near_rows))
    pn_rows = part_near(db, geo, span, sids, kind)
    db.executemany("INSERT INTO part_near (ord, part_ix, near, m) VALUES (?,?,?,?)", pn_rows)
    cov.filled("part_near", len(pn_rows))
    cross_rows = crossing_near(db, geo, span, sids, kind)
    db.executemany("INSERT INTO crossing_near (ord, token, km, near, m) VALUES (?,?,?,?,?)", cross_rows)
    cov.filled("crossing_near", len(cross_rows))
    # T517: the words of a mark are `answers.display.end_words`' — the one function that names every run end
    from pipeline.deliver.export.read import read_splits
    words_doc = {"splits": read_splits(db), "waters": {i: {"name": n} for i, n in db.execute("SELECT item_id, name FROM item")}}
    grp_run = {(o, p): (g, r) for o, p, g, r in db.execute("SELECT ord, part_ix, grp, run FROM part")}
    # T517: a part's whole key — two parts with ONE key are runs another part separates (else they would be one part),
    # so where they come within TOUCH_M of each other (across a short sign zone) they do not meet: no mark
    key_of = {(o, p): (g, c) for o, p, g, c in db.execute("SELECT ord, part_ix, grp, chain FROM part")}
    # T517 (owner 2026-11-08) THE LAKE GAP: two pieces of one run (grp, run) the salmon chain cut either side of a lake
    # — ANY lake, named or not — meet AT the lake: a mark, so the app joins them by the one live rule (grp + run + live
    # salmon key) as though the lake were not there; different rules either side are separate parts already
    # T228: each section's DFO salmon chain (written before this by `salmon.write_bundle`; 0: the unlisted chain)
    chain: Dict[int, int] = dict(db.execute("SELECT sid, chain FROM salmon_section"))

    def is_polygon_part(o: int, ss: List[int]) -> bool:
        if kind.get(o) != "stream":
            return True
        rows = [span.get(s) for s in ss]
        return bool(rows) and all(r is not None and r[5] == 1 and r[0] is None for r in rows)

    by_ord: Dict[int, List[dict]] = defaultdict(list)
    part_of_sid = {s: (o, p) for (o, p), ss in sids.items() for s in ss}
    for (o, p), ss in sorted(sids.items()):
        poly = is_polygon_part(o, ss)
        runs = [] if poly or any(s not in span for s in ss) else SP.compose_runs(span, touch, ss)
        near = sorted({part_of_sid[y][1] for x in ss for y in touch.get(x, ())
                       if y in part_of_sid and part_of_sid[y][0] == o and part_of_sid[y][1] != p})
        by_ord[o].append({"v": p, "sids": ss, "km": None if poly else part_km(runs), "runs": runs, "poly": poly,
                          "touches": near})

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

    part_rows, mark_rows, shapes = [], [], {}
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
            part_rows.append((o, p["v"], mid, *ll(pt), *box(g)))
        # T517: a part IS one connected run (`derived.part_rows`), so the nodes are the parts, in rank order, and the
        # rank is the part index (river order is decided once, there)
        for rank, p in enumerate(ranked):
            if rank != p["v"]:
                raise SystemExit(f"part_geo: water ord {o} part {p['v']} ranks {rank}: part_ix is not river order")
        nodes = [{"part": p, "v": p["v"], "sids": p["sids"], "km": p["km"], "g": shapes[(o, p["v"])]} for p in ranked]
        # T228 DIAGNOSTIC: two parts of one run (same grp, run) cut where the salmon chain changes with no named split
        for a_ in nodes:
            for b_ in nodes:
                if a_["v"] >= b_["v"] or grp_run.get((o, a_["v"])) != grp_run.get((o, b_["v"])):
                    continue
                bs = set(b_["sids"])
                for x in a_["sids"]:
                    for y in sorted(touch.get(x, ())):
                        if y in bs and not cut_between(x, y, span, splits):
                            gap_pairs.append((x, y))
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

        def lake_gap(dn: dict, up: dict) -> bool:
            """Two neighbouring pieces of one run with a lake between them (the lower one's top a lake outlet, the
            upper one's bottom a lake inlet)."""
            if (dn["v"], up["v"]) not in next_in_run:
                return False
            top, bottom = _stem_ends(dn["part"])[1], _stem_ends(up["part"])[0]
            return (top or "").startswith("lake_outlet") and (bottom or "").startswith("lake_inlet")

        def stem_g(x):
            if "stem_g" not in x:
                gs = [geo[s] for s in x["sids"] if s in geo and s in span and not span[s][4]]
                x["stem_g"] = unary_union(gs) if gs else None
            return x["stem_g"]

        # each run's pieces in river order: a lake gap joins only NEIGHBOURING pieces of one run
        run_seq: Dict[tuple, List[int]] = defaultdict(list)
        for x in nodes:
            run_seq[grp_run[(o, x["v"])]].append(x["v"])
        next_in_run = {(q[k], q[k + 1]) for q in run_seq.values() for k in range(len(q) - 1)}

        raw = []
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                dn, up = nodes[i], nodes[j]
                if key_of[(o, dn["v"])] == key_of[(o, up["v"])]:
                    continue
                a, b = dn["g"], up["g"]
                if a.distance(b) > TOUCH_M and not lake_gap(dn, up):
                    continue
                # the mark stands where the MAIN STEMS meet when they do: a cut swept across a braid also separates
                # the side channel, and the nearest pair there put the mark on the braid (Cranberry canyon bottom)
                sa, sb = stem_g(dn), stem_g(up)
                if sa is not None and sb is not None and sa.distance(sb) <= TOUCH_M:
                    a, b = sa, sb
                pa, pb = nearest_points(a, b)
                lon, lat = to_ll.transform((pa.x + pb.x) / 2, (pa.y + pb.y) / 2)
                if a.distance(b) > TOUCH_M:           # a lake gap: the mark says the lake ("A lake")
                    from pipeline.deliver.answers.display.part import end_words
                    tok = _stem_ends(up["part"])[0]
                    label, conf = end_words(words_doc, tok, "from"), None
                else:
                    label, conf, tok = mark_words(dn["part"], up["part"], words_doc, splits)
                alt = mark_alt(dn["part"], up["part"], words_doc)
                raw.append({"lon": round(lon, 6), "lat": round(lat, 6), "down": dn["v"], "up": up["v"],
                            "label": label, "token": tok if label else None, "confidence": conf,
                            **({"alt": alt} if alt else {})})
        for k, m in enumerate(merge_marks(raw)):
            mark_rows.append((o, k, m["lon"], m["lat"], m["label"], m["down"], json.dumps(m["up"]),
                              m.get("confidence"), json.dumps(m["alt"]) if m.get("alt") else None, m.get("token")))

    mouth = _mouths(part_rows)
    shapes_of: Dict[int, List[Tuple[int, Geometry]]] = defaultdict(list)
    for (o, p), g in sorted(shapes.items(), key=lambda kv: kv[0]):
        shapes_of[o].append((p, g))
    exits = []
    size_of = dict(db.execute("SELECT ord, COUNT(*) FROM item_section GROUP BY ord"))
    streams = {o for o, k in kind.items() if k == "stream"}
    tops = code_top(codes, size_of) if codes is not None else None
    downs: Dict[int, List[int]] = defaultdict(list)
    for a, b in db.execute("SELECT ord, down_ord FROM item_down ORDER BY ord, down_ord"):
        downs[a].append(b)
    exit_down: List[Tuple[int, int]] = []
    for o, cands in sorted(downs.items()):
        if mouths is not None:
            cands = exit_receivers(o, cands, mouths, downs, size_of, codes, streams, tops)
        # (T679 D3) every receiver left once the flow-back is removed: the nearer ✱ row reads these, not `item_down`
        exit_down += [(o, d) for d in sorted(set(cands))]
        if o not in mouth or not cands:
            continue
        here = shapes[(o, mouth[o])]
        best = None
        for d in cands:
            for p, g in shapes_of.get(d, ()):
                cand = (here.distance(g), d, p)
                if best is None or cand < best:
                    best = cand
        if best is None:                                # (T517 review: no silent exit with no part — 0 on 2026-11-08)
            raise SystemExit(f"water_exit: {name.get(o)} (ord {o}) drains into {[name.get(d) for d in cands]}, "
                             f"none of which has a drawn part")
        exits.append((o, best[1], best[2], name.get(best[1])))

    db.executemany("INSERT INTO part_geo (ord, part_ix, mid_sid, mid_lon, mid_lat, w, s, e, n) "
                   "VALUES (?,?,?,?,?,?,?,?,?)", part_rows)
    db.executemany("INSERT INTO part_mark (ord, mark_ix, lon, lat, label, down_part, up_parts, confidence, alt, token) "
                   "VALUES (?,?,?,?,?,?,?,?,?,?)", mark_rows)
    db.executemany("INSERT INTO water_exit (ord, down_ord, down_part, label) VALUES (?,?,?,?)", exits)
    db.executemany("INSERT INTO item_exit_down (ord, down_ord) VALUES (?,?)", exit_down)
    cov.filled("item_exit_down", len(exit_down))
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
    cov.filled("water_exit", len(exits))
    print(f"  part topology: {len(part_rows):,} parts, {len(mark_rows):,} marks, "
          f"{len(exits):,} exits "
          f"({no_measure:,} stream parts placed by section length: no route measure)")


#: T517 (owner 2026-11-09: no fallback hides an error) a named feature farther than this from a gauge cut is not "near"
#: it: the Dean's 08FC003 had only Tanswanket Creek's drawn sections 13.2 km away. Past it the cut is said by its km.
NEAR_M = 2000.0


def gauge_near(db: sqlite3.Connection, geo: Dict[int, Geometry], span: Dict[int, tuple],
               sids: Dict[Tuple[int, int], List[int]], kind: Dict[int, str]) -> List[Tuple[str, str, float]]:
    """T517 (review finding 7) THE NEAREST NAMED FEATURE TO A GAUGE CUT a part ends at: a gauge's code and station name
    are never the words of a place a regulation's part ends at (they go to the mark's `alt`), so the end is said by what
    stands nearest the cut on the ground — a named tributary's confluence (`item_down` into the water, distance to its
    nearest drawn section), or a named non-gauge, non-length cut of the water (the end of its sections). Rows (split_id,
    token, metres): `token` an end token the answers name (`confluence:<item>` or the cut's id). Only gauge cuts that are
    a run end of a named water's section, and only a feature within `NEAR_M`: none farther (the answers then say the
    cut by its km from the mouth, `part._named_token`)."""
    from shapely.ops import nearest_points
    kinds = dict(db.execute("SELECT split_id, kind FROM split"))
    # a cut with a name of its own: never a gauge, never a length or shore cut (`length:…`, T660 `shore:…`: its name is a generated description
    # of an unnamed side channel's junction, "side channel above Iltasyuko River", not a named feature)
    named_cut = {k for k, n in db.execute("SELECT split_id, name FROM split")
                 if n and kinds.get(k) != "gauge" and not k.startswith(("length:", "shore:"))}
    item_id = dict(db.execute("SELECT ord, item_id FROM item"))
    owner = {x: o for (o, _p), ss in sids.items() for x in ss}
    ends: Dict[Tuple[int, str], List[int]] = defaultdict(list)       # (water, token) -> the sections ending there
    for x, r in span.items():
        if x in owner:
            for t in {r[2], r[3]} - {None}:
                ends[(owner[x], t)].append(x)
    tribs: Dict[int, List[int]] = defaultdict(list)
    for a, b in db.execute("SELECT ord, down_ord FROM item_down ORDER BY ord"):
        if kind.get(a) == "stream":
            tribs[b].append(a)
    secs_of: Dict[int, List[int]] = defaultdict(list)
    for (o, _p), ss in sids.items():
        secs_of[o] += ss
    best: Dict[str, Tuple[float, str]] = {}           # one feature per cut: the nearest over every water it ends a part of
    for (o, t), xs in sorted(ends.items()):
        if kinds.get(t) != "gauge":
            continue
        gs = [geo[x] for x in sorted(xs) if x in geo]
        if not gs:
            continue
        if len(gs) >= 2:
            pa, pb = nearest_points(gs[0], gs[1])
            from shapely.geometry import Point
            pt = Point((pa.x + pb.x) / 2, (pa.y + pb.y) / 2)
        else:
            pt = gs[0].representative_point()
        cands: List[Tuple[float, str]] = []
        for tr in tribs.get(o, ()):
            d = min((geo[x].distance(pt) for x in secs_of.get(tr, ()) if x in geo), default=None)
            if d is not None:
                cands.append((d, f"confluence:{item_id[tr]}"))
        for (o2, c), cx in ends.items():
            if o2 == o and c in named_cut:
                d = min((geo[x].distance(pt) for x in cx if x in geo), default=None)
                if d is not None:
                    cands.append((d, c))
        cands = [c for c in cands if c[0] <= NEAR_M]
        if cands:
            c = min(cands)
            if t not in best or c < best[t]:
                best[t] = c
    return [(t, tok, round(d, 1)) for t, (d, tok) in sorted(best.items())]


#: a named tributary whose drawn sections come no closer than this to the water's main stem meets it at no place the
#: atlas draws (its named part starts far up): it is no feature ON the water
TRIBUTARY_REACH_M = 200.0
#: (owner 2026-11-09) a B.C.-border crossing with no named water feature within `NEAR_M` is said near the nearest
#: B.C. town (`place`: city, town, village, hamlet) within this — "B.C. border near Osoyoos"
TOWN_M = 10_000.0
TOWN_KINDS = ("city", "town", "village", "hamlet")
def part_near(db: sqlite3.Connection, geo: Dict[int, Geometry], span: Dict[int, tuple],
              sids: Dict[Tuple[int, int], List[int]], kind: Dict[int, str]) -> List[tuple]:
    """T517 (owner 2026-11-09) WHERE A PART WITH NO MAIN STEM LIES, for a water with several such parts (they are
    said "<water> near <what>"): (ord, part_ix, the token of the nearest OTHER named water within `NEAR_M` — `water:
    <item>` — else of the nearest B.C. town within `TOWN_M` — `place:<name>` —, metres). A part with neither gets no
    row and its name cannot be told apart: `answers.display.name_check` stops the build."""
    from pyproj import Transformer
    from shapely.geometry import Point
    from shapely.ops import unary_union
    item_id = dict(db.execute("SELECT ord, item_id FROM item"))
    stemless: Dict[int, List[int]] = defaultdict(list)
    for (o, p), ss in sorted(sids.items()):
        if kind.get(o) == "stream" and not any(x in span and not span[x][4] and span[x][0] is not None for x in ss):
            stemless[o].append(p)
    want = {o: ps for o, ps in stemless.items() if len(ps) > 1}
    if not want:
        return []
    to3005 = Transformer.from_crs(4326, 3005, always_xy=True)
    towns = [(Point(*to3005.transform(lon, lat)), name) for name, lon, lat in db.execute(
        f"SELECT name, lon, lat FROM place WHERE kind IN ({','.join('?' * len(TOWN_KINDS))}) AND name IS NOT NULL "
        "AND lat IS NOT NULL ORDER BY place_id", TOWN_KINDS)]
    shapes: Dict[int, object] = {}
    for (o, _p), ss in sids.items():
        gs = [geo[x] for x in ss if x in geo]
        if gs:
            shapes[o] = unary_union([shapes[o], *gs]) if o in shapes else unary_union(gs)
    out: List[tuple] = []
    for o, ps in sorted(want.items()):
        for p in ps:
            g = unary_union([geo[x] for x in sids[(o, p)] if x in geo])
            box = g.buffer(NEAR_M)
            near = min(((g.distance(sg), item_id[w]) for w, sg in shapes.items() if w != o and sg.intersects(box)),
                       default=None)
            if near is not None:
                out.append((o, p, f"water:{near[1]}", round(near[0], 1)))
                continue
            d, name = min(((g.distance(tp), nm) for tp, nm in towns), default=(None, None))
            if d is not None and d <= TOWN_M:
                out.append((o, p, f"place:{name}", round(d, 1)))
    return out


def is_crossing(t: Optional[str]) -> bool:
    """An end token a part can share with another part of its water at ANOTHER place (said near what stands there)."""
    return bool(t) and (t in ("bc_border", "polygon", "lake_inlet", "lake_outlet")
                        or t.startswith(("area:", "region_line:")))


def crossing_near(db: sqlite3.Connection, geo: Dict[int, Geometry], span: Dict[int, tuple],
                  sids: Dict[Tuple[int, int], List[int]], kind: Dict[int, str]) -> List[tuple]:
    """T517 (owner 2026-11-09) WHAT STANDS AT EACH CROSSING: for every place a named stream's main stem meets a token
    two of its parts could share (`is_crossing`: an area's boundary, the B.C. border, a region line, a river polygon,
    an unnamed lake), rows (ord, token, km from the mouth, the end token that names what stands there, metres):
      * the nearest NAMED WATER FEATURE within `NEAR_M` along the stem — a named tributary that meets the stem
        (`item_down` into the water, its drawn sections within `TRIBUTARY_REACH_M`: `confluence:<item>`) or a named cut
        (not a gauge, not a length cut);
      * else, for the B.C. border, the nearest B.C. town within `TOWN_M` (`place:<name>`);
      * else no row — the answers then say the crossing at its km.
    `answers.display.builders._names` reads it where two parts of one water would otherwise say one name."""
    from pyproj import Transformer
    from shapely.geometry import Point
    from shapely.ops import nearest_points, unary_union
    kinds = dict(db.execute("SELECT split_id, kind FROM split"))
    named_cut = {k for k, n in db.execute("SELECT split_id, name FROM split")
                 if n and kinds.get(k) != "gauge" and not k.startswith(("length:", "shore:"))}
    item_id = dict(db.execute("SELECT ord, item_id FROM item"))
    to3005 = Transformer.from_crs(4326, 3005, always_xy=True)
    towns = [(Point(*to3005.transform(lon, lat)), name) for name, lon, lat in db.execute(
        f"SELECT name, lon, lat FROM place WHERE kind IN ({','.join('?' * len(TOWN_KINDS))}) AND name IS NOT NULL "
        "AND lat IS NOT NULL ORDER BY place_id", TOWN_KINDS)]
    secs_of: Dict[int, List[int]] = defaultdict(list)
    for (o, _p), ss in sids.items():
        secs_of[o] += ss
    tribs: Dict[int, List[int]] = defaultdict(list)
    for a, b in db.execute("SELECT ord, down_ord FROM item_down ORDER BY ord"):
        if kind.get(a) == "stream":
            tribs[b].append(a)
    out: List[tuple] = []
    for o, ss in sorted(secs_of.items()):
        if kind.get(o) != "stream":
            continue
        stem = sorted((x for x in ss if x in span and x in geo and not span[x][4] and span[x][0] is not None
                       and span[x][1] is not None), key=lambda x: (span[x][0], x))
        ends = [(t, m, x, side) for x in stem for t, m, side in ((span[x][2], span[x][0], "lo"),
                                                                  (span[x][3], span[x][1], "hi")) if is_crossing(t)]
        if not ends:
            continue
        feats: List[Tuple[float, str]] = []                 # (metres along the stem, token)
        for x in stem:
            for t, m in ((span[x][2], span[x][0]), (span[x][3], span[x][1])):
                if t in named_cut:
                    feats.append((float(m), t))
        sg = unary_union([geo[x] for x in stem])
        for tr in tribs.get(o, ()):
            tg = [geo[x] for x in secs_of.get(tr, ()) if x in geo]
            if not tg:
                continue
            tu = unary_union(tg)
            if tu.distance(sg) > TRIBUTARY_REACH_M:
                continue
            p = nearest_points(sg, tu)[0]
            x = min(stem, key=lambda s: (geo[s].distance(p), s))
            feats.append((_measure_at(x, p, stem, span, geo), f"confluence:{item_id[tr]}"))
        done = set()
        for t, m, x, side in sorted(ends, key=lambda e: (e[1], e[0])):
            key = (t, round(m / 1000.0, 2))
            if key in done:
                continue
            done.add(key)
            near = min(((abs(fm - m), ft) for fm, ft in feats if ft != t), default=None)
            if near is not None and near[0] <= NEAR_M:
                out.append((o, t, key[1], near[1], round(near[0], 1)))
                continue
            if t == "bc_border" and towns:
                pt = _end_point(x, side, stem, span, geo)
                d, name = min(((pt.distance(tp), nm) for tp, nm in towns), key=lambda v: (v[0], v[1]))
                if d <= TOWN_M:
                    out.append((o, t, key[1], f"place:{name}", round(d, 1)))
    return out


def _end_point(x: int, side: str, stem: List[int], span: Dict[int, tuple], geo: Dict[int, Geometry]):
    """The drawn point of stem section `x`'s lower (`lo`) or upper (`hi`) end: the end of its line nearer the stem
    section below it (else farther from the one above) — the line's own direction is not trusted."""
    from shapely.geometry import Point
    g = geo[x]
    if g.geom_type == "MultiLineString":
        from shapely.ops import linemerge
        g = linemerge(g)
    if g.geom_type != "LineString":
        return g.representative_point()
    a, b = Point(g.coords[0]), Point(g.coords[-1])
    below = [s for s in stem if s != x and span[s][1] <= span[x][0]]
    above = [s for s in stem if s != x and span[s][0] >= span[x][1]]
    if below:
        n = geo[max(below, key=lambda s: (span[s][1], s))]
        low = a if a.distance(n) <= b.distance(n) else b
    elif above:
        n = geo[min(above, key=lambda s: (span[s][0], s))]
        low = b if a.distance(n) <= b.distance(n) else a
    else:
        low = a
    return low if side == "lo" else (b if low is a else a)


def _measure_at(x: int, p, stem: List[int], span: Dict[int, tuple], geo: Dict[int, Geometry]) -> float:
    """The main-stem measure (metres) of point `p` on stem section `x`, read from its lower end (`_end_point`)."""
    from shapely.ops import linemerge
    g = geo[x]
    if g.geom_type == "MultiLineString":
        g = linemerge(g)
    if g.geom_type != "LineString" or g.length == 0:
        return float(span[x][0])
    f = g.project(p, normalized=True)
    forward = _end_point(x, "lo", stem, span, geo).distance(g.interpolate(0.0)) < 1e-6
    return span[x][0] + (f if forward else 1.0 - f) * (span[x][1] - span[x][0])


def _mouths(part_rows: Sequence[tuple]) -> Dict[int, int]:
    """{ord: its mouth part — part_ix 0, river order}."""
    return {o: p for o, p, *_ in part_rows if p == 0}
