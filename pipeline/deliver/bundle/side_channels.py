"""T1140 (owner 2026-11-25: "They should always have the cut names or distance it is impossible not to", "the distance must
be derived"; coordinator's rules the same evening) WHAT A PART WITH NO MAIN STEM IS, MEASURED ON THE GROUND, in the bundle
stage while the named sections' shapes are in hand (`topology.keep_named`, EPSG:3005 metres). Two facts, each a general rule:

1. WHERE A SIDE CHANNEL JOINS ITS RIVER, where the flow pointers do not say it (`spans.compute`'s `attach` walks the graph
   down to the main stem; a channel that never reaches it there — its line ends short of the river, an added stream with
   no flow link — keeps `section_span.lo_m` NULL and its part would read "(from the mouth up to the top of the river)").
   The join is the point of its OWN water's main line nearest to the channel's DOWNSTREAM END — the end nearest the
   section its flow pointer leaves the channel for (`section_down`) — or, where no pointer leaves it for a section with
   a shape (the two ends cannot then be told apart), the END NEARER THE MAIN LINE; measured along the main line from the mouth as every
   other span is. Guards: the main line is the same water's (`item_section`); the snap is at most `MAX_SNAP_M`; the main
   line section it lands on must be orientable (its downstream or upstream neighbour on the stem says which end is the
   mouth's). (T1140 proposal, coordinator 2026-11-25, "a consistent way to handle this") A channel whose downstream end
   TOUCHES a water body of its own river joins the river AT it: a named lake its main line runs through names the
   channel's lower end (`section_span.lo` = `lake_inlet:<lake>`: "side channel into <Lake>"); its own polygon (a wide
   reach, an unnamed lake of the river) is measured at the main line's point nearest the touch, whatever the distance
   — the snap limit holds only where the end touches NO water body of its river. A channel failing one gets NO join (never a guessed distance), logged here; a part whose NAME then lacks a
   distance stops the build at the answers step, named (`answers.display.builders._names`). Written into
   `section_span.lo_m = hi_m` of every section of that channel: the export then says "joining N km from the mouth" by
   its one rule (`display.part.runs_label`).
2. WHICH AREA A PART LIES INSIDE, OR AT (`part_inside`): a part with no main stem whose every section is measured and
   at least `INSIDE_SHARE` of it lies in one cut area of the atlas's catalogue (`area_catalog.gpkg`, `cut` = 1: the
   areas the atlas cuts streams at — reserves, national parks, restricted land; never a region, an MU group or a sign
   zone): its lines by length, its polygon sections by area. `answers.display` names it "<water> inside <area>". Its
   lines that far inside while a polygon section of it straddles the area's edge (in it, but not that far): "<water> at
   <area>" (owner 2026-11-25, the Okanagan River Oxbows at Hayne's Lease Ecological Reserve).
3. (PROPOSAL, coordinator 2026-11-25, under the owner's "do whatever makes our current state decent until this is fixed
   in hardening") A PART OF ADDED LINES WITH ITS OWN NAME (`part_own_name`): every section an added-stream line (a
   negative blk in `section_measure`), all one added stream's name (`added_streams.json` `streams[].name`, read at build
   time), not the water's: named by it, with the words of its source (`name_source.label`). Noons Creek p4 is the City of
   Port Moody's Turner Creek, Seymour River p3 the City of Burnaby's Tunnel Creek (a watershed-code collision filed them
   there). One with no own name gets no row (and stops the build at the answers step if its name needs a distance).
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

#: The farthest (metres) a channel's end may lie from its river's main line for the join to be said. Set from the data
#: (T1140, r-71d09ad82e81, every channel the rule applies to printed at build): the 15 it measures lie at 0 m; the nearest
#: beyond is 231 m (the Squamish's, said inside its reserve, needing none), then 344 m. 50 m keeps every real join and
#: none of those.
MAX_SNAP_M = 50.0
#: The share of a part that must lie in one area for the part to be said inside it.
INSIDE_SHARE = 0.95
#: How near (metres) a channel's end must lie to a water body to TOUCH it (lines end on the polygons' edges).
TOUCH_M = 1.0
#: The water kinds a channel can flow into on its river's main line: named lakes.
LAKE_KINDS = ("lake", "reservoir")
#: Area kinds of the catalogue that are never a part's name (a region, an MU group, a sign zone: lines, not places).
NOT_A_PLACE = ("region", "mu_group", "sign_zone")


class SideChannelError(SystemExit):
    """What the rules cannot measure (a main line of one section; no area catalogue): never a guessed answer."""


def _components(sids: List[int], touch: Dict[int, set]) -> List[List[int]]:
    """The connected groups of `sids` by `section_touch`, each sorted, in order of their lowest sid."""
    left, out = set(sids), []
    for s in sorted(sids):
        if s not in left:
            continue
        comp, todo = [], [s]
        left.discard(s)
        while todo:
            x = todo.pop()
            comp.append(x)
            for y in sorted(touch.get(x, ())):
                if y in left:
                    left.discard(y)
                    todo.append(y)
        out.append(sorted(comp))
    return out


def _ends(geoms: List[object]) -> List[object]:
    """The end points of a channel's lines (a polygon has none: its outline stands for it)."""
    from shapely.geometry import Point
    from shapely.ops import linemerge, unary_union
    lines = [g for g in geoms if g.geom_type in ("LineString", "MultiLineString")]
    if not lines:
        return [unary_union(geoms)]
    u = unary_union(lines)
    m = linemerge(u) if u.geom_type == "MultiLineString" else u
    parts = list(m.geoms) if m.geom_type == "MultiLineString" else [m]
    pts = []
    for p in parts:
        cs = list(p.coords)
        pts += [Point(cs[0]), Point(cs[-1])]
    return pts


def downstream_ends(channel: List[object], down: Optional[object]) -> List[object]:
    """The channel's downstream end, as a list of one, where its flow pointer says it (`down`: the shape the flow leaves it
    for); else both ends — which is downstream cannot be told."""
    ends = _ends(channel)
    if down is not None and len(ends) > 1:
        return [min(ends, key=lambda p: (round(p.distance(down), 3), p.x, p.y))]
    return ends


def touched(ends: List[object], own: List[Tuple[int, object]], lakes: List[Tuple[str, object]]) -> Optional[tuple]:
    """What water body of its river a channel's downstream end touches (within `TOUCH_M`): ("lake", lake item id, the
    end) for a named lake its main line runs through — first, a lake beats the river's own polygon it lies in — else
    ("own", sid, the end) for the river's own polygon section; None when it touches neither. Ties: the lowest id."""
    for kind, bodies in (("lake", lakes), ("own", own)):
        hits = sorted(((key, e) for e in ends for key, g in bodies if e.distance(g) <= TOUCH_M),
                      key=lambda h: (h[0], h[1].x, h[1].y))
        if hits:
            return kind, hits[0][0], hits[0][1]
    return None


def join_measure(channel: List[object], stem: List[Tuple[int, int, object]],
                 down: Optional[object] = None, end: Optional[object] = None) -> Tuple[float, float, int]:
    """(the join's metres from the mouth along the main line, the snap in metres, the stem section it lands on) for a
    channel (its sections' shapes) against its water's main line (`stem`: [(lo_m, hi_m, shape)] of its stem sections).
    The end of the channel nearer the main line, snapped to the nearest stem section, placed between that section's
    measures from the end its downstream neighbour (else upstream neighbour) touches. Ties go to the lower measure.
    `down`: the shape the channel's flow leaves it for — its end nearest that is its downstream end, the one snapped;
    `end`: the end to snap, given (the end that touches a water body of the river)."""
    from shapely.ops import nearest_points
    best = None
    ends = [end] if end is not None else _ends(channel)
    if end is None and down is not None and len(ends) > 1:
        ends = [min(ends, key=lambda p: (round(p.distance(down), 3), p.x, p.y))]
    for e in ends:
        for k, (lo, hi, g) in enumerate(stem):
            d = e.distance(g)
            key = (round(d, 3), lo, k)
            if best is None or key < best[0]:
                best = (key, e, k)
    assert best is not None
    (_d, _lo, k), e, _ = best[0], best[1], best[2]
    lo, hi, g = stem[k]
    on = nearest_points(e, g)[1]
    # which end of this stem section is downstream: the one nearer the stem sections below it, else farther from those
    # above it; a stem of one section cannot be oriented (the caller stops)
    below = [s for s in stem if s[1] <= lo + 1 and s[2] is not g]
    above = [s for s in stem if s[0] >= hi - 1 and s[2] is not g]
    from shapely.geometry import Point
    line = g if g.geom_type == "LineString" else max(getattr(g, "geoms", [g]), key=lambda x: x.length)
    p0, p1 = Point(line.coords[0]), Point(line.coords[-1])
    if below:
        ref = min(below, key=lambda s: (s[2].distance(p0) + s[2].distance(p1), s[0]))[2]
        down0 = ref.distance(p0) <= ref.distance(p1)
    elif above:
        ref = min(above, key=lambda s: (s[2].distance(p0) + s[2].distance(p1), s[0]))[2]
        down0 = ref.distance(p0) > ref.distance(p1)
    else:
        raise SideChannelError("side channel: its water's main line is one section — which end is the mouth's is not known")
    t = line.project(on) / line.length if line.length else 0.0
    frac = t if down0 else 1.0 - t
    return lo + (hi - lo) * frac, float(e.distance(g)), k


def write(db: sqlite3.Connection, geo: Dict[int, object], build_dir: Path, cov, *, log=print,
          problems: Optional[List[str]] = None, added: Optional[Dict[int, str]] = None) -> None:
    """Fill the join measures `spans.compute` could not walk to, and write `part_inside` (the module's two rules).
    A channel the join rule cannot say is left without one and logged ("no join"; `problems` collects them too): only
    a part whose name needs that distance stops the build (`answers.display.builders._names`)."""
    def stop(msg: str) -> None:
        if problems is not None:
            problems.append(msg)
        log(f"     side channel NO JOIN: {msg}")
    item = {o: (i, n, k) for o, i, n, k in db.execute("SELECT ord, item_id, name, kind FROM item")}
    secs: Dict[int, List[int]] = defaultdict(list)
    for o, s in db.execute("SELECT ord, sid FROM item_section ORDER BY ord, sid"):
        secs[o].append(s)
    span = {s: (a, b, off, shape) for s, a, b, off, shape in db.execute(
        "SELECT sid, lo_m, hi_m, off_stem, shape FROM section_span")}
    down = dict(db.execute("SELECT sid, down_sid FROM section_down"))
    touch: Dict[int, set] = defaultdict(set)
    for a, b in db.execute("SELECT a, b FROM section_touch"):
        touch[a].add(b)
        touch[b].add(a)
    # 1. the joins — of the parts with NO main stem only (a part on its main stem is said by its ends: its channels'
    # joins name nothing, and are left exactly as the flow says them)
    stemless: set = set()
    by_part: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    # a LISTED part only: keyed (in B.C., with rules) and not a tidal piece (never on the map, never named)
    for o, s, p in db.execute("SELECT ps.ord, ps.sid, ps.part_ix FROM part_section ps JOIN part pt ON pt.ord = ps.ord "
                              "AND pt.part_ix = ps.part_ix WHERE pt.tidal = 0 AND pt.key_ix IS NOT NULL "
                              "ORDER BY ps.ord, ps.part_ix, ps.sid"):
        by_part[(o, p)].append(s)
    # a water with SEVERAL such parts names each by what it lies near (`topology.part_near`): only the ONE stemless part
    # of a water is said by its channels' joins (the T1140 rule's parts)
    per_water: Dict[int, List[Tuple[int, int]]] = defaultdict(list)
    for (o, p), ss in by_part.items():
        if all(s in span and span[s][2] == 1 for s in ss):
            per_water[o].append((o, p))
    for o, ps in per_water.items():
        if len(ps) == 1:
            stemless.update(by_part[ps[0]])
    filled = 0
    # the named lakes, for the touch rule: each lake item's polygon sections
    lake_geo: Dict[str, List[object]] = defaultdict(list)
    for o, (iid, _n, k) in item.items():
        if k in LAKE_KINDS:
            lake_geo[iid] += [geo[s] for s in secs.get(o, ()) if s in geo]
    lake_items = sorted(i for i, gs in lake_geo.items() if gs)
    lake_tree = None
    if lake_items:
        from shapely.ops import unary_union
        from shapely.strtree import STRtree
        lake_shape = {i: unary_union(lake_geo[i]) for i in lake_items}
        lake_tree = STRtree([lake_shape[i] for i in lake_items])
    eid = dict((t, e) for e, t in db.execute("SELECT eid, token FROM span_end"))
    for o in sorted(secs):
        iid, name, kind = item[o]
        if kind != "stream":
            continue
        loose = [s for s in secs[o] if s in stemless and span[s][0] is None]
        if not loose:
            continue
        stem = sorted((span[s][0], span[s][1], geo[s]) for s in secs[o]
                      if s in span and span[s][2] == 0 and span[s][3] == 0 and span[s][0] is not None and s in geo)
        if not stem:
            continue
        # this river's water bodies: its own polygon sections, and the named lakes its main line runs through
        own = [(s, geo[s]) for s in secs[o] if s in span and span[s][3] == 1 and s in geo]
        lakes: List[Tuple[str, object]] = []
        if lake_tree is not None:
            from shapely.ops import unary_union
            line = unary_union([g for _a, _b, g in stem])
            lakes = sorted((lake_items[int(j)], lake_shape[lake_items[int(j)]]) for j in lake_tree.query(line)
                           if lake_shape[lake_items[int(j)]].distance(line) <= TOUCH_M)
        for comp in _components(loose, touch):
            shapes = [geo[s] for s in comp if s in geo]
            if len(shapes) != len(comp):
                stop(f"side channel: {iid} ({name}) sections {comp}: some have no shape in this atlas")
                continue
            inside = set(comp)
            leave = sorted({down[s] for s in comp if s in down and down[s] not in inside and down[s] in geo})
            dn = geo[leave[0]] if len(leave) == 1 else None
            hit = touched(downstream_ends(shapes, dn), [x for x in own if x[0] not in inside], lakes)
            if hit and hit[0] == "lake":
                # into a named lake on its river's main line: that is its lower end (rule A names it)
                tok = f"lake_inlet:{hit[1]}"
                if tok not in eid:
                    eid[tok] = (max(eid.values()) + 1) if eid else 1
                    db.execute("INSERT INTO span_end (eid, token) VALUES (?, ?)", (eid[tok], tok))
                low = min(comp, key=lambda s: (geo[s].distance(hit[2]), s))
                db.execute("UPDATE section_span SET lo = ? WHERE sid = ?", (eid[tok], low))
                log(f"     side channel: {iid} {name} sections {comp[0]}…({len(comp)}): flows into {hit[1]} (touches it)")
                continue
            try:
                m, snap, _k = join_measure(shapes, stem, dn, hit[2] if hit else None)
            except SideChannelError as e:
                stop(f"{e} ({iid} {name}, sections {comp})")
                continue
            how = (f"touching its own polygon {hit[1]}" if hit else
                   f"from its {'downstream' if len(leave) == 1 else 'nearer'} end")
            log(f"     side channel: {iid} {name} sections {comp[0]}…({len(comp)}): joins at {m / 1000:.1f} km, "
                f"snap {snap:.0f} m {how}")
            if snap > MAX_SNAP_M and not hit:
                stop(f"side channel: {iid} ({name}) sections {comp}: its joining end is {snap:.0f} m from "
                     f"the main line, beyond {MAX_SNAP_M:.0f} m — no join to say (T1140: never a guess)")
                continue
            db.executemany("UPDATE section_span SET lo_m = ?, hi_m = ? WHERE sid = ?",
                           [(round(m), round(m), s) for s in comp])
            filled += len(comp)
    log(f"     side channel: {filled} sections given the join their flow does not say")
    # 2. inside an area
    named = own_name_rows(db, added_stream_names() if added is None else added)
    db.executemany("INSERT INTO part_own_name (ord, part_ix, name, label) VALUES (?,?,?,?)", named)
    cov.filled("part_own_name", len(named))
    rows = inside_rows(db, geo, build_dir, span)
    db.executemany("INSERT INTO part_inside (ord, part_ix, area, share, how) VALUES (?,?,?,?,?)", rows)
    cov.filled("part_inside", len(rows))


def added_stream_names(path: Optional[Path] = None) -> Dict[int, str]:
    """{blk: the added stream's own name} from the promoted added-streams file (`CURATED.waters.added_streams`)."""
    import json
    if path is None:
        from pipeline.common.curated import CURATED
        path = CURATED.waters.added_streams
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    return {int(r["blk"]): str(r["name"]).strip() for r in data.get("streams") or [] if str(r.get("name") or "").strip()}


def own_name_rows(db: sqlite3.Connection, names: Dict[int, str]) -> List[tuple]:
    """(ord, part_ix, name, label) for every LISTED part made only of added-stream lines that carry one own name, not the
    water's, with its source's words (`name_source`). A part with a section that is no added line, two names, a line with
    no name, or no source words for the name: no row."""
    item = {o: (i, n) for o, i, n in db.execute("SELECT ord, item_id, name FROM item")}
    blk: Dict[int, set] = defaultdict(set)
    for sid, b in db.execute("SELECT sid, blk FROM section_measure"):
        blk[sid].add(int(b))
    label = {(i, n): lb for i, n, lb in db.execute("SELECT item_id, name, label FROM name_source")}
    parts: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    for o, s, p in db.execute("SELECT ps.ord, ps.sid, ps.part_ix FROM part_section ps JOIN part pt ON pt.ord = ps.ord "
                              "AND pt.part_ix = ps.part_ix WHERE pt.tidal = 0 AND pt.key_ix IS NOT NULL "
                              "ORDER BY ps.ord, ps.part_ix, ps.sid"):
        parts[(o, p)].append(s)
    out = []
    for (o, p), ss in sorted(parts.items()):
        bs = [b for s in ss for b in blk.get(s, ())]
        if not ss or any(not blk.get(s) for s in ss) or any(b >= 0 for b in bs):
            continue
        own = {names.get(b) for b in bs}
        if len(own) != 1 or None in own:
            continue
        nm = own.pop()
        iid, water = item[o]
        if nm.casefold() == water.casefold() or (iid, nm) not in label:
            continue
        out.append((o, p, nm, label[(iid, nm)]))
    return out


def inside_rows(db: sqlite3.Connection, geo: Dict[int, object], build_dir: Path,
                span: Dict[int, tuple]) -> List[tuple]:
    """(ord, part_ix, the area's catalogue name, the lines' share, "inside" | "at") for every stream part with no main
    stem lying at least `INSIDE_SHARE` in one cut area ("inside"), or its lines so while a polygon section of it straddles
    the area's edge ("at") — every section measured (a section with no shape: neither)."""
    import pyogrio
    from shapely import wkb
    from shapely.ops import unary_union
    kind = dict(db.execute("SELECT ord, kind FROM item"))
    sids: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    # a LISTED part only: keyed (in B.C., with rules) and not a tidal piece (never on the map, never named)
    for o, s, p in db.execute("SELECT ps.ord, ps.sid, ps.part_ix FROM part_section ps JOIN part pt ON pt.ord = ps.ord "
                              "AND pt.part_ix = ps.part_ix WHERE pt.tidal = 0 AND pt.key_ix IS NOT NULL "
                              "ORDER BY ps.ord, ps.part_ix, ps.sid"):
        sids[(o, p)].append(s)
    want = {k: ss for k, ss in sids.items() if kind.get(k[0]) == "stream"
            and all(s in span and span[s][2] == 1 for s in ss) and all(s in geo for s in ss)}
    if not want:
        return []
    cat = Path(build_dir) / "area_catalog.gpkg"
    if not cat.exists():
        raise SideChannelError(f"part_inside: {cat} not found — an atlas build writes it")
    df = pyogrio.read_dataframe(cat, layer="areas", where="cut = 1")
    areas = [(n, g) for n, k, g in zip(df["name"], df["kind"], df.geometry) if k not in NOT_A_PLACE and g is not None]
    from shapely.strtree import STRtree
    tree = STRtree([g for _, g in areas])
    out = []
    for (o, p), ss in sorted(want.items()):
        shapes = [geo[s] for s in ss]
        whole = unary_union(shapes)
        hits = []
        for i in sorted(tree.query(whole)):
            name, poly = areas[int(i)]
            lines = [g for g in shapes if g.geom_type in ("LineString", "MultiLineString")]
            polys = [g for g in shapes if g.geom_type in ("Polygon", "MultiPolygon")]
            share_l = (sum(g.intersection(poly).length for g in lines) / sum(g.length for g in lines)) if lines else 1.0
            share_a = (sum(g.intersection(poly).area for g in polys) / sum(g.area for g in polys)) if polys else 1.0
            if share_l < INSIDE_SHARE:
                continue
            if share_a >= INSIDE_SHARE:
                hits.append((round(share_l, 4), name, "inside"))
            elif lines and all(g.intersection(poly).area > 0 for g in polys):
                hits.append((round(share_l, 4), name, "at"))       # every polygon in it, not all of it: straddling
        if len(hits) == 1:
            out.append((o, p, hits[0][1], hits[0][0], hits[0][2]))
    return out
