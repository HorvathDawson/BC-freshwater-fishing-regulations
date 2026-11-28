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
   mouth's; (T1172, owner 2026-11-26) a main line of ONE section has neither: its mouth is its FIRST VERTEX, FWA's own
   order — measure 0, the downstream end, which the atlas keeps (`blk_chains.build_blk_chains` sorts a blue line's
   pieces by `down_m`, `cutting.merge_ordered` never reverses one, and the graph's flow is read from that order,
   `cutting.blk_endpoints`) — never a flow pointer (`section_down` is the gauges' route pointer and lands mid-line: the
   Old Kootenay River Channel's at 588 m); one drawn in pieces that do not join has no first vertex to say it: NO
   JOIN). THE ATLAS'S ORDER IS CHECKED ON EVERY BUILD (`check_stem_order`): every stem section with a stem section
   below it starts at that section, and a stem section with none below it agrees with the FWA measures of the
   tributaries the graph joins to it (`stem_marks`); either drawn backwards stops the build, named. What this catches
   is the ATLAS turning a section round (a merge, a cut); it cannot catch FWA itself drawing a blue line from its
   source, since the graph's junctions are read from the same vertex order. A section with no junction that tells the
   two readings apart is TRUSTED, counted in the log, never stopped (2026-11-26: 2,509 lowest pieces; of the
   one-section main lines with side channels, 5 — none measured by this module today). (T1172, owner 2026-11-26: "Fix
   the measure in this PR") a channel whose downstream end lies beyond `MAX_SNAP_M` of its main line while another end
   lies on it LEAVES the main line there, and is measured there (`join_measure`, "leaves").
   (T1140 proposal, coordinator 2026-11-25, "a consistent way to handle this") A channel whose downstream end
   TOUCHES a water body of its own river joins the river AT it: a named lake its main line runs through names the
   channel's lower end (only where its flow pointer says that end is the downstream one: never a lake it leaves) (`section_span.lo` = `lake_inlet:<lake>`: "side channel into <Lake>"); its own polygon (a wide
   reach) or an unnamed lake its main line runs through is measured at the main line's point nearest the touch,
   whatever the distance
   — the snap limit holds only where the end touches NO water body of its river. A channel failing one gets NO join
   (never a guessed distance), logged here; (T1172) the answers step logs, by name, a part said by its label with such a
   channel (`builders.unmeasured_side_parts`), and stops on one whose label needs its distance (`builders.untold_side_parts`).
   (Coordinator's proposal 2026-11-25) one beyond the snap limit that touches no water body still gets the main line's
   NEAREST point (`section_join.how` = 'nearest'). Written as the
   join of every section of that channel (`section_join`): the export's runs carry it (`km_from`), which orders the
   part in its river; (T1172, owner 2026-11-26: "Distance only to tell parts apart", "No verb for a leaving channel")
   its label says "N km from the mouth", with no verb, only where its water has more than one such part
   (`display.part.runs_label`). Never into `section_span`: the parts were
   numbered in river order from the flow's own measures (`derived.part_rows`, asserted by `topology.part_geo`), and only
   the export's runs read the join (`export.read.with_joins`).
2. WHICH AREA A PART LIES INSIDE, OR AT (`part_inside`): a part with no main stem whose every section is measured and
   at least `INSIDE_SHARE` of it lies in one cut area of the atlas's catalogue (`area_catalog.gpkg`, `cut` = 1: the
   areas the atlas cuts streams at — reserves, national parks, restricted land; never a region, an MU group or a sign
   zone): its lines by length, its polygon sections by area. `answers.display` names it "<water> inside <area>". Its
   lines that far inside while a polygon section of it straddles the area's edge (in it, but not that far): "<water> at
   <area>" (owner 2026-11-25, the Okanagan River Oxbows at Hayne's Lease Ecological Reserve). (T1172, owner 2026-11-26:
   "Yes, but name it by the area") an area the atlas does NOT cut at (`cut` = 0: it binds whole sections by overlap)
   is a place too where it is a park, a wildlife management area, an ecological reserve or a national park
   (`MEMBERSHIP_PLACES`; never Indigenous land, permit land or a watershed). (T1172, owner 2026-11-26: "The area shows
   in the app SUBTITLE, universally") EVERY part is measured against EVERY such area, reviewed or not: this module
   says the fact (with `cut`, whether the atlas cuts at the area), the answers step decides the words — the subtitle
   says the area always, the NAME only for a reviewed pair (`answers.display.part.REVIEWED_INSIDE`: an unreviewed cut
   area stops the build there, an unreviewed membership-only one is listed and the name takes its next rung). The Old
   Kootenay River Channel's side part, 99.4 % inside the Creston Valley Wildlife Management Area (whose two rules and
   permit are all that tell it from its main line).
   (T1199, RULINGS E30; owner 2026-11-26: "be intelligent about the split then where the river is cut will define the
   section that falls inside the area when cut happens it gets inside area") IN A CLEAN-CUT AREA THE CUT SAYS IT (a
   national park, an ecological reserve, the Chilkoot: `cut_mode` "clean"): a line section is inside when the cut puts it there (the registry's area item holds it,
   `cut_area_members`), never by its shape's share — E30's 100 m floor grows a short inside piece outward (the Gordon
   River's 47 m in Pacific Rim to 100 m, 46 % of it inside the polygon), and the piece is the area's water all the
   same. Polygon sections (the cut never cuts a lake), `first_last` areas (restricted land: a whole piece is in it when
   any of it is) and membership-only areas are measured on their shapes as above.
   A part whose shapes and cut disagree across `INSIDE_SHARE` is logged ("INSIDE BY THE CUT"), never silent.
3. (PROPOSAL, coordinator 2026-11-25, under the owner's "do whatever makes our current state decent until this is fixed
   in hardening") A PART OF ADDED LINES WITH ITS OWN NAME (`part_own_name`): every section an added-stream line (a
   negative blk in `section_measure`), all one added stream's name (the atlas graph's name tuple for it,
   `added_stream_names`), not the water's: named by it, with the words of its source (`name_source.label`). Noons Creek p4 was the City of
   Port Moody's Turner Creek, Seymour River p3 the City of Burnaby's Tunnel Creek, until T1246 gave both a code of
   their own (a watershed-code collision had filed them there). One with no own name gets no row (the answers step
   then says it by its label, the plain count).
"""
from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

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
#: (T1172, owner 2026-11-26) the kinds of a MEMBERSHIP-ONLY area (`cut` = 0) a part is measured against, reviewed or not
#: (the answers step names a part by one only for a reviewed pair; its subtitle says it always): places with a name of
#: their own. Never Indigenous land (`indigenous_land`) without the owner's word, never permit land or a watershed.
MEMBERSHIP_PLACES = ("park", "wma", "ecological_reserves", "national_parks")
#: (T1172) how far (metres) a tributary's junction may lie from where its FWA measure puts it along a stem section, for
#: the junction to say which way the section is drawn (`check_stem_order`); a junction where the two ways lie within
#: twice this of each other says nothing.
MARK_M = 5.0


class SideChannelError(SystemExit):
    """What the rules cannot measure (no area catalogue), or a stem section drawn backwards: never a guessed answer."""


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


def _unnamed_lakes(db: sqlite3.Connection, build_dir: Path) -> Dict[str, object]:
    """{handle: polygon} of every lake outline of the atlas (`waterbody_polys.pkl`) that no named water owns."""
    import pickle
    p = Path(build_dir) / "waterbody_polys.pkl"
    if not p.exists():
        return {}
    from pipeline.common.section_handles import id_at, read as read_handles
    ids, _ = read_handles(build_dir)
    owned = {id_at(ids, s) for (s,) in db.execute("SELECT DISTINCT sid FROM item_section")}
    with p.open("rb") as fh:
        polys = pickle.load(fh)
    return {k: g for k, g in polys.items() if k not in owned}


def downstream_ends(channel: List[object], down: Optional[object]) -> List[object]:
    """The channel's downstream end, as a list of one, where its flow pointer says it (`down`: the shape the flow leaves it
    for); else both ends — which is downstream cannot be told."""
    ends = _ends(channel)
    if down is not None and len(ends) > 1:
        return [min(ends, key=lambda p: (round(p.distance(down), 3), p.x, p.y))]
    return ends


def touched(ends: List[object], own: List[Tuple[int, object]], lakes: List[Tuple[str, object]],
            unnamed: Sequence[Tuple[str, object]] = ()) -> Optional[tuple]:
    """What water body of its river a channel's downstream end touches (within `TOUCH_M`): ("lake", lake item id, the
    end) for a named lake its main line runs through — first, a lake beats the river's own polygon it lies in — else
    ("own", sid, the end) for the river's own polygon section, else ("unnamed", lake handle, the end) for an unnamed lake
    its main line runs through; None when it touches none. Ties: the lowest id, then the end's place."""
    for kind, bodies in (("lake", lakes), ("own", own), ("unnamed", unnamed)):
        hits = sorted(((key, e) for e in ends for key, g in bodies if e.distance(g) <= TOUCH_M),
                      key=lambda h: (h[0], h[1].bounds))
        if hits:
            return kind, hits[0][0], hits[0][1]
    return None


def first_line(g) -> Optional[object]:
    """A stem section's shape as ONE line in its drawn order (its pieces joined head to tail, never turned round:
    `line_merge(directed=True)`), else None — pieces that do not join have no one first vertex."""
    import shapely
    if g.geom_type == "LineString":
        return g
    if g.geom_type != "MultiLineString":
        return None
    m = shapely.line_merge(g, directed=True)
    return m if m.geom_type == "LineString" else None


def stem_marks(db: sqlite3.Connection, graph, ids: Sequence[str]) -> Dict[int, List[Tuple[float, float, float, float]]]:
    """(T1172) {sid: [(the junction's metres from the section's lower end by FWA's measures, the same from its upper end,
    x, y)]} for every stem section of a stream with NO stem section below it (a main line's lowest piece; the whole of a
    one-section main line): each graph edge INTO its piece, whose `at_measure` is FWA's route measure on the piece's own
    blue line (`FlowEdge`), and whose point is where the joining line meets it. Read while the graph is loaded; the
    measures come from FWA's attributes, not from the order the shape is drawn in, so `check_stem_order` can test that
    order against them. A section that is no graph piece (an added city line) has no row."""
    from pipeline.common.section_handles import id_at
    kind = dict(db.execute("SELECT ord, kind FROM item"))
    span = {s: (a, b) for s, a, b in db.execute(
        "SELECT sid, lo_m, hi_m FROM section_span WHERE off_stem = 0 AND shape = 0 AND lo_m IS NOT NULL")}
    stems: Dict[int, List[int]] = defaultdict(list)
    for o, s in db.execute("SELECT ord, sid FROM item_section ORDER BY ord, sid"):
        if kind.get(o) == "stream" and s in span:
            stems[o].append(s)
    out: Dict[int, List[Tuple[float, float, float, float]]] = {}
    for o, ss in stems.items():
        his = {span[s][1] for s in ss}
        for s in ss:
            if span[s][0] in his or s < 1 or s > len(ids):
                continue                          # a stem section below it says its order; or no graph piece
            nid = id_at(ids, s)
            node = graph.nodes.get(nid)
            if node is None or getattr(node, "down_m", None) is None or getattr(node, "up_m", None) is None:
                continue
            got = []
            for ix in graph.up_adj.get(nid, ()):
                e = graph.edges[ix]
                if e.x or e.y:
                    got.append((float(e.at_measure) - node.down_m, node.up_m - float(e.at_measure),
                                float(e.x), float(e.y)))
            out[s] = sorted(got)
    return out


def check_stem_order(db: sqlite3.Connection, geo: Dict[int, object], marks: Dict[int, list], *, log=print) -> dict:
    """(T1172) THE ORDER A MAIN LINE IS DRAWN IN, CHECKED, never trusted — `join_measure` reads a one-section main
    line's mouth from its first vertex. Every stem section (`off_stem` 0, a line, with a span) of every stream: one with
    a stem section below it must START there (its first vertex within `TOUCH_M` of that section); one with none below
    it is held to the FWA measures of the tributaries joined to it (`stem_marks`): a junction is said `MARK_M` from
    where its measure puts it counted from the first vertex — or, drawn backwards, from the last. Any section drawn
    backwards stops the build, every one named. A section neither test can reach (no stem section below, no junction
    whose two readings differ) is counted, never guessed. Returns the counts."""
    from shapely.geometry import Point
    item = {o: (i, n, k) for o, i, n, k in db.execute("SELECT ord, item_id, name, kind FROM item")}
    span = {s: (a, b) for s, a, b in db.execute(
        "SELECT sid, lo_m, hi_m FROM section_span WHERE off_stem = 0 AND shape = 0 AND lo_m IS NOT NULL")}
    stems: Dict[int, List[int]] = defaultdict(list)
    for o, s in db.execute("SELECT ord, sid FROM item_section ORDER BY ord, sid"):
        if item[o][2] == "stream" and s in span and s in geo:
            stems[o].append(s)
    n = {"below": 0, "marks": 0, "unchecked": 0, "pieces": 0}
    bad: List[str] = []
    for o, ss in sorted(stems.items()):
        iid, name, _k = item[o]
        by_hi: Dict[int, List[int]] = defaultdict(list)
        for s in ss:
            by_hi[span[s][1]].append(s)
        for s in ss:
            line = first_line(geo[s])
            if line is None:
                n["pieces"] += 1                  # no one first vertex: `join_measure` refuses it (NO JOIN)
                continue
            p0, p1 = Point(line.coords[0]), Point(line.coords[-1])
            below = sorted(b for b in by_hi.get(span[s][0], ()) if b != s)
            if below:
                d0 = min(geo[b].distance(p0) for b in below)
                if d0 > TOUCH_M:
                    d1 = min(geo[b].distance(p1) for b in below)
                    bad.append(f"{iid} ({name}) section {s}: its first vertex is {d0:.0f} m from the stem section below "
                               f"it {below} (its last {d1:.0f} m)")
                else:
                    n["below"] += 1
                continue
            fwd = bwd = 0
            for m_lo, m_hi, x, y in marks.get(s, ()):
                if abs(m_lo - m_hi) <= 2 * MARK_M:
                    continue                      # mid-line: both readings agree, it says nothing
                t = line.project(Point(x, y))
                if abs(t - m_lo) <= MARK_M:
                    fwd += 1
                elif abs(t - m_hi) <= MARK_M:
                    bwd += 1
                else:
                    n["odd"] = n.get("odd", 0) + 1    # off its own measure either way: it says nothing (counted)
            # ANY junction read backwards stops it, however many read forwards: a majority rule could let a truly
            # turned section through on coincidental forward readings (a junction near the middle, a second line meeting
            # it at a mirrored point), and on 2026-11-26 no section of the province has even one backwards reading
            if bwd > 0:
                bad.append(f"{iid} ({name}) section {s}: drawn backwards — {bwd} junction(s) lie where their FWA measure "
                           f"puts them counted from its LAST vertex ({fwd} from its first)")
            elif fwd:
                n["marks"] += 1
            else:
                n["unchecked"] += 1
    log(f"     stem order: {n['below']} stem sections start at the section below them; {n['marks']} with none below agree "
        f"with their junctions' FWA measures; {n['unchecked']} with none below and no junction to say (FWA's order "
        f"taken); {n['pieces']} in pieces that do not join (never oriented); {n.get('odd', 0)} junctions off their "
        f"measure either way; {len(bad)} drawn backwards")
    if bad:
        raise SideChannelError("side channel: stem sections drawn backwards — a main line's mouth is its first vertex "
                               "only while the atlas keeps FWA's order (T1172):\n  " + "\n  ".join(bad))
    return n


def join_measure(channel: List[object], stem: List[Tuple[int, int, object]],
                 down: Optional[object] = None, end: Optional[object] = None) -> Tuple[float, float, int, str]:
    """(the join's metres from the mouth along the main line, the snap in metres, the stem section it lands on, which
    end was snapped: "given" | "downstream" | "leaves" | "nearer") for a channel (its sections' shapes) against its
    water's main line (`stem`: [(lo_m, hi_m, shape)] of its stem sections). The end of the channel nearer the main
    line, snapped to the nearest stem section, placed between that section's measures from the end its downstream
    neighbour (else upstream neighbour) touches; (T1172) a main line of ONE section from its first vertex, FWA's
    measure 0 (`check_stem_order` checks that order on every build); one of pieces that do not join into one line has
    no first vertex to say it: `SideChannelError` (NO JOIN). Ties go to the lower measure. `down`: the shape the
    channel's flow leaves it for — its end nearest that is its downstream end, the one snapped — unless (T1172, owner
    2026-11-26: "Fix the measure in this PR") that end lies beyond `MAX_SNAP_M` of the main line while another end lies
    within it: the channel LEAVES the main line there (a distributary: the Old Kootenay River Channel's into Leach
    Lake, off the line at 588 m), and that end is the one measured ("leaves"); `end`: the end to snap, given (the end
    that touches a water body of the river)."""
    from shapely.ops import nearest_points
    best = None
    ends = [end] if end is not None else _ends(channel)
    which = "given" if end is not None else "nearer"
    if end is None and down is not None and len(ends) > 1:
        low = min(ends, key=lambda p: (round(p.distance(down), 3), p.x, p.y))
        off = lambda p: min(g.distance(p) for _lo, _hi, g in stem)          # noqa: E731
        on_line = [p for p in ends if p is not low and off(p) <= MAX_SNAP_M]
        if off(low) > MAX_SNAP_M and on_line:
            ends, which = on_line, "leaves"
        else:
            ends, which = [low], "downstream"
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
    # above it; (T1172) a stem of one section: its first vertex (FWA's measure 0)
    below = [s for s in stem if s[1] <= lo + 1 and s[2] is not g]
    above = [s for s in stem if s[0] >= hi - 1 and s[2] is not g]
    from shapely.geometry import Point
    if below or above:
        line = g if g.geom_type == "LineString" else max(getattr(g, "geoms", [g]), key=lambda x: x.length)
    else:
        line = first_line(g)
        if line is None:
            raise SideChannelError("side channel: its water's main line is one section drawn in pieces that do not join "
                                   "— it has no first vertex to say which end is the mouth's")
    p0, p1 = Point(line.coords[0]), Point(line.coords[-1])
    if below:
        ref = min(below, key=lambda s: (s[2].distance(p0) + s[2].distance(p1), s[0]))[2]
        down0 = ref.distance(p0) <= ref.distance(p1)
    elif above:
        ref = min(above, key=lambda s: (s[2].distance(p0) + s[2].distance(p1), s[0]))[2]
        down0 = ref.distance(p0) > ref.distance(p1)
    else:
        down0 = True                    # FWA draws a blue line from its mouth: the first vertex is measure 0
    t = line.project(on) / line.length if line.length else 0.0
    frac = t if down0 else 1.0 - t
    return lo + (hi - lo) * frac, float(e.distance(g)), k, which


def write(db: sqlite3.Connection, geo: Dict[int, object], build_dir: Path, cov, *, log=print,
          problems: Optional[List[str]] = None, added: Optional[Dict[int, str]] = None,
          unnamed: Optional[Dict[str, object]] = None, marks: Optional[Dict[int, list]] = None,
          held: Optional[Dict[str, frozenset]] = None, follow: Optional[frozenset] = None) -> None:
    """Fill the join measures `spans.compute` could not walk to, and write `part_inside` (the module's two rules).
    A channel the join rule cannot say is left without one and logged ("no join"; `problems` collects them too): only
    a part whose name needs that distance stops the build (`answers.display.builders._names`). First the order every
    main line is drawn in is checked (`check_stem_order`, against `marks`: `stem_marks`, read from the graph)."""
    def stop(msg: str) -> None:
        if problems is not None:
            problems.append(msg)
        log(f"     side channel NO JOIN: {msg}")
    if added is None:
        raise SideChannelError("side_channels.write: the added streams' own names are not given (`added_stream_names`)")
    if marks is None:
        raise SideChannelError("side_channels.write: the stems' junction measures are not given (`stem_marks`)")
    if held is None or follow is None:
        raise SideChannelError("side_channels.write: the sections each cut area holds are not given (`cut_area_members`)")
    check_stem_order(db, geo, marks, log=log)
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
    # the UNNAMED lakes (no water owns them: `waterbody_polys.pkl`, keyed by handle), for the touch rule
    if unnamed is None:
        unnamed = _unnamed_lakes(db, build_dir)
    un_keys = sorted(unnamed)
    un_tree = None
    if un_keys:
        from shapely.strtree import STRtree
        un_tree = STRtree([unnamed[k] for k in un_keys])
    for o in sorted(secs):
        iid, name, kind = item[o]
        if kind != "stream":
            continue
        loose = [s for s in secs[o] if s in stemless and span[s][0] is None]
        if not loose:
            continue
        stem_sids = [s for s in secs[o]
                     if s in span and span[s][2] == 0 and span[s][3] == 0 and span[s][0] is not None and s in geo]
        stem = sorted((span[s][0], span[s][1], geo[s]) for s in stem_sids)
        if not stem:
            continue
        if len(stem_sids) == 1:
            # (T1172) its mouth is its first vertex; said in the log how far that was checked (`check_stem_order`)
            n_marks = len(marks.get(stem_sids[0], ()))
            log(f"     side channel: {iid} {name}: main line of one section ({stem_sids[0]}): its mouth is its first "
                f"vertex (FWA's order; {n_marks} junction measure(s) on it for the stem-order check)")
        # this river's water bodies: its own polygon sections, and the named lakes its main line runs through
        own = [(s, geo[s]) for s in secs[o] if s in span and span[s][3] == 1 and s in geo]
        lakes: List[Tuple[str, object]] = []
        if lake_tree is not None:
            from shapely.ops import unary_union
            line = unary_union([g for _a, _b, g in stem])
            lakes = sorted((lake_items[int(j)], lake_shape[lake_items[int(j)]]) for j in lake_tree.query(line)
                           if lake_shape[lake_items[int(j)]].distance(line) <= TOUCH_M)
        on_stem: List[Tuple[str, object]] = []
        if un_tree is not None:
            from shapely.ops import unary_union
            line = unary_union([g for _a, _b, g in stem])
            on_stem = sorted((un_keys[int(j)], unnamed[un_keys[int(j)]]) for j in un_tree.query(line)
                             if unnamed[un_keys[int(j)]].distance(line) <= TOUCH_M)
        for comp in _components(loose, touch):
            shapes = [geo[s] for s in comp if s in geo]
            if len(shapes) != len(comp):
                stop(f"side channel: {iid} ({name}) sections {comp}: some have no shape in this atlas")
                continue
            inside = set(comp)
            leave = sorted({down[s] for s in comp if s in down and down[s] not in inside and down[s] in geo})
            dn = geo[leave[0]] if len(leave) == 1 else None
            # a named lake names the channel's lower end only where its flow pointer says which end is downstream
            # (audit of #265: with no pointer the touching end may be where it LEAVES the lake: Robertson Creek's channel
            # runs up to the Boot Lagoon outlet, so "into Boot Lagoon" would be backwards)
            hit = touched(downstream_ends(shapes, dn), [x for x in own if x[0] not in inside],
                          lakes if dn is not None else [], on_stem)
            if hit and hit[0] == "lake":
                # into a named lake on its river's main line: that is its lower end (rule A names it)
                # on the channel's lowest piece, whose lower end its run says (`spans.compose_runs`)
                db.execute("INSERT INTO section_join (sid, m, lo, how) VALUES (?, NULL, ?, NULL)",
                           (comp[0], f"lake_inlet:{hit[1]}"))
                log(f"     side channel: {iid} {name} sections {comp[0]}…({len(comp)}): flows into {hit[1]} (touches it)")
                continue
            try:
                m, snap, _k, which = join_measure(shapes, stem, dn, hit[2] if hit else None)
            except SideChannelError as e:
                stop(f"{e} ({iid} {name}, sections {comp})")
                continue
            how = (f"touching {'its own polygon' if hit[0] == 'own' else 'an unnamed lake on its main line'} {hit[1]}"
                   if hit else
                   "where it leaves the main line (its downstream end lies away from it)" if which == "leaves" else
                   f"from its {which} end")
            log(f"     side channel: {iid} {name} sections {comp[0]}…({len(comp)}): joins at {m / 1000:.1f} km, "
                f"snap {snap:.0f} m {how}")
            how = "join"
            if snap > MAX_SNAP_M and not hit:
                # (coordinator's proposal 2026-11-25, under "They should always have the cut names or distance it is
                # impossible not to") no join to say: its NEAREST point on the main line
                how = "nearest"
                log(f"     side channel NEAREST: {iid} ({name}) sections {comp}: its end is {snap:.0f} m from the main line, "
                    f"beyond {MAX_SNAP_M:.0f} m and touching no water body of it: said {m / 1000:.1f} km from the mouth")
            db.executemany("INSERT INTO section_join (sid, m, lo, how) VALUES (?, ?, NULL, ?)",
                           [(s, round(m), how) for s in comp])
            filled += len(comp)
    log(f"     side channel: {filled} sections given the join their flow does not say")
    # 2. inside an area
    named = own_name_rows(db, added)
    db.executemany("INSERT INTO part_own_name (ord, part_ix, name, label) VALUES (?,?,?,?)", named)
    cov.filled("part_own_name", len(named))
    rows = inside_rows(db, geo, build_dir, span, held, follow, log=log)
    db.executemany("INSERT INTO part_inside (ord, part_ix, area, share, how, cut) VALUES (?,?,?,?,?,?)", rows)
    cov.filled("part_inside", len(rows))


def added_stream_names(graph) -> Dict[int, str]:
    """{blk: the added stream's own name} from the ATLAS GRAPH (the bundle reads no curated waters file): an added line's
    node (a negative blk) carries the name its city layer gives it as a name tuple noted "added…" (the atlas's ingest of
    `added_streams.json`), beside the name of the water it was filed under."""
    out: Dict[int, str] = {}
    for node in graph.nodes.values():
        b = str(getattr(node, "blk", "") or "")
        if not b.startswith("-"):
            continue
        own = sorted({t.name.strip() for t in getattr(node, "name_tuples", ()) or ()
                      if str(t.note or "").startswith("added") and str(t.name or "").strip()})
        if len(own) == 1:
            out[int(b)] = own[0]
    return out


_MUNICIPALITY = re.compile(r"^(?:City|District|Town|Township|Village|Municipality) of (.+?)(?: (?:ESA )?(?:streams?|waterways|"
                           r"watercourses?|creeks?)\b.*)?$")


def municipality(label: str) -> Optional[str]:
    """The municipality a city layer's label names ("City of Port Moody ESA streams layer" → "Port Moody"), else None."""
    m = _MUNICIPALITY.match(label.strip())
    return m.group(1).strip() if m else None


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
        place = municipality(label[(iid, nm)])
        if place is None:
            continue
        out.append((o, p, nm, place))
    return out


def cut_area_members(registry_items: Sequence[dict], handle_by_id, build_dir: Path, area_defs: Sequence[dict], *,
                     log=print) -> Tuple[Dict[str, frozenset], frozenset]:
    """(T1199, E30) `(held, follow)`: `held` {area id: the section handles its registry area item holds} for every cut
    area that is a place (`area_catalog.gpkg`'s `area_id`, `cut` = 1, not `NOT_A_PLACE`); `follow` the ids of those the
    CLEAN cutter cuts (`area_splits.cut_mode` "clean": national parks, ecological reserves, the Chilkoot — the T1199
    rule's inside intervals), whose parts are measured by the cut. A `first_last` area (`membership: both_sides`,
    restricted land) puts a WHOLE piece in itself when any real length lies in its polygon: measured on the shapes as
    before (`inside_rows` logs where the two disagree). The registry makes an area item only where water lies in the
    area: a cut place with none holds nothing, named and counted in the log. An area section with no handle stops."""
    import pyogrio
    from pipeline.atlas.splits.area_splits import cut_mode
    cat = Path(build_dir) / "area_catalog.gpkg"
    if not cat.exists():
        raise SideChannelError(f"cut_area_members: {cat} not found — an atlas build writes it")
    df = pyogrio.read_dataframe(cat, layer="areas", read_geometry=False)
    want = {a: (n, k) for a, n, k, c in zip(df["area_id"], df["name"], df["kind"], df["cut"])
            if bool(c) and k not in NOT_A_PLACE}
    clean_kinds = {ad.get("kind", ad["id"]) for ad in area_defs if cut_mode(ad) == "clean"}
    follow = frozenset(a for a, (_n, k) in want.items() if k in clean_kinds)
    held: Dict[str, frozenset] = {a: frozenset() for a in want}
    for it in registry_items:
        if it.get("id") in want:
            missing = [s for s in it.get("section_ids") or () if s not in handle_by_id]
            if missing:
                raise SideChannelError(f"cut_area_members: {it['id']}: {len(missing)} section(s) not in the handle table "
                                       f"({missing[:5]})")
            held[it["id"]] = frozenset(handle_by_id[s] for s in it.get("section_ids") or ())
    none = sorted(a for a in want if not held[a])
    log(f"     side channel areas: {len(want)} cut places, {len(follow)} cut clean (measured by the cut); {len(none)} "
        f"hold no water (no registry item): {'; '.join(f'{want[a][0]} ({want[a][1]})' for a in none)}")
    return held, follow


def cut_area_holds(registry_items: Sequence[dict], handle_by_id, build_dir: Path) -> Dict[str, frozenset]:
    """(T1199 PR 2) {area name: the section handles its registry area item holds} for EVERY area the atlas cuts at whose
    edge a part's ends can name (`area:<name>` tokens: parks, reserves, the Chilkoot, restricted land, MU groups, sign
    zones; never a region, whose lines are `region_line:` tokens). Two catalogue rows of one name are one area (their
    sections together). A cut area with no item holds nothing; an area section with no handle stops the build."""
    import pyogrio
    cat = Path(build_dir) / "area_catalog.gpkg"
    if not cat.exists():
        raise SideChannelError(f"cut_area_holds: {cat} not found — an atlas build writes it")
    df = pyogrio.read_dataframe(cat, layer="areas", read_geometry=False)
    name_of = {a: n for a, n, k, c in zip(df["area_id"], df["name"], df["kind"], df["cut"]) if bool(c) and k != "region"}
    out: Dict[str, set] = {n: set() for n in name_of.values()}
    for it in registry_items:
        if it.get("id") in name_of:
            missing = [s for s in it.get("section_ids") or () if s not in handle_by_id]
            if missing:
                raise SideChannelError(f"cut_area_holds: {it['id']}: {len(missing)} section(s) not in the handle table "
                                       f"({missing[:5]})")
            out[name_of[it["id"]]].update(handle_by_id[s] for s in it.get("section_ids") or ())
    return {n: frozenset(s) for n, s in out.items()}


def area_held_rows(db: sqlite3.Connection, holds: Dict[str, frozenset]) -> List[tuple]:
    """(T1199 PR 2) (ord, part_ix, area name) for every LISTED part every section of which the area's item holds: the
    cut areas whose rule the part carries (`part_area_held`, read by the answers step's end words: a stretch between two
    crossings of an area is "within" it only then)."""
    by_sec: Dict[int, set] = defaultdict(set)
    for n, ss in holds.items():
        for s in ss:
            by_sec[s].add(n)
    parts: Dict[Tuple[int, int], List[int]] = defaultdict(list)
    for o, s, p in db.execute("SELECT ps.ord, ps.sid, ps.part_ix FROM part_section ps JOIN part pt ON pt.ord = ps.ord "
                              "AND pt.part_ix = ps.part_ix WHERE pt.key_ix IS NOT NULL ORDER BY ps.ord, ps.part_ix"):
        parts[(o, p)].append(s)
    out = []
    for (o, p), ss in sorted(parts.items()):
        common = set.intersection(*(by_sec.get(s, set()) for s in ss)) if ss else set()
        out += [(o, p, n) for n in sorted(common)]
    return out


def write_area_held(db: sqlite3.Connection, holds: Dict[str, frozenset], cov) -> None:
    """`part_area_held`, and `cut_area`: the VOCABULARY, every cut area a part's end can name (`cut_area_holds`' keys,
    holding water or not), against which the answers step refuses an end token naming an area it does not know."""
    rows = area_held_rows(db, holds)
    db.executemany("INSERT INTO part_area_held (ord, part_ix, area) VALUES (?,?,?)", rows)
    cov.filled("part_area_held", len(rows))
    db.executemany("INSERT INTO cut_area (area) VALUES (?)", [(n,) for n in sorted(holds)])
    cov.filled("cut_area", len(holds))


def inside_rows(db: sqlite3.Connection, geo: Dict[int, object], build_dir: Path,
                span: Dict[int, tuple], held: Dict[str, frozenset], follow: frozenset, *, log=print) -> List[tuple]:
    """(ord, part_ix, the area's catalogue name, the lines' share, "inside" | "at", 1 if the atlas cuts at the area else
    0) for every stream part with no main stem lying at least `INSIDE_SHARE` in one area ("inside"), or its lines so
    while a polygon section of it straddles the area's edge ("at") — every section measured (a section with no shape:
    neither). The areas, reviewed or not (T1172): every cut area that is a place (not `NOT_A_PLACE`), and every
    membership-only one of the `MEMBERSHIP_PLACES` kinds. ONE AREA: a part in two cut areas, or in no cut area and two
    membership-only ones, has no row (logged); a part in one cut area is said by it, whatever membership-only areas
    hold it too (the atlas's own line before an area it does not cut at). (T1199, E35) A CLEAN-cut area (`follow`)
    says it EXACTLY by the cut: inside when the area's item holds ALL its lines (`held`, `cut_area_members`), never by
    shape; a part the cut puts only partly in such an area, or one it puts there without the area as its row, stops
    the build by name. Every other area by shape. Where the cut and the shapes fall on different sides of
    `INSIDE_SHARE` it is logged, in either kind of area."""
    import pyogrio
    from shapely.ops import unary_union
    kind = dict(db.execute("SELECT ord, kind FROM item"))
    iid_of = dict(db.execute("SELECT ord, item_id FROM item"))
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
    df = pyogrio.read_dataframe(cat, layer="areas")
    # (name, shape, cut) of every place: a cut area not a line; a membership-only area of a named-place kind
    areas = [(n, g, bool(c), a) for n, k, c, g, a in zip(df["name"], df["kind"], df["cut"], df.geometry, df["area_id"])
             if g is not None and ((bool(c) and k not in NOT_A_PLACE) or (not c and k in MEMBERSHIP_PLACES))]
    unheld = sorted({a for _n, _g, c, a in areas if c and a not in held})
    if unheld:
        raise SideChannelError(f"part_inside: {len(unheld)} cut area(s) with no held sections given: {unheld[:5]}")
    if not follow <= set(held):
        raise SideChannelError(f"part_inside: clean-cut area(s) not among the held: {sorted(follow - set(held))[:5]}")
    # (T1199) the cut areas that hold a section, by section: a part the cut puts in an area is measured against it even
    # where its shapes miss the polygon's box
    held_by: Dict[int, List[int]] = defaultdict(list)
    for i, (_n, _g, c, a) in enumerate(areas):
        if c:
            for s in held[a]:
                held_by[s].append(i)
    from shapely.strtree import STRtree
    tree = STRtree([g for _, g, _c, _a in areas])
    out = []
    disagree: List[str] = []
    for (o, p), ss in sorted(want.items()):
        shapes = [geo[s] for s in ss]
        whole = unary_union(shapes)
        hits = []
        clean_holds: set = set()                 # the clean-cut areas whose cut puts any of this part's lines in them
        cand = {int(i) for i in tree.query(whole)} | {i for s in ss for i in held_by.get(s, ())}
        for i in sorted(cand):
            name, poly, cut, aid = areas[i]
            lines = [(s, g) for s, g in zip(ss, shapes) if g.geom_type in ("LineString", "MultiLineString")]
            polys = [g for g in shapes if g.geom_type in ("Polygon", "MultiPolygon")]
            total = sum(g.length for _s, g in lines)
            by_shape = (sum(g.intersection(poly).length for _s, g in lines) / total) if lines else 1.0
            share_l = by_shape
            if cut and lines:
                by_item = sum(g.length for s, g in lines if s in held[aid]) / total
                if aid in follow:
                    # (T1199, E35) exactly: inside when the cut puts ALL of its lines in the area, else not inside;
                    # a part the cut puts partly in it carries the area's rule on some sections only: refused below
                    share_l = 1.0 if by_item >= 1.0 - 1e-9 else 0.0
                    if by_item > 0:
                        clean_holds.add(name)
                        if share_l < 1.0:
                            disagree.append(f"{iid_of[o]} part {p}: the cut puts {by_item:.1%} of its lines in "
                                            f"{name!r}, not all of them")
                if (by_item >= INSIDE_SHARE) != (by_shape >= INSIDE_SHARE):
                    log(f"     side channel INSIDE BY THE CUT: {iid_of[o]} part {p} {name!r}: the cut holds "
                        f"{by_item:.1%} of its lines, their shapes lie {by_shape:.1%} in it — "
                        + ("measured by the cut (E35)" if aid in follow else "a first_last area: measured by its shapes"))
            share_a = (sum(g.intersection(poly).area for g in polys) / sum(g.area for g in polys)) if polys else 1.0
            if share_l < INSIDE_SHARE:
                continue
            if share_a >= INSIDE_SHARE:
                how = "inside"
            elif lines and all(g.intersection(poly).area > 0 for g in polys):
                how = "at"                                         # every polygon in it, not all of it: straddling
            else:
                continue
            hits.append((round(share_l, 4), name, how, int(cut)))
        # (T1172, review L3) ONE AREA is one name: two catalogue rows of one name (an area kept in pieces) are one area
        # — its best row says it ("inside" before "at", then the larger share); two names are two areas
        by_name: Dict[str, tuple] = {}
        for h in hits:
            if h[1] not in by_name or (h[2] == "inside", h[0]) > (by_name[h[1]][2] == "inside", by_name[h[1]][0]):
                by_name[h[1]] = h
        # the atlas's own cut areas first; a membership-only area only where no cut area holds the part
        cut_names = [h for h in by_name.values() if h[3]]
        pick = cut_names if cut_names else list(by_name.values())
        if len(cut_names) == 1 and len(by_name) > 1:
            log(f"     side channel INSIDE: {iid_of[o]} part {p} said by the cut area {cut_names[0][1]!r} over the "
                f"membership-only {sorted(h[1] for h in by_name.values() if not h[3])}")
        if len(pick) == 1:
            share, name, how, cut = pick[0]
            out.append((o, p, name, share, how, cut))
        elif len(pick) > 1:
            log(f"     side channel INSIDE two areas: {iid_of[o]} part {p} in {sorted(h[1] for h in pick)}: no row")
        said = pick[0][1] if len(pick) == 1 else None
        if clean_holds and said not in clean_holds:
            disagree.append(f"{iid_of[o]} part {p}: carries the rule of {sorted(clean_holds)} (the cut puts it there) "
                            f"but is named {('by ' + repr(said)) if said else 'by no area'}")
    if disagree:
        # (T1199, E35) a side-channel part at a clean-cut park or reserve is named with the area exactly when it carries
        # the area's rule: never silent drift between the name and the rule
        raise SideChannelError(f"part_inside: {len(disagree)} part(s) whose name would disagree with their area's rule "
                               f"(E35): " + "; ".join(sorted(disagree)[:10]))
    return out
