"""Drop braid pieces that are pure loops off a single mainstem.

FWA models a braided river as the main blue line plus a blue line per anabranch. Most of those
anabranches leave the mainstem and rejoin the SAME mainstem with nothing else flowing into them —
they carry no tributary, no name, and no regulation can ever refer to them. They are pure complexity:
tens of thousands of pieces province-wide, and they are what makes a reach ambiguous, because "is
this braid above or below the cut" has no answer when it is attached at both ends.

A piece is pruned only when ALL of these hold, so the graph never loses connectivity or a nameable
water:

  * it has no name of its OWN — that is, no name other than the RIVER's. Compared against the river
    (the watershed code's longest blue line), case-insensitively, because FWA carries the gazetted
    name in caps alongside the display name: "FRASER RIVER" is not a second water.

    A NAMED piece is not merely kept, it is not a candidate at all, so it counts as network for
    everything below. That distinction is the whole game on the Fraser at Nicomen Slough: the
    anonymous braids hang off Nicomen Slough, and judging one component containing both would let
    the slough's name immunise every braid attached to it.

    The curated half of that promise needs `protected_blks`. `apply_name_variants` runs LATE in the
    build (after the splits, so freshly cut pieces get named), which is long after this prune — so at
    prune time a curated channel still carries only its host's inherited name and looks anonymous.
    That silently deleted "Seabird Island North Side Channel" (4 blue lines named in
    name_variants.json) before it could ever be named. The caller therefore passes the blks that
    name_variants targets, and they are never pruned.

  * it belongs to a connected component of anonymous pieces that all share the river's WATERSHED
    CODE and none of which is the mainstem blue line — i.e. the river's own braiding, nested
    channels included;
  * the component takes water from the rest of the network AND returns it (never a dead end);
  * nothing from a DIFFERENT watershed code flows into it — a different code means another named
    water discharges there. Pass `reconnect_tributaries` to take those too, re-homing the tributary.

CONNECTIVITY IS ENFORCED, NOT ASSUMED. An earlier version reasoned that a component with an inflow
and an outflow is a "loop off the mainstem", so its upstream always kept a parallel path and nothing
could be orphaned. That is false: an inflow plus an outflow is a PASS-THROUGH, and when the component
is some node's only way downstream, deleting it strands that node. It stranded 147 nodes in a single
pass. So every external inflow edge is now checked against the drop set, and any source that would be
left with no downstream edge at all is re-homed onto the component's exit — alongside the tributary
mouths, which are re-homed because they are named water, not because they would otherwise dangle.

    graph, n, fids = prune_mainstem_loops(graph, geoms, protected_blks=nv_blks(nv))
"""

from __future__ import annotations

import heapq
from dataclasses import replace

from shapely.geometry import Point
from shapely.ops import nearest_points

from pipeline.common.models import NameSource, NameTuple, NodeKind, StreamGraph


def _has_own_name(node, river_name: str) -> bool:
    """True if this piece carries a name of its OWN — anything that is not just the river's.

    `display_name` alone is not enough: a `name_variants` entry attaches a curated name as a name
    TUPLE while the display name stays the mainstem's. Matching is casefolded because FWA stores the
    gazetted name in caps ("FRASER RIVER") next to the display name ("Fraser River") — reading those
    as two different waters made every piece of a big river look individually named, which is exactly
    what stopped the Fraser's braids from ever being pruned.

    An INHERITED name (`side-channel`: the water the piece belongs to, `names.resolve_names`) is never
    its own. Since T497 an unnamed braid off Nicomen Slough inherits "Nicomen Slough" rather than the
    Fraser's name; read as its own, that kept 20 pure braid loops the prune always dropped."""
    rn = (river_name or "").strip().casefold()
    inherited = {(t.name or "").strip().casefold() for t in node.name_tuples
                 if t.source == NameSource.side_channel}
    own = {(t.name or "").strip().casefold() for t in node.name_tuples
           if t.source != NameSource.side_channel}
    d = (node.display_name or "").strip().casefold()
    if d not in ("", rn) and (d in own or d not in inherited):
        return True
    return any(n not in ("", rn) for n in own)


def _mainstem_blk(nodes, fwa_main: frozenset[str] | set[str] | None = None) -> str:
    """The blue line that IS the river (FIX round, user ruling 2026-10-06 — by principle, in order):
    FWA's OWN MAINSTEM first (a blue line whose WATERSHED_KEY is itself, `blk_chains.
    fwa_mainstem_blks`; a braid's side channels carry the river's key, never their own), then the
    one carrying the most length in this watershed code, then the blk id. Without `fwa_main` (a
    fixture with no FWA keys) it is the length rule alone, as before."""
    by_blk: dict[str, float] = {}
    for n in nodes:
        by_blk[n.blk] = by_blk.get(n.blk, 0.0) + (n.length_m or 0.0)
    if not by_blk:
        return ""
    main = fwa_main or ()
    return max(by_blk, key=lambda b: (b in main, by_blk[b], b))


def _river_name(nodes, main: str) -> str:
    """The river's own name: the display name of the longest piece on the mainstem blue line.

    Taken from the mainstem rather than from a piece's upstream neighbour. A braid's neighbour is
    whatever happens to flow in — often a tributary — so comparing against it made a piece carrying
    the plain inherited river name look like it had a name of its own."""
    best = None
    for n in nodes:
        if n.blk == main and (best is None or (n.length_m or 0.0) > (best.length_m or 0.0)):
            best = n
    return (best.display_name or "").strip() if best is not None else ""


def nv_blks(name_variants) -> set[str]:
    """Blue lines that `name_variants` gives a curated name to — the prune must never remove these.

    Only blk targets matter here: a `wbk` target names a lake (never a braid piece), and `gnis`/`wsc`
    targets name water that already carries a gazetted name, which the name test protects."""
    out: set[str] = set()
    for e in name_variants or ():
        t = (e.get("target") or {})
        out.update(str(b) for b in t.get("blks", []))
        if t.get("blk"):
            out.add(str(t["blk"]))
    return out


def loop_nodes(graph: StreamGraph, protected_blks: set[str] | None = None,
               take_tributaries: bool = False, records: list | None = None,
               fwa_main: frozenset[str] | set[str] | None = None) -> set[str]:
    """Node ids of the prunable braiding (see the module docstring for the conditions).

    ``take_tributaries``: also take a braid that a DIFFERENT water flows into, re-homing that water's
    mouth onto the component's exit. Without it such a component is kept — which is what leaves a big
    braided river (the Fraser: 330 anabranches, all unnamed) a hairball.

    ``records`` (optional): the caller's re-homing worklist. Every removed component appends
    ``(component, all external in-edge ids, foreign in-edge ids, exit candidates)``. It is filled
    whether or not ``take_tributaries`` is set, because re-homing is not only about tributaries: a
    plain same-watershed inflow whose only path downstream ran through the component has to be
    re-homed too or it is stranded.

    Works on whole braid COMPONENTS, not one piece at a time. A braid often carries another braid off
    it; the inner one is not attached to the mainstem at all, so a per-piece loop test keeps it, and
    keeping it then keeps the outer one too. Grouping the anonymous same-watershed pieces into
    connected components and judging each as a unit removes the whole nest at once — which is the
    only way to remove it, since half a nest is worse than all of it."""
    from pipeline.common.utils.wsc import trim_wsc

    by_wsc: dict[str, list] = {}
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk and n.wsc:
            by_wsc.setdefault(trim_wsc(n.wsc), []).append(n)

    # Lakes are indexed too. A lake sitting on the river carries the river's code, and leaving it out
    # made "the river flows in through its own lake" read as a foreign water discharging into a braid.
    wsc_of = {n.node_id: trim_wsc(n.wsc) for n in graph.nodes.values() if n.wsc}
    out: set[str] = set()

    protected = protected_blks or set()
    for wsc, nodes in by_wsc.items():
        main = _mainstem_blk(nodes, fwa_main)
        river = _river_name(nodes, main)
        cand = {n.node_id for n in nodes
                if n.blk != main and n.blk not in protected and not _has_own_name(n, river)}
        if not cand:
            continue
        seen: set[str] = set()
        for start in sorted(cand):                   # DETERMINISM: records in node-id order
            if start in seen:
                continue
            comp, stack = set(), [start]
            seen.add(start)
            while stack:
                nid = stack.pop()
                comp.add(nid)
                nbrs = {graph.edges[i].from_node for i in graph.up_adj.get(nid, [])}
                nbrs |= {graph.edges[i].to_node for i in graph.down_adj.get(nid, [])}
                for x in nbrs & cand:
                    if x not in seen:
                        seen.add(x)
                        stack.append(x)
            # judge the component as a unit
            in_edges: list[int] = []
            foreign_edges: list[int] = []
            exits: set[str] = set()
            out_edges: list[int] = []
            for nid in comp:
                for i in graph.up_adj.get(nid, []):
                    src = graph.edges[i].from_node
                    if src in comp:
                        continue
                    in_edges.append(i)                    # takes water from the rest of the network
                    if wsc_of.get(src) != wsc:
                        foreign_edges.append(i)           # a DIFFERENT water discharges into it
                for i in graph.down_adj.get(nid, []):
                    if graph.edges[i].to_node not in comp:
                        exits.add(graph.edges[i].to_node)
                        out_edges.append(i)
            if not (in_edges and exits):
                continue                                  # a dead end: never removable
            if foreign_edges and not take_tributaries:
                continue                                  # would strand a named water
            out |= comp
            if records is not None:
                records.append((frozenset(comp), tuple(in_edges), tuple(foreign_edges),
                                frozenset(exits), tuple(out_edges)))
    return out


def _confluence_point(geoms, graph: StreamGraph, exit_node: str, measure: float):
    """The spot on the exit's blue line where a re-homed water now joins — None without geometry."""
    line = (geoms or {}).get(exit_node)
    node = graph.nodes.get(exit_node)
    if line is None or line.is_empty or node is None or node.down_m is None:
        return None
    if line.geom_type not in ("LineString", "MultiLineString"):
        return None
    return line.interpolate(max(0.0, min(measure - node.down_m, line.length)))


def _gap(geoms, graph: StreamGraph, from_node: str, exit_node: str, measure: float) -> float:
    """How far the re-homed water now sits from the confluence it is given, in metres.

    This is the distance the map will show as a hole, and it is the quantity worth capping. The
    earlier metric compared the OLD target with the NEW one, which reads ~0 whenever a side channel
    touches the river it feeds — true of every braid, so it never fired where it mattered."""
    src = (geoms or {}).get(from_node)
    if src is None or src.is_empty:
        return 0.0
    pt = _confluence_point(geoms, graph, exit_node, measure)
    if pt is None:
        # The exit is a lake (or has no measure to interpolate along). Measuring nothing here would
        # exempt every lake-bound component from the cap entirely, so fall back to the distance to the
        # exit itself — 154 edges land on a lake, and the cap has to mean something for them too.
        exit_geom = (geoms or {}).get(exit_node)
        if exit_geom is None or exit_geom.is_empty:
            return 0.0
        return float(src.distance(exit_geom))
    return float(src.distance(pt))


def _exit_measure(graph: StreamGraph, out_edges, exit_node: str) -> float | None:
    """Where the component itself met this exit — the confluence the removed water actually used.

    This is the "next level down": the water ran source -> component -> exit and entered the exit at a
    real place, so that place is where the re-homed edge belongs. Projecting the source's own mouth
    onto the exit instead lands it at the nearest point of the line, which is a different spot
    whenever the component ran at an angle to the river. Lowest measure wins when a component meets
    one exit more than once: that is where its water finally leaves."""
    ms = [graph.edges[i].at_measure for i in out_edges if graph.edges[i].to_node == exit_node]
    return min(ms) if ms else None


def _measure_on(geoms, graph: StreamGraph, from_node: str, exit_node: str, old: float,
                out_edges=()) -> float:
    """The route measure a re-homed confluence sits at ON ITS NEW BLUE LINE.

    `FlowEdge.at_measure` is a measure on the `to_node`, so moving an edge without recomputing it
    leaves a number belonging to the blue line that was just deleted. Nothing complains — a later
    stage files each incoming edge into the piece whose measure range contains `at_measure`, so a
    stale value quietly parks the confluence on the right river in the wrong place. Maria Slough came
    out attached to the Fraser 43 km below where it actually joins.

    Prefer the measure at which the removed component met this exit; fall back to projecting the
    source's mouth onto the exit, remembering geometry is stored downstream-end first."""
    node = graph.nodes.get(exit_node)
    if node is None or node.kind != NodeKind.stream or node.down_m is None:
        return old                                   # a lake has no measure to speak of
    m = _exit_measure(graph, out_edges, exit_node)
    if m is None:
        line, src = (geoms or {}).get(exit_node), (geoms or {}).get(from_node)
        if line is None or src is None or line.is_empty or src.is_empty:
            return old
        if line.geom_type not in ("LineString", "MultiLineString"):
            return old                               # nothing to project along
        mouth = Point(src.coords[0]) if src.geom_type == "LineString" else nearest_points(src, line)[0]
        m = node.down_m + line.project(mouth)
    return min(max(m, node.down_m), node.up_m if node.up_m is not None else m)


def _joins_already(graph: StreamGraph, source: str, exit_node: str, drop: set[str], skip: int) -> bool:
    """Does `source` already flow, by an edge other than `skip`, into a surviving piece of `exit_node`'s water — the
    exit itself, or another piece of its blue line?"""
    ex = graph.nodes.get(exit_node)
    for j in graph.down_adj.get(source, ()):
        if j == skip:
            continue
        t = graph.edges[j].to_node
        if t in drop:
            continue
        if t == exit_node:
            return True
        n = graph.nodes.get(t)
        if ex is not None and n is not None and ex.kind == NodeKind.stream and n.kind == NodeKind.stream \
                and ex.blk and n.blk == ex.blk:
            return True
    return False


def _moved_point(geoms, graph: StreamGraph, edge, exit_node: str, measure: float) -> tuple[float, float]:
    """Where a re-homed junction now is: the point at `measure` on the exit's line, or (a lake exit) the point of the
    lake nearest the old junction. No geometry for the exit: the old point (nothing to place it on)."""
    pt = _confluence_point(geoms, graph, exit_node, measure)
    if pt is None:
        g = (geoms or {}).get(exit_node)
        if g is None or g.is_empty:
            return edge.x, edge.y
        pt = nearest_points(g.boundary if g.geom_type in ("Polygon", "MultiPolygon") else g, Point(edge.x, edge.y))[0]
    return float(pt.x), float(pt.y)


def _nearest_exit(geoms, graph: StreamGraph, live: list, old_target: str) -> str:
    """The exit to re-home onto: whichever surviving one lies closest to the confluence being moved.

    A component can leave the network at more than one place, and the choice is the whole of the
    error introduced. Ordering the exits by ``down_m`` picked the smallest route measure — but a
    route measure is relative to the exit's OWN blue line, so comparing two exits on two lines
    compares nothing, and a mouth once moved 126 km. Distance to the mouth's current position is the
    quantity actually being minimised, so minimise it directly; without geometry, fall back to a
    stable deterministic pick."""
    if len(live) == 1:
        return live[0]
    here = geoms.get(old_target) if geoms else None
    if here is None or here.is_empty:
        return min(live, key=lambda n: (graph.nodes[n].kind != NodeKind.stream,
                                        getattr(graph.nodes[n], "down_m", 0.0) or 0.0, n))
    def d(n):
        gm = geoms.get(n)
        return float(here.distance(gm)) if gm is not None and not gm.is_empty else float("inf")
    return min(live, key=lambda n: (d(n), n))


#: Two line ends closer than this meet (metres, EPSG:3005). FWA's own junctions coincide exactly.
TOUCH_M = 1.0
#: A closure search gives up after this many pieces (it keeps nothing it did not finish).
_CLOSE_CAP = 5000


def _ends(g):
    """The end points of a piece's line (each part of a multi-line)."""
    for part in getattr(g, "geoms", [g]):
        cs = part.coords
        if len(cs):
            yield Point(cs[0])
            yield Point(cs[-1])


def _nbrs(graph: StreamGraph, nid: str) -> set[str]:
    out = {graph.edges[i].from_node for i in graph.up_adj.get(nid, [])}
    out |= {graph.edges[i].to_node for i in graph.down_adj.get(nid, [])}
    out.discard(nid)
    return out


def dangling_ends(graph: StreamGraph, geoms: dict, drop: set[str], tol: float = TOUCH_M):
    """[(surviving piece, [dropped pieces its end touches], "up" | "down")] — every end of a SURVIVING stream line that
    touches a dropped piece and no surviving line. It is an end the prune made: before it the water went
    on there. A real source or mouth touches no dropped piece and is never listed.

    "No surviving line" is asked of the piece's graph neighbours AND the dropped pieces' neighbours, so a
    sibling channel leaving the same junction counts as meeting it even with no edge between the two.

    "up": the water came IN through this end from a dropped piece (a kept channel's stub); "down": it went
    OUT through it to dropped pieces only (a creek whose channel went)."""
    out = []
    cands = set()
    for d in drop:
        cands |= _nbrs(graph, d)
    for n in sorted(cands - drop):                   # DETERMINISM
        node = graph.nodes.get(n)
        g = geoms.get(n)
        if node is None or node.kind != NodeKind.stream or g is None or g.is_empty:
            continue
        near = _nbrs(graph, n)
        for e in _ends(g):
            hit = sorted(d for d in near & drop
                         if geoms.get(d) is not None and geoms[d].distance(e) < tol)
            if not hit:
                continue
            # the water must have gone ON through this end: in from a dropped piece, or out to one with
            # no other way down. A piece's head at a distributary that leaves it is not that.
            into = {graph.edges[i].from_node for i in graph.up_adj.get(n, [])}
            outs = {graph.edges[i].to_node for i in graph.down_adj.get(n, [])}
            way = "up" if any(d in into for d in hit) else "down" if (outs and outs <= drop) else None
            if way is None:
                continue
            around = set(near)
            for d in hit:
                around |= _nbrs(graph, d)
            around -= drop
            around.discard(n)
            if any(geoms.get(v) is not None and not geoms[v].is_empty and geoms[v].distance(e) < tol
                   for v in around):
                continue
            out.append((n, [d for d in hit if d in (into if way == "up" else outs)], way))
    return out


def close_dangling_ends(graph: StreamGraph, geoms: dict | None, drop: set[str],
                        tol: float = TOUCH_M) -> set[str]:
    """The dropped pieces to put back so that no surviving line ends where the water goes on.

    For each `dangling_ends` end, the SHORTEST run of dropped pieces (by length) that carries the water
    on IN ITS OWN DIRECTION to the surviving network: a stub is followed upstream to the river it left, a
    creek downstream to where its channel rejoins. Following flow, not mere adjacency, is what keeps
    every restored piece on a way down: FWA has side channels with no outflow at all (Williams Creek's
    1100 at Lakelse), and restoring one would hand a creek a dead end the re-homing had repaired. An end
    with no such run is left as it is. Restored pieces are
    the river's own braid — its watershed code, its inherited name — so they join the river's item and
    its rules. Repeats until nothing dangles, since a restored piece can itself end on a dropped one.
    Without geometry there are no ends to judge and nothing is restored."""
    if not geoms:
        return set()
    restored: set[str] = set()
    for _round in range(50):
        live = drop - restored
        new: set[str] = set()
        for n, start, way in dangling_ends(graph, geoms, live, tol):
            if not start or any(s in new for s in start):
                continue                              # this round already reconnects it
            dist = {s: graph.nodes[s].length_m or 0.0 for s in start}
            prev: dict[str, str | None] = {s: None for s in start}
            pq = [(dist[s], s) for s in start]
            heapq.heapify(pq)
            done: set[str] = set()
            hit = None
            while pq and len(done) < _CLOSE_CAP:
                d, u = heapq.heappop(pq)
                if u in done:
                    continue
                done.add(u)
                if way == "down":
                    nb = {graph.edges[i].to_node for i in graph.down_adj.get(u, [])}
                else:
                    nb = {graph.edges[i].from_node for i in graph.up_adj.get(u, [])}
                if any(v != n and v not in live and v in graph.nodes for v in nb):
                    hit = u
                    break
                for v in sorted(nb & live):
                    nd = d + (graph.nodes[v].length_m or 0.0)
                    if v not in dist or nd < dist[v]:
                        dist[v], prev[v] = nd, u
                        heapq.heappush(pq, (nd, v))
            while hit is not None:
                new.add(hit)
                hit = prev[hit]
        if not new:
            break
        restored |= new
    return restored


def prune_mainstem_loops(graph: StreamGraph, geoms: dict | None = None,
                         protected_blks: set[str] | None = None,
                         reconnect_tributaries: bool = False,
                         moved: list | None = None,
                         max_shift_m: float = 5000.0,
                         kept_out: list | None = None,
                         fwa_main: frozenset[str] | set[str] | None = None,
                         closed: dict | None = None,
                         ) -> tuple[StreamGraph, int, set[str]]:
    """Reduce each braid nest to the channels that carry something. Returns (graph, n_removed, fids).

    A nest is no longer kept or dropped whole. `nests.essential_routes` decides which of its channels
    are load-bearing — one route for every water to every destination it would otherwise lose — and the
    rest go. Whole-nest removal is just the case where nothing was load-bearing.

    Everything that pointed at a removed channel is re-homed onto a surviving one, with its route
    measure recomputed on the new blue line. A re-home that would move a confluence further than
    `max_shift_m` is refused and the channel kept instead. Pieces a surviving line would otherwise end
    on are kept too (`close_dangling_ends`); ``closed`` (a dict) gets ``dangling_before`` / ``restored`` /
    ``dangling_after``. ``moved`` collects
    ``(source, old target, new target, metres moved)``; ``kept_out`` collects
    ``(nest size, kept, demands already satisfied elsewhere)`` for reporting.

    ``fwa_main`` (`blk_chains.fwa_mainstem_blks`): FWA's own mainstem blue lines — the river a nest
    hangs off is chosen by it first (`_mainstem_blk`), and a nest's surviving channel prefers it
    (`nests.essential_routes`)."""
    from pipeline.atlas.graph.nests import essential_routes

    records: list = []
    loop_nodes(graph, protected_blks, reconnect_tributaries, records, fwa_main=fwa_main)

    drop: set[str] = set()
    targets: dict[str, list[str]] = {}          # dropped piece -> where its inflows may go instead
    for comp, _in_edges, _foreign, exits, _out_edges in records:
        comp = set(comp)
        keep, spare = essential_routes(graph, comp, fwa_main=fwa_main)
        gone = comp - keep
        if not gone:
            continue
        drop |= gone
        for nid in gone:
            targets[nid] = sorted(keep) + sorted(exits)
        if kept_out is not None:
            kept_out.append((len(comp), len(keep), spare))
    if not drop:
        return graph, 0, set()

    # NO LINE MAY END WHERE THE WATER GOES ON (owner 2026-10-28, FIX "complete"). Dropping part of a nest
    # can leave a surviving line ending on a dropped one: a kept channel's stub above the creek it carries,
    # or a creek whose receiving channel went (its edge is re-homed, its line still stops on a gravel bar).
    # The pieces that close each such end are put back — see `close_dangling_ends`. BEFORE the re-homing,
    # so a restored piece's own flow into a still-dropped one is re-homed like any other inflow.
    if closed is not None:
        closed["dangling_before"] = len(dangling_ends(graph, geoms, drop)) if geoms else 0
    restored = close_dangling_ends(graph, geoms, drop)
    drop -= restored
    if closed is not None:
        closed["restored"] = sorted(restored)
    if not drop:
        return graph, 0, set()

    # Re-home every edge that pointed into a removed channel. A source left with nowhere to go is the
    # failure this guards against, so the target list is tried in order and a refusal keeps the piece.
    rehomed: dict[int, tuple[str, float]] = {}
    for i, e in enumerate(graph.edges):
        if e.to_node not in drop or e.from_node in drop:
            continue
        # A braid that leaves a piece and rejoins THAT SAME PIECE lists it in its own exits, so the
        # nearest exit for this inflow is the source itself. Re-homing there writes `X -> X`: the
        # water is already where it would be sent, the edge carries nothing, and a self-loop traps
        # any downstream walk. Excluded as a target — if it was the only one, the edge simply goes
        # with the braid, because removing a loop off X leaves X's water on X.
        live = [t for t in targets.get(e.to_node, ())
                if t not in drop and t != e.from_node]
        if not live:
            if any(t == e.from_node for t in targets.get(e.to_node, ())):
                continue                        # loop back onto the source: drop the edge with it
            drop.discard(e.to_node)             # nothing survives to carry it: keep the channel
            continue
        exit_node = _nearest_exit(geoms, graph, live, e.to_node)
        # ONE JUNCTION PER WATER AND RIVER (2026-11-13, the mis-measured junctions): a source that already flows into
        # this river at its OWN mouth keeps that junction; re-homing its second way out (through the removed channel)
        # would add another junction where the channel left — Ash River into the Stamp at 14,264 m as well as at its
        # own mouth (14,509 m), parking it in the piece below. The edge goes with the channel instead.
        # the confluence the water actually used: where the removed channel met this target
        out_of = [j for j in graph.down_adj.get(e.to_node, [])]
        m = _measure_on(geoms, graph, e.from_node, exit_node, e.at_measure, out_of)
        if _gap(geoms, graph, e.from_node, exit_node, m) > max_shift_m:
            drop.discard(e.to_node)             # too far to move a confluence for a simplification
            continue
        # (only once the channel may go at all: the gap rule above keeps the channels it kept before, so this
        # removes a duplicate junction, never a channel)
        if _joins_already(graph, e.from_node, exit_node, drop, i):
            continue
        rehomed[i] = (exit_node, m)
    rehomed = {i: v for i, v in rehomed.items() if graph.edges[i].to_node in drop}
    # ...and among the re-homes themselves: a source whose water left by two removed channels gets ONE junction on
    # the river, the one nearest its own mouth (the rest go with their channels)
    best: dict[tuple[str, str], tuple[float, float, int]] = {}
    for i, (exit_node, m) in sorted(rehomed.items()):
        e = graph.edges[i]
        ex = graph.nodes.get(exit_node)
        water = ex.blk if ex is not None and ex.kind == NodeKind.stream and ex.blk else exit_node
        key = (e.from_node, water)
        cand = (_gap(geoms, graph, e.from_node, exit_node, m), m, i)
        if key not in best or cand < best[key]:
            best[key] = cand
    keep = {i for _, _, i in best.values()}
    rehomed = {i: v for i, v in rehomed.items() if i in keep}
    if closed is not None:
        closed["dangling_after"] = len(dangling_ends(graph, geoms, drop)) if geoms else 0
    if not drop:
        return graph, 0, set()

    if moved is not None:
        for i, (exit_node, m) in rehomed.items():
            e = graph.edges[i]
            moved.append((e.from_node, e.to_node, exit_node,
                          round(_gap(geoms, graph, e.from_node, exit_node, m), 1)))
    for i, (exit_node, m) in rehomed.items():
        # the junction's map point MOVES WITH IT (2026-11-13): it was the removed channel's end, so a reader of x/y
        # (the review's map, the gpkg export, anything locating the confluence) put the water where it no longer joins
        x, y = _moved_point(geoms, graph, graph.edges[i], exit_node, m)
        graph.edges[i] = replace(graph.edges[i], to_node=exit_node, at_measure=m, x=x, y=y)

    fids = {f for nid in drop for f in graph.nodes[nid].member_fids}
    for nid in drop:
        graph.nodes.pop(nid, None)
        if geoms is not None:
            geoms.pop(nid, None)
    graph.edges = [e for e in graph.edges
                   if e.from_node not in drop and e.to_node not in drop]
    graph.up_adj, graph.down_adj = {}, {}             # edge indices shifted: rebuild adjacency
    for i, e in enumerate(graph.edges):
        graph.up_adj.setdefault(e.to_node, []).append(i)
        graph.down_adj.setdefault(e.from_node, []).append(i)
    return graph, len(drop), fids


# ---- T1242: creek-less braid loops (owner option D, "simplify the water itself"; ruling E31) ---------------------
#
# ON in every map build (`pipeline.atlas.build`, `--drop-small-loops inf` by default; `--keep-small-loops` turns it
# off for comparisons). Only `SMALL_LOOP_CREEKS = "keep"` ships: a loop a creek flows into is never touched (T1260).
# The "tail" and "rehome" modes are the measured trial variants, kept for T1260 and never used by a build.
#
# What survives `prune_mainstem_loops` and why (T1242 part A, the 2026-11-26 atlas): of the 11,875 loop groups the
# design found "meeting every stated condition", 9,445 are kept by `nests.essential_routes` as the one route an
# UNNAMED creek has out of the nest (the design counted creeks by the bundle's `section_down` pointer, which unnamed
# water does not have), 760 because the loop leaves and rejoins the SAME graph piece (the water is already where it
# is going, but `_reaches_without(src, want=src)` can never say so: a bug), 754 as the river's own route where the
# river cannot reach the exit another way, 571 have a name of their own, the rest are lakes / restored ends.
#
# The trial's rule: a component of the river's own anonymous braiding (`loop_nodes`) that leaves the river's MAIN
# blue line and rejoins it (every same-water neighbour is a main-line piece), whose every vertex lies within
# `max_offset_m` of those main-line pieces, and that holds no blk of the keep list, is removed. Never removed: a loop
# that touches a lake (the river's feed into a lake: removing it let the Bulkley r2 and Serpentine closures go), a
# loop that is only PART of a blue line whose other pieces stay (it leaves a fragment: Attichika Creek's 585 m piece
# "leaves BC unjoined"), a loop off a kept side channel, a piece without geometry.
#
# A creek on a removed loop (`SMALL_LOOP_CREEKS`):
#   "tail"   (variant d, owner 2026-11-27: "every stream simply a single line the whole way down") the creek TAKES THE
#            LOOP'S LOWER PART as its own last reach: the pieces from its junction down to the river (the shortest way,
#            one tree for the whole loop) stay, renamed to the creek (its watershed code, names, gnis), the entry piece
#            cut at the junction; the rest of the loop goes. No gap: the creek's mouth is where the loop met the river.
#            Two creeks whose ways down meet: the bigger (magnitude) owns the shared reach, the smaller joins it.
#   "rehome" (variant a) the creek's mouth edge moves onto the nearest main-line piece; its line stops short.
#   "keep"   (variant c) a loop with a creek on it is not removed.

#: The trial's threshold (braids-design option C/D at 100 m: 5,209 loops, 1,231 km by the design's count).
SMALL_LOOP_TRIAL_M = 100.0
#: What happens to a creek on a removed loop: "tail" (d), "rehome" (a) or "keep" (c). The owner's direction for 0.10.0
#: (2026-11-27): "just drop loops without tributary input streams" = "keep": a loop a creek flows into is not removed,
#: so no creek moves, no tail is renamed, no rule question arises.
SMALL_LOOP_CREEKS = "keep"
#: Whose water a "tail" is (the owner's open question, 2026-11-27):
#:   "creek"          the tail is renamed to the creek: its code, names, rules (136 tails lost a river closure);
#:   "river"          the tail stays the river's side channel (no rule moves; the creek's line ends at the channel);
#:   "unless-looser"  (e) renamed to the creek EXCEPT where that would take a river rule off it: the keep list's
#:                    `tail_river` blue lines (worked out from the baseline's bindings) stay the river's.
SMALL_LOOP_TAIL_RULES = "unless-looser"


def _tail_plan(graph: StreamGraph, comp: set[str], river: set[str], foreign: list[int]):
    """For SMALL_LOOP_CREEKS == "tail": {kept node: (owner creek, metres kept from its downstream end or None = whole)},
    or a reason string when the loop cannot become single lines. One shortest-way-down tree for the whole loop (each
    piece's next piece towards the river), so two creeks' ways down meet and stay together."""
    import heapq as _hq
    dist: dict[str, float] = {}
    nxt: dict[str, str | None] = {}
    pq = []
    for x in sorted(comp):
        if any(graph.edges[j].to_node in river for j in graph.down_adj.get(x, [])):
            dist[x], nxt[x] = graph.nodes[x].length_m or 0.0, None
            pq.append((dist[x], x))
    _hq.heapify(pq)
    while pq:
        d, u = _hq.heappop(pq)
        if d != dist[u]:
            continue
        for j in graph.up_adj.get(u, []):
            v = graph.edges[j].from_node
            if v in comp:
                nd = d + (graph.nodes[v].length_m or 0.0)
                if nd < dist.get(v, float("inf")) or (nd == dist.get(v) and u < (nxt.get(v) or "~")):
                    dist[v], nxt[v] = nd, u
                    _hq.heappush(pq, (nd, v))
    entries: dict[str, list] = {}
    for i in foreign:
        e = graph.edges[i]
        if e.to_node not in dist:
            return "a creek has no way down inside the loop"
        entries.setdefault(e.to_node, []).append((e.at_measure, e.from_node))
    def size(c):
        n = graph.nodes[c]
        return (n.stream_magnitude or 0, n.length_m or 0.0, c)
    owner: dict[str, str] = {}
    paths = {}
    for x, js in entries.items():
        for _m, c in js:
            u, p = x, []
            while u is not None:
                p.append(u)
                u = nxt[u]
            paths[(c, x)] = p
    for (c, x), p in sorted(paths.items(), key=lambda kv: size(kv[0][0]), reverse=True):
        for u in p:
            owner.setdefault(u, c)
    fed = {nxt[u] for u in owner if nxt[u] is not None}
    plan = {}
    for u, c in owner.items():
        keep_m = None
        if u in entries and u not in fed:          # the creek's own entry piece, nothing above it kept: cut there
            n = graph.nodes[u]
            m = max(mm for mm, _c in entries[u])
            keep_m = max(0.0, min(m - (n.down_m or 0.0), n.length_m or 0.0))
        plan[u] = (c, keep_m)
    return plan


#: A blue-line key inside a curated file: 8 to 10 digits standing alone (FWA keys; added streams are negative).
_BLK_IN_TEXT = r"(?<![\d.])-?\d{8,10}(?![\d.])"


#: `waters/loop_exemptions.json`: the document's keys, and each row's (all required but `reading`; `words` may be "")
EXEMPTION_TOP_KEYS = ("_about", "format", "rows")
EXEMPTION_ROW_KEYS = ("water", "blks", "why", "id", "words", "date")
EXEMPTION_ROW_OPTIONAL = ("reading",)


def exemption_problems(doc) -> list[str]:
    """Every way `waters/loop_exemptions.json` is malformed (empty: well formed). For `loop_keeps` (the build stops on
    any) and `check_curated --waters`. A row names blue-line keys only (a lake is never a loop), a unique id, a reason,
    the words that name the water (the book's or DFO's printed text, whole; the owner's words for his own rows) and a
    date; an unknown key is refused, never ignored."""
    import re as _re
    if not isinstance(doc, dict):
        return ["loop_exemptions: the file must be a JSON object"]
    out = [f"loop_exemptions: unknown key {k!r}" for k in doc if k not in EXEMPTION_TOP_KEYS]
    out += [f"loop_exemptions: no `{k}`" for k in EXEMPTION_TOP_KEYS if k not in doc]
    rows = doc.get("rows")
    if not isinstance(rows, list):
        return out + ["loop_exemptions: `rows` must be a list"]
    seen: set = set()
    for i, r in enumerate(rows):
        where = f"loop_exemptions.rows[{i}]"
        if not isinstance(r, dict):
            out.append(f"{where}: not an object")
            continue
        where = f"loop_exemptions.rows[{r.get('id') or i}]"
        out += [f"{where}: unknown key {k!r}" for k in r if k not in EXEMPTION_ROW_KEYS + EXEMPTION_ROW_OPTIONAL]
        out += [f"{where}: no `{k}`" for k in EXEMPTION_ROW_KEYS if k not in r]
        blks = r.get("blks")
        if not isinstance(blks, list) or not blks:
            out.append(f"{where}: `blks` must be a non-empty list")
        else:
            out += [f"{where}: {b!r} is not a blue-line key (digits; a lake is never a loop)" for b in blks
                    if not (isinstance(b, str) and b.lstrip("-").isdigit())]
        for k in ("water", "why", "id"):
            if k in r and not (isinstance(r[k], str) and r[k].strip()):
                out.append(f"{where}: `{k}` must be a non-empty string")
        for k in ("words", "reading"):
            if k in r and not isinstance(r[k], str):
                out.append(f"{where}: `{k}` must be a string")
        if "date" in r and not (isinstance(r["date"], str) and _re.fullmatch(r"\d{4}-\d{2}-\d{2}", r["date"])):
            out.append(f"{where}: `date` must be YYYY-MM-DD")
        if r.get("id") in seen:
            out.append(f"{where}: the id is used twice")
        seen.add(r.get("id"))
    return out


def loop_keeps(exemptions, structural: dict, blk_sets: dict | None = None) -> dict[str, list[str]]:
    """THE ONLY THINGS THAT KEEP A LOOP (owner 2026-11-27: "the simplification should happen at the build level before
    regulations apply"), all read in the map build itself, never from a rules run: {blk: [reasons]}.

      * `exemptions`: the curated `waters/loop_exemptions.json`, one row per water kept BY NAME (a book row or DFO place
        names it, or the owner keeps it), its blue lines listed. Malformed (`exemption_problems`): the build stops;
      * `structural`: {reason: curated file} whose blue lines a loop must keep because something is placed ON it: a
        tidal limit, a gauge, an owner call. Any blue-line key the file names counts (over-inclusive by design:
        keeping a loop too many is safe, removing one a cut stands on is not);
      * `blk_sets`: {reason: blue lines} the build works out itself: the lines its curated and gauge cuts RESOLVE onto
        (every anchor kind, placed by the cut placement itself), the lines an accepted found name labels.

    Loops that touch a lake and loops that are part of a longer blue line are kept by `small_loops` itself."""
    import json as _json
    import re as _re
    out: dict[str, list[str]] = {}
    doc = _json.loads(open(exemptions, encoding="utf-8").read())
    bad = exemption_problems(doc)
    if bad:
        raise SystemExit("waters/loop_exemptions.json is malformed:\n  " + "\n  ".join(bad))
    for row in doc["rows"]:
        for b in row["blks"]:
            out.setdefault(str(b), []).append(f"named: {row['id']}")
    for reason, path in sorted(structural.items()):
        for b in sorted(set(_re.findall(_BLK_IN_TEXT, open(path, encoding="utf-8").read()))):
            if reason not in out.setdefault(b, []):
                out[b].append(reason)
    for reason, blks in sorted((blk_sets or {}).items()):
        for b in sorted({str(x) for x in blks}):
            if reason not in out.setdefault(b, []):
                out[b].append(reason)
    return out


#: a blue-line key named in free text that also holds hex digests (the review marks): digits standing alone, not
#: inside a longer word (a sha's digit run is never matched)
_BLK_IN_PROSE = r"(?<![\w.])-?\d{8,10}(?![\w.])"


def marked_removed_lines(removed_blks, marks_text: str) -> list[str]:
    """(T1242 review) The removed loops' blue lines the owner's review marks name, sorted. Marks are no keep by design
    (E31 keeps by name and structure); a removed line a mark names is a refusal, never a quiet removal."""
    import re as _re
    named = set(_re.findall(_BLK_IN_PROSE, marks_text))
    return sorted({str(b) for b in removed_blks} & named)


def _mains(graph: StreamGraph, fwa_main) -> dict[str, str]:
    """{trimmed watershed code: its mainstem blk}, exactly as `loop_nodes` chooses it."""
    from pipeline.common.utils.wsc import trim_wsc
    by_wsc: dict[str, list] = {}
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk and n.wsc:
            by_wsc.setdefault(trim_wsc(n.wsc), []).append(n)
    return {w: _mainstem_blk(ns, fwa_main) for w, ns in by_wsc.items()}


def small_loops(graph: StreamGraph, geoms: dict, max_offset_m: float, keep_blks: set[str] | frozenset = frozenset(),
                fwa_main: frozenset[str] | set[str] | None = None, why: dict | None = None,
                tail_river_blks: set[str] | frozenset = frozenset()) -> list[dict]:
    """The loop components the trial removes, in node-id order: [{nodes, blks, offset_m, length_m, wsc, tribs}].

    ``why`` (a dict) collects a count per reason a surviving component was NOT removed."""
    import numpy as np
    import shapely
    from shapely.ops import unary_union
    from pipeline.common.utils.wsc import trim_wsc

    if not geoms:
        raise ValueError("small_loops needs the geometry sidecar: an offset cannot be judged without it")
    why = why if why is not None else {}

    def count(k):
        why[k] = why.get(k, 0) + 1

    keep = {str(b) for b in keep_blks}
    mains = _mains(graph, fwa_main)
    pieces_of_blk: dict[str, int] = {}
    for n in graph.nodes.values():
        if n.kind == NodeKind.stream and n.blk:
            pieces_of_blk[n.blk] = pieces_of_blk.get(n.blk, 0) + 1
    records: list = []
    loop_nodes(graph, keep, True, records, fwa_main=fwa_main)
    out = []
    for comp, in_edges, foreign, exits, _out_edges in sorted(records, key=lambda r: min(r[0])):
        wsc = trim_wsc(graph.nodes[min(comp)].wsc)
        main = mains.get(wsc, "")
        fset = set(foreign)
        own_srcs = {graph.edges[i].from_node for i in in_edges if i not in fset}
        nbrs = own_srcs | set(exits)

        def on_river(nid):
            n = graph.nodes.get(nid)
            if n is None:
                return False
            if n.kind == NodeKind.stream:
                return n.blk == main
            return trim_wsc(n.wsc or "") == wsc           # a lake on the river carries its code

        if not own_srcs:
            count("fed by other water only")
            continue
        srcs_all = {graph.edges[i].from_node for i in in_edges}
        if any(graph.nodes[x].kind != NodeKind.stream for x in srcs_all | set(exits) if x in graph.nodes):
            count("touches a lake")
            continue
        blks = {graph.nodes[x].blk for x in comp}
        if sum(pieces_of_blk.get(b, 0) for b in blks) != len(comp):
            count("part of a longer blue line")
            continue
        if not all(on_river(x) for x in nbrs):
            count("off a side channel or another water, not the main line")
            continue
        river = sorted(x for x in nbrs if graph.nodes[x].kind == NodeKind.stream)
        if not river:
            count("off a lake only")
            continue
        if any(graph.nodes[x].blk in keep for x in comp):
            count("keep list")                                # defensive: loop_nodes already skips them
            continue
        if foreign and SMALL_LOOP_CREEKS == "keep":
            count("a creek flows into it")
            continue
        plan = None
        if foreign and SMALL_LOOP_CREEKS == "tail":
            plan = _tail_plan(graph, set(comp), set(river), list(foreign))
            if isinstance(plan, str):
                count(plan)
                continue
        gs = [geoms.get(x) for x in sorted(comp)]
        rg = [geoms.get(x) for x in river]
        if any(g is None or g.is_empty for g in gs) or any(g is None or g.is_empty for g in rg):
            count("no geometry")
            continue
        pts = np.vstack([np.asarray(p.coords)[:, :2] for g in gs for p in getattr(g, "geoms", [g])])
        off = float(shapely.distance(shapely.points(pts), unary_union(rg)).max())
        if off >= max_offset_m:
            count(f"far side {max_offset_m:.0f} m or more from the river")
            continue
        rename = bool(plan) and (SMALL_LOOP_TAIL_RULES == "creek" or (
            SMALL_LOOP_TAIL_RULES == "unless-looser" and not any(graph.nodes[x].blk in tail_river_blks for x in plan)))
        if plan and not rename:
            count("tail stays the river's" if SMALL_LOOP_TAIL_RULES == "river" else "tail stays the river's: a rule would go")
        out.append({"nodes": sorted(comp), "blks": sorted({graph.nodes[x].blk for x in comp}), "rename": rename,
                    # T1247's case: the loop leaves and rejoins the SAME main-line piece (the braid prune can never
                    # take it); flagged so its share of a build can be told apart
                    "same_piece": bool(own_srcs & set(exits)),
                    "offset_m": round(off, 1), "length_m": round(sum(graph.nodes[x].length_m or 0.0 for x in comp), 1),
                    "wsc": wsc, "river": river, "tribs": sorted({graph.edges[i].from_node for i in foreign}),
                    "tail": ({k: [v[0], None if v[1] is None else round(v[1], 1)] for k, v in sorted(plan.items())}
                             if plan else {})})
        count("removed, creek takes the tail" if plan else "removed")
    return out


def drop_small_loops(graph: StreamGraph, geoms: dict, loops: list[dict],
                     moved: list | None = None) -> tuple[StreamGraph, int, set[str]]:
    """Remove the `small_loops` components. Returns (graph, pieces removed, fids). ``moved`` collects
    ``(creek, old target, new target, new measure, metres of gap)``: a "tail" loop's creek keeps its own junction (the
    gap is 0; the new target is where its new last reach meets the river), a "rehome" loop's creek is moved onto the
    nearest main-line piece at the point of that line nearest its old mouth.

    The river's own inflow into a loop is dropped (the river goes on down its main line). A loop is put back, whole,
    when removing it would leave any source with no way downstream (connectivity is checked, never assumed)."""
    from shapely.ops import substring
    drop: set[str] = set()
    plan: dict[int, tuple[str, float, float, float, float]] = {}
    tails: dict[str, tuple[str, float | None]] = {}
    unnamed: dict[str, tuple] = {}                     # tails that stay the river's: cut, never renamed
    tail_moves: list = []
    for lp in loops:
        comp = set(lp["nodes"])
        tail = {k: (v[0], v[1]) for k, v in (lp.get("tail") or {}).items()}
        if not lp.get("rename", True):
            unnamed.update(tail)
        gone = comp - set(tail)
        river = [r for r in lp["river"] if r in graph.nodes and r not in drop]
        local: dict[tuple, tuple] = {}
        ok = bool(river)
        for nid in sorted(comp):
            for i in graph.up_adj.get(nid, []):
                e = graph.edges[i]
                src = e.from_node
                if src in comp:
                    continue
                others = [graph.edges[j].to_node for j in graph.down_adj.get(src, []) if j != i]
                if src in river or (graph.nodes[src].kind != NodeKind.stream):
                    back = any(graph.edges[j].to_node == src for x in comp for j in graph.down_adj.get(x, []))
                    if not back and not [t for t in others if t not in comp and t not in drop]:
                        ok = False                     # the loop was its only way on
                    continue                           # the river's own inflow: goes with the loop
                if tail:
                    continue                           # "tail": the creek keeps its junction on its new last reach
                if _joins_already(graph, src, river[0], drop | comp, i):
                    continue                           # the creek already has its own junction on the river
                here = Point(e.x, e.y) if (e.x or e.y) else None
                if here is None:
                    sg = geoms.get(src)
                    here = Point(sg.coords[0]) if sg is not None and sg.geom_type == "LineString" else None
                if here is None:
                    ok = False
                    continue
                tgt = min(river, key=lambda r: (float(geoms[r].distance(here)), r))
                line, node = geoms[tgt], graph.nodes[tgt]
                m = (node.down_m or 0.0) + float(line.project(here))
                m = min(max(m, node.down_m), node.up_m if node.up_m is not None else m)
                pt = line.interpolate(max(0.0, min(m - (node.down_m or 0.0), line.length)))
                gap = float(here.distance(pt))
                key = (src, tgt)
                cand = (gap, i, tgt, m, float(pt.x), float(pt.y))
                if key not in local or cand < local[key]:
                    local[key] = cand
        if not ok:
            continue
        drop |= gone
        tails.update(tail)
        for gap, i, tgt, m, x, y in local.values():
            plan[i] = (tgt, m, x, y, gap)
        for c in lp.get("tribs") or ():
            # where the creek's water now reaches the river: the end of its kept way down
            u = next((k for k, v in tail.items() if v[0] == c), None)
            seen = set()
            while u is not None and u not in seen:
                seen.add(u)
                nx = [graph.edges[j] for j in graph.down_adj.get(u, []) if graph.edges[j].to_node in tail]
                if not nx:
                    out = [graph.edges[j] for j in graph.down_adj.get(u, []) if graph.edges[j].to_node in river]
                    if out:
                        tail_moves.append((c, u, out[0].to_node, round(out[0].at_measure, 1), 0.0))
                    break
                u = nx[0].to_node
    if not drop and not tails:
        return graph, 0, set()
    for i, (tgt, m, x, y, gap) in sorted(plan.items()):
        e = graph.edges[i]
        if moved is not None:
            moved.append((e.from_node, e.to_node, tgt, round(m, 1), round(gap, 1)))
        graph.edges[i] = replace(e, to_node=tgt, at_measure=m, x=x, y=y)
    if moved is not None:
        moved.extend(tail_moves)
    # the tails become their creeks' last reaches: the creek's code, names and size; an entry piece is cut at the junction
    for nid, (c, keep_m) in sorted(tails.items()):
        n, cr = graph.nodes[nid], graph.nodes[c]
        kw = {} if nid in unnamed else dict(wsc=cr.wsc, gnis_id=cr.gnis_id, display_name=cr.display_name,
                                            name_tuples=cr.name_tuples, stream_magnitude=cr.stream_magnitude,
                                            stream_order=cr.stream_order)
        if keep_m is not None and keep_m < (n.length_m or 0.0) - 0.5:
            kw.update(up_m=(n.down_m or 0.0) + keep_m, length_m=keep_m)
            g = geoms.get(nid)
            if g is not None and g.geom_type == "LineString":
                geoms[nid] = substring(g, 0, keep_m)
        if kw:
            graph.nodes[nid] = replace(n, **kw)
    fids = {f for nid in drop for f in graph.nodes[nid].member_fids}
    for nid in drop:
        graph.nodes.pop(nid, None)
        geoms.pop(nid, None)

    creeks = {c for lp in loops for c in (lp.get("tribs") or ())}

    def keeps(e):
        if e.from_node in drop or e.to_node in drop:
            return False
        if e.to_node in tails and e.from_node not in tails:
            if e.from_node not in creeks:
                return False                       # the river's own inflow into what is now a creek's reach
            n = graph.nodes[e.to_node]
            if e.at_measure > (n.up_m or 0.0) + 0.5:
                return False                       # it entered the part of the entry piece that went
        return True

    graph.edges = [e for e in graph.edges if keeps(e)]
    graph.up_adj, graph.down_adj = {}, {}
    for i, e in enumerate(graph.edges):
        graph.up_adj.setdefault(e.to_node, []).append(i)
        graph.down_adj.setdefault(e.from_node, []).append(i)
    return graph, len(drop), fids
