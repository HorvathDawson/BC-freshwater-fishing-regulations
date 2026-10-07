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
    what stopped the Fraser's braids from ever being pruned."""
    rn = (river_name or "").strip().casefold()
    if (node.display_name or "").strip().casefold() not in ("", rn):
        return True
    return any((t.name or "").strip().casefold() not in ("", rn) for t in node.name_tuples)


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


def prune_mainstem_loops(graph: StreamGraph, geoms: dict | None = None,
                         protected_blks: set[str] | None = None,
                         reconnect_tributaries: bool = False,
                         moved: list | None = None,
                         max_shift_m: float = 5000.0,
                         kept_out: list | None = None,
                         fwa_main: frozenset[str] | set[str] | None = None,
                         ) -> tuple[StreamGraph, int, set[str]]:
    """Reduce each braid nest to the channels that carry something. Returns (graph, n_removed, fids).

    A nest is no longer kept or dropped whole. `nests.essential_routes` decides which of its channels
    are load-bearing — one route for every water to every destination it would otherwise lose — and the
    rest go. Whole-nest removal is just the case where nothing was load-bearing.

    Everything that pointed at a removed channel is re-homed onto a surviving one, with its route
    measure recomputed on the new blue line. A re-home that would move a confluence further than
    `max_shift_m` is refused and the channel kept instead. ``moved`` collects
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
        # the confluence the water actually used: where the removed channel met this target
        out_of = [j for j in graph.down_adj.get(e.to_node, [])]
        m = _measure_on(geoms, graph, e.from_node, exit_node, e.at_measure, out_of)
        if _gap(geoms, graph, e.from_node, exit_node, m) > max_shift_m:
            drop.discard(e.to_node)             # too far to move a confluence for a simplification
            continue
        rehomed[i] = (exit_node, m)
    rehomed = {i: v for i, v in rehomed.items() if graph.edges[i].to_node in drop}
    if not drop:
        return graph, 0, set()

    if moved is not None:
        for i, (exit_node, m) in rehomed.items():
            e = graph.edges[i]
            moved.append((e.from_node, e.to_node, exit_node,
                          round(_gap(geoms, graph, e.from_node, exit_node, m), 1)))
    for i, (exit_node, m) in rehomed.items():
        graph.edges[i] = replace(graph.edges[i], to_node=exit_node, at_measure=m)

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
