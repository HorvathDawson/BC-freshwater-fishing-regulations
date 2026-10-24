"""THE BRAID SWEEP (T67, owner rulings R14 2026-10-16 and 2026-10-24): a cut is a boundary on every
channel of its river that crosses it — but ONLY on a channel that attaches to the mainstem on BOTH
sides of the cut.

A curated, gauge or length split cuts ONE blue line (its host) at one measure. A braid — a side channel
or a network of them — that leaves the host below the cut and rejoins it above is not cut by that, so the
reach step had to put the whole channel on one side (RULINGS E15, the majority side). That left hundreds
of metres to kilometres of side channel with the wrong answer (the Peace below the Halfway, the Adams at
the viewing platforms, the Skeena above Cedarvale). This module cuts those channels where the cut's line
crosses them, under the split's OWN id, so every reader downstream sees ordinary sections.

THE RULE, made exact:

1. THE CUT LINE is the perpendicular at the cut point to the host chord from M−100 m to M+100 m (the
   "median tangent"), reaching `REACH_M` each way and CLIPPED at its first re-crossing of the host line, so
   it never reaches across a meander. No other cap (owner Q4, 2026-10-24).
2. THE TOPOLOGY GATE. A network is every connected group of non-host pieces (any name, any code) within
   `REACH_M` of the cut point. It STRADDLES when it touches the host below the cut AND above it (a lake on
   the host line is host). Only its THROUGH pieces — those on a simple path from the host below to the host
   above (the biconnected block of a virtual LO–HI edge) — may be cut; a tributary dangling off a braid is
   never through. A network touching ONE side only (a bulge, a bend, a ditch grid, South Atnarko) is never
   cut, however the line crosses it (`gate_refuses_one_side`).
3. ONLY THE HOST'S OWN WATER is cut: a through piece must have the host's registry key (`water_key`, the
   registry's own `item_id`; a differently-named channel with no gnis of its own, and a 2300-only ditch, are
   other waters). A straddling network of another water is left whole.
4. WHERE. At each crossing at least `END_M` from the piece's ends; on one piece only the crossing nearest the
   cut point (a second crossing is a bulge). Every crossed through piece of a network is cut once; the rest
   settle by topology.
5. JUNCTION AT THE CUT (every crossing within `END_M` of an end, or the junction itself within `END_M` of the
   cut): nothing is cut; the touches within `END_M` of the cut are RE-HOMED to the host piece on the side the
   channel's body lies, by geometry. A straddle the line misses entirely is reported (`line misses`), never
   guessed.
6. EDGE CONSISTENCY (`make_edges_consistent`, after `split_graph_at`): every edge touching a network piece or
   a braid-cut piece has its FROM end re-homed to the piece of its blue line that the meeting point lies on,
   by GEOMETRY (the to-node's line at the edge's measure), never by the edge's FWA x/y — four curated cases
   carry x/y 0.5–2 km off.

The braid SplitPoints carry the host split's `split_id`, label, anchor type and source, with
`concern: "braid cut …"`; `build.py` applies them in their own `split_graph_at` call with proximity pickup
OFF, and each crossing is at least `END_M` inside its piece, so a braid cut never lands on, relabels or
aliases a lake edge or any other boundary.

Pure geometry and topology, sorted iteration, no clocks. The same gate decides where the automatic length
cuts may go (`length_splits.junction_cuts` asks `spans_a_braid`), so one predicate answers "would a cut here
straddle a braid" for every split kind.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field, replace
from typing import Iterable, Optional

from shapely.geometry import LineString, Point

from pipeline.common.fwa_edge_types import is_manmade_secondary_only
from pipeline.common.models import FlowEdge, NodeKind, SplitPoint, StreamGraph

#: half the chord the cut line is square to (m): the "median tangent" averages ±100 m of the host
TANGENT_M = 100.0
#: how far the network search and the cut line reach from the cut point (m)
REACH_M = 3000.0
#: a crossing closer than this to its piece's end is a junction, not a cut (m) — never a sliver
END_M = 10.0
#: a network is walked no further than this many pieces (a delta's whole plain is not a braid)
MAX_MEMBERS = 400
#: a re-crossing of the host closer than this to the cut point is the cut point itself (m)
_SELF_M = 5.0

#: POLICY: the topology gate (rule 2). False cuts any own-water channel the line crosses — the mutation pin
#: that shows a bulge / South Atnarko fixture is then cut.
GATE_REFUSES_ONE_SIDE = True
#: POLICY: only the host's own water is cut (rule 3). False is the mutation pin (another water's network).
ONLY_THE_HOSTS_OWN_WATER = True
#: POLICY: the edge-consistency pass (rule 6). False is the mutation pin (a braid's edge leaks across).
EDGES_REHOMED_BY_GEOMETRY = True


# ---------------------------------------------------------------- the graph view


def _as_line(g) -> Optional[LineString]:
    if g is None or g.is_empty:
        return None
    if g.geom_type == "LineString":
        return g if len(g.coords) > 1 else None
    if g.geom_type == "MultiLineString":
        cs = [c for p in g.geoms for c in p.coords]
        return LineString(cs) if len(cs) > 1 else None
    return None


def _points(x) -> list[Point]:
    if x is None or x.is_empty:
        return []
    t = x.geom_type
    if t == "Point":
        return [x]
    if hasattr(x, "geoms"):
        return [q for y in x.geoms for q in _points(y)]
    if t == "LineString":
        return [Point(x.coords[0]), Point(x.coords[-1])]
    return []


class View:
    """What the sweep reads of the graph, indexed once. Rebuild after the graph is cut."""

    def __init__(self, graph: StreamGraph, geoms: dict):
        from pipeline.atlas.registry.build import _wsc_gnis_map
        self.g = graph
        self.geoms = geoms
        self.by_blk: dict[str, list[str]] = defaultdict(list)
        for nid, n in graph.nodes.items():
            if n.kind == NodeKind.stream and n.blk:
                self.by_blk[n.blk].append(nid)
        for b in self.by_blk:
            self.by_blk[b].sort(key=lambda s: graph.nodes[s].down_m)
        self.adj: dict[str, list[int]] = defaultdict(list)
        for i, e in enumerate(graph.edges):
            self.adj[e.from_node].append(i)
            self.adj[e.to_node].append(i)
        self.wsc_gnis = _wsc_gnis_map(graph)
        self._lines: dict[str, Optional[LineString]] = {}

    def line(self, nid: str) -> Optional[LineString]:
        if nid not in self._lines:
            self._lines[nid] = _as_line(self.geoms.get(nid))
        return self._lines[nid]

    def neighbours(self, nid: str):
        """(other node, edge index), sorted."""
        out = []
        for i in self.adj.get(nid, ()):
            e = self.g.edges[i]
            o = e.to_node if e.from_node == nid else e.from_node
            if o != nid:
                out.append((o, i))
        return sorted(out)

    def point_at(self, blk: str, m: float) -> Optional[Point]:
        best = None
        for s in self.by_blk.get(blk, ()):
            n, ln = self.g.nodes[s], self.line(s)
            if ln is None:
                continue
            if n.down_m - 1e-6 <= m <= n.up_m + 1e-6:
                return ln.interpolate(min(max(m - n.down_m, 0.0), ln.length))
            for mm, end in ((n.down_m, 0.0), (n.up_m, 1.0)):
                if best is None or abs(m - mm) < best[0]:
                    best = (abs(m - mm), s, end)
        if best is None:
            return None
        return self.line(best[1]).interpolate(best[2], normalized=True)

    def measure_of(self, blk: str, p: Point, near: Optional[Iterable[str]] = None) -> tuple[float, float, str]:
        """(route measure, distance, piece) of `p` on `blk`'s nearest piece."""
        best = None
        for s in (near if near is not None else self.by_blk.get(blk, ())):
            ln = self.line(s)
            if ln is None:
                continue
            d = ln.distance(p)
            if best is None or d < best[0] or (d == best[0] and s < best[1]):
                best = (d, s)
        if best is None:
            return float("nan"), float("inf"), ""
        d, s = best
        return self.measure_on_piece(s, p), d, s

    def measure_on_piece(self, s: str, p: Point) -> float:
        n, ln = self.g.nodes[s], self.line(s)
        if ln is None or ln.length == 0:
            return n.down_m
        # THE SECTIONIZER'S UNITS: a piece's route measure runs along its geometry from its low end, metre for
        # metre (`sectionizer._split_one` cuts the line at `M - down_m`), so a crossing is measured the same way
        return n.down_m + min(ln.project(p), n.up_m - n.down_m)

    def usable_up(self, s: str) -> float:
        """The highest measure of a piece its geometry still reaches (route range and line length agree
        only to within FWA's rounding; a cut past the line's end would leave a point, not a line)."""
        n, ln = self.g.nodes[s], self.line(s)
        return min(n.up_m, n.down_m + (ln.length if ln is not None else 0.0))

    def point_on_piece(self, s: str, m: float) -> Optional[Point]:
        n, ln = self.g.nodes[s], self.line(s)
        if ln is None:
            return None
        return ln.interpolate(min(max(m - n.down_m, 0.0), ln.length))


def water_key(view: View, nid: str) -> str:
    """The registry item a stream piece will belong to — `registry.build.item_id`, with the registry's
    two exceptions applied the same way: a channel with no gnis of its own whose display name differs from
    its group's is its own water (`split_distinct_names`), and a 2300-only line with no gnis of its own is a
    ditch, in no river item (owner Q1, 2026-10-24; `registry.build.DITCHES_LEAVE_RIVER_ITEMS`)."""
    from pipeline.atlas.registry.build import item_id
    n = view.g.nodes[nid]
    if n.kind != NodeKind.stream:
        return f"wbk:{n.wbk}"
    if not n.gnis_id and is_manmade_secondary_only(n.edge_types):
        return f"ditch:{n.blk}"
    k = item_id(n, view.wsc_gnis)
    if not n.gnis_id and n.display_name:
        k = f"{k}|{n.display_name}"
    return k


def _same_water(view: View, a: str, b: str) -> bool:
    ka, kb = water_key(view, a), water_key(view, b)
    if ka == kb:
        return True
    # a gazetted piece of the river and its unnamed braid: the braid's key carries the inherited name
    base_a, _, name_a = ka.partition("|")
    base_b, _, name_b = kb.partition("|")
    if base_a != base_b or ka.startswith("ditch:") or kb.startswith("ditch:"):
        return False
    host_name = view.g.nodes[a].display_name
    return (not name_b or name_b == host_name) and (not name_a or name_a == view.g.nodes[b].display_name)


# ---------------------------------------------------------------- the frame and the networks


@dataclass(frozen=True)
class Frame:
    blk: str
    M: float
    c: Point
    t: tuple[float, float]           # unit tangent, upstream
    line: LineString                 # clipped cut line

    def side(self, p: Point) -> float:
        return (p.x - self.c.x) * self.t[0] + (p.y - self.c.y) * self.t[1]


def frame(view: View, blk: str, M: float, host: Iterable[str]) -> Optional[Frame]:
    a, b, c = view.point_at(blk, M - TANGENT_M), view.point_at(blk, M + TANGENT_M), view.point_at(blk, M)
    if a is None or b is None or c is None:
        return None
    dx, dy = b.x - a.x, b.y - a.y
    h = math.hypot(dx, dy)
    if h == 0:
        return None
    t = (dx / h, dy / h)
    nrm = (t[1], -t[0])
    reach = [REACH_M, REACH_M]
    for k, sgn in enumerate((1.0, -1.0)):
        ray = LineString([(c.x, c.y), (c.x + sgn * REACH_M * nrm[0], c.y + sgn * REACH_M * nrm[1])])
        for s in sorted(host):
            ln = view.line(s)
            if ln is None or ln.distance(c) > REACH_M:
                continue
            for q in _points(ray.intersection(ln)):
                d = q.distance(c)
                if _SELF_M < d < reach[k]:
                    reach[k] = d
    line = LineString([(c.x + reach[0] * nrm[0], c.y + reach[0] * nrm[1]),
                       (c.x - reach[1] * nrm[0], c.y - reach[1] * nrm[1])])
    return Frame(blk=blk, M=M, c=c, t=t, line=line)


@dataclass
class Network:
    members: list[str]
    touches: list[dict]              # {node, host, m, side}
    straddles: bool = False
    through: list[str] = field(default_factory=list)
    through_own: list[str] = field(default_factory=list)
    through_other: list[str] = field(default_factory=list)
    side_channels: list[str] = field(default_factory=list)   # own-water stream members (gate display)


def _host_lakes(view: View, host: set[str], centre: Point) -> set[str]:
    out = set()
    for s in sorted(host):
        ln = view.line(s)
        if ln is not None and ln.distance(centre) > REACH_M:
            continue
        for o, i in view.neighbours(s):
            if o in view.g.nodes and view.g.nodes[o].kind != NodeKind.stream:
                ins = {view.g.edges[j].from_node for j in view.adj.get(o, ()) if view.g.edges[j].to_node == o}
                outs = {view.g.edges[j].to_node for j in view.adj.get(o, ()) if view.g.edges[j].from_node == o}
                if ins & host and outs & host:
                    out.add(o)
    return out


def _lake_side_measure(view: View, lake: str, host: set[str]) -> Optional[float]:
    """Where a lake on the host line sits on it: the measure the host leaves it at (its outflow)."""
    ms = [view.g.edges[j].at_measure for j in view.adj.get(lake, ())
          if view.g.edges[j].from_node == lake and view.g.edges[j].to_node in host]
    if ms:
        return min(ms)
    ups = [view.g.nodes[view.g.edges[j].from_node].up_m for j in view.adj.get(lake, ())
           if view.g.edges[j].to_node == lake and view.g.edges[j].from_node in host]
    return max(ups) if ups else None


def touch_measure(view: View, member: str, edge_ix: int, host_node: str, blk: str) -> Optional[float]:
    """Where a member meets the host line, by GEOMETRY: an edge INTO the host carries the host measure;
    an edge FROM the host meets the member at the member's own line at that edge's measure, projected."""
    e = view.g.edges[edge_ix]
    if e.to_node == host_node:
        return e.at_measure
    p = view.point_on_piece(member, e.at_measure) if view.g.nodes[member].kind == NodeKind.stream else None
    if p is None:
        p = Point(e.x, e.y) if (e.x or e.y) else None
    if p is None:
        return None
    m, _, _ = view.measure_of(blk, p)
    return m


def networks(view: View, blk: str, M: float, host: set[str], *, centre: Point) -> list[Network]:
    """Every network of non-host pieces within `REACH_M` of `centre` that touches the host, with its
    touches classed by side of `M` (lo = below, hi = at or above, as `_split_one` re-homes an edge)."""
    hostlake = _host_lakes(view, host, centre)
    g = view.g

    def candidate(x: str) -> bool:
        if x in host or x in hostlake or x not in g.nodes:
            return False
        ln = view.line(x)
        if ln is not None:
            return ln.distance(centre) <= REACH_M
        if g.nodes[x].kind == NodeKind.stream:
            return False
        return any(math.hypot(g.edges[i].x - centre.x, g.edges[i].y - centre.y) <= REACH_M
                   for i in view.adj.get(x, ()) if (g.edges[i].x or g.edges[i].y))

    lake_m = {lk: _lake_side_measure(view, lk, host) for lk in sorted(hostlake)}
    seeds = set()
    for s in sorted(host | hostlake):
        ln = view.line(s)
        if ln is not None and ln.distance(centre) > REACH_M:
            continue
        for o, _ in view.neighbours(s):
            if candidate(o):
                seeds.add(o)
    out: list[Network] = []
    seen: set[str] = set()
    for sd in sorted(seeds):
        if sd in seen:
            continue
        comp, stack = set(), [sd]
        while stack and len(comp) < MAX_MEMBERS:
            x = stack.pop()
            if x in comp:
                continue
            comp.add(x)
            for o, _ in view.neighbours(x):
                if o not in comp and candidate(o):
                    stack.append(o)
        seen |= comp
        touches = []
        for x in sorted(comp):
            for o, i in view.neighbours(x):
                if o in host:
                    m = touch_measure(view, x, i, o, g.nodes[o].blk)
                elif o in hostlake:
                    m = lake_m.get(o)
                else:
                    continue
                if m is None or m != m:
                    continue
                side = "lo" if m < M else "hi"
                hn = g.nodes[o]
                if o in host and hn.kind == NodeKind.stream and hn.blk == blk:
                    # THE GRAPH'S OWN ANSWER FIRST: a host piece wholly below or above the cut is that
                    # side whatever the geometry says — that is what the reach will read. Only the piece
                    # holding the cut (before it is made) is sided by the touch's measure.
                    if hn.up_m <= M + 1e-6 and hn.down_m < M:
                        side = "lo"
                    elif hn.down_m >= M - 1e-6:
                        side = "hi"
                touches.append({"node": x, "host": o, "edge": i, "m": m, "side": side, "M": M})
        out.append(Network(members=sorted(comp), touches=touches))
    return out


def judge(view: View, net: Network, host_piece: str) -> Network:
    """The gate: does `net` straddle, and which of its pieces are THROUGH (host water / other)."""
    import networkx as nx
    # A touch within END_M of the cut is a junction AT the cut, not a side: it is re-homed to the side the
    # rest of the channel meets the host on (rule 5). Straddling is decided by the touches farther away.
    far = [t for t in net.touches if abs(t["m"] - t.get("M", t["m"])) >= END_M or "M" not in t]
    sides = {t["side"] for t in far}
    net.straddles = sides == {"lo", "hi"}
    net.side_channels = [x for x in net.members
                         if view.g.nodes[x].kind == NodeKind.stream and _same_water(view, host_piece, x)]
    if not net.straddles:
        return net
    G = nx.Graph()
    comp = set(net.members)
    for x in net.members:
        G.add_node(x)
        for o, _ in view.neighbours(x):
            if o in comp:
                G.add_edge(x, o)
    for t in far:
        G.add_edge(t["node"], "\x00LO" if t["side"] == "lo" else "\x00HI")
    G.add_edge("\x00LO", "\x00HI")
    through: set[str] = set()
    for bc in nx.biconnected_components(G):
        if "\x00LO" in bc and "\x00HI" in bc:
            through = set(bc) - {"\x00LO", "\x00HI"}
            break
    net.through = sorted(through)
    streams = [x for x in net.through if view.g.nodes[x].kind == NodeKind.stream]
    own = [x for x in streams if (not ONLY_THE_HOSTS_OWN_WATER) or _same_water(view, host_piece, x)]
    net.through_own = own
    net.through_other = [x for x in streams if x not in set(own)]
    return net


def _side_by_own_cut(g: StreamGraph, net: "Network", sps: list[SplitPoint], blk: str) -> None:
    """A touch on a host line other than this point's is sided by THAT line's cut of the same split — the
    graph's own piece first (wholly below / above it), the touch's measure only on the piece holding it."""
    for t in net.touches:
        hn = g.nodes[t["host"]]
        if hn.kind != NodeKind.stream or not hn.blk or hn.blk == blk:
            continue
        ms = [x.route_measure for x in sps if x.blk == hn.blk]
        if not ms:
            continue
        mm = min(ms, key=lambda q: abs(q - t["m"]))
        side = "lo" if t["m"] < mm else "hi"
        if hn.up_m <= mm + 1e-6 and hn.down_m < mm:
            side = "lo"
        elif hn.down_m >= mm - 1e-6:
            side = "hi"
        t["side"], t["M"] = side, mm


def host_piece_at(view: View, blk: str, M: float) -> Optional[str]:
    ps = view.by_blk.get(blk, ())
    if not ps:
        return None
    return min(ps, key=lambda s: (0.0 if view.g.nodes[s].down_m <= M <= view.g.nodes[s].up_m
                                  else min(abs(view.g.nodes[s].down_m - M), abs(view.g.nodes[s].up_m - M)), s))


# ---------------------------------------------------------------- the gate as a predicate


def spans_a_braid(view: View, blk: str, M: float) -> bool:
    """Would a cut on `blk` at `M` straddle a braid of the host's own water? The one predicate — the sweep
    cuts such a braid, and `length_splits.junction_cuts` never picks such a junction."""
    host = set(view.by_blk.get(blk, ()))
    hp = host_piece_at(view, blk, M)
    c = view.point_at(blk, M)
    if hp is None or c is None:
        return False
    for net in networks(view, blk, M, host, centre=c):
        if judge(view, net, hp).straddles and net.through_own:
            return True
    return False


# ---------------------------------------------------------------- the sweep


@dataclass
class Case:
    """One network judged at one cut: what the gate said, and what was cut."""
    split_id: str
    label: str
    source: str
    anchor_type: str
    host_blk: str
    M: float
    gate: str                        # "cut" | "refused: …" | "rehomed: junction <10 m"
    members: list[str]
    through_own: list[str]
    through_other: list[str]
    side_channels: list[str]
    touches: list[dict]
    crossings: list[dict]            # {node, m, dist, to_end, x, y, used}
    frame_line: list[tuple[float, float]]
    frame: Optional["Frame"] = None
    cut_points: list[SplitPoint] = field(default_factory=list)
    rehomes: list[dict] = field(default_factory=list)


@dataclass
class SweepResult:
    points: list[SplitPoint]
    cases: list[Case]
    rehomes: list[dict]              # {edge: (from, to, at_measure), member, host_blk, M, side}

    def counts(self) -> dict[str, int]:
        c: dict[str, int] = defaultdict(int)
        for k in self.cases:
            c[k.gate] += 1
        return dict(sorted(c.items()))


#: what a length cut may meet: nothing it would have to cut, nothing left straddling
LENGTH_CUT_OK = frozenset({"refused: one side", "refused: another water", "rehomed: junction <10 m",
                           "junction at the cut (already on its side)"})


def length_cut_failures(result: "SweepResult") -> list["Case"]:
    """The build check on the automatic length cuts (owner 2026-10-24): `junction_cuts` never takes a
    junction a braid spans, so the sweep must find nothing to cut there. A case outside `LENGTH_CUT_OK`
    means the junction choice failed."""
    return [c for c in result.cases if c.gate not in LENGTH_CUT_OK] if not result.points else \
        list(result.cases)


def _body_side(view: View, fr: Frame, x: str) -> Optional[str]:
    ln = view.line(x)
    if ln is None:
        return None
    s = fr.side(ln.interpolate(0.5, normalized=True))
    return "lo" if s < 0 else "hi" if s > 0 else None


def sweep(graph: StreamGraph, geoms: dict, points: list[SplitPoint],
          view: Optional[View] = None) -> SweepResult:
    """The braid cuts (and junction re-homes) every split point in `points` needs, against the graph as it
    stands BEFORE those points are applied. Points of one split id on several blue lines all count as host."""
    view = view or View(graph, geoms)
    g = view.g
    by_split: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in points:
        by_split[sp.split_id].append(sp)
    out_pts: list[SplitPoint] = []
    cases: list[Case] = []
    rehomes: list[dict] = []
    for sid in sorted(by_split):
        sps = sorted(by_split[sid], key=lambda s: (s.blk, s.route_measure))
        cut_blks = {s.blk for s in sps}
        host_all = set().union(*(set(view.by_blk.get(b, ())) for b in cut_blks))
        done: set[str] = set()                         # a member is cut once per split
        for sp in sps:
            blk, M = sp.blk, sp.route_measure
            host = set(view.by_blk.get(blk, ()))
            hp = host_piece_at(view, blk, M)
            if hp is None:
                continue
            fr = frame(view, blk, M, host)
            if fr is None:
                continue
            for net in networks(view, blk, M, host_all, centre=fr.c):
                _side_by_own_cut(g, net, sps, blk)
                judge(view, net, hp)
                cands = net.through_own if net.straddles else (
                    [] if GATE_REFUSES_ONE_SIDE else net.side_channels)
                shown = net.through_own if net.straddles else net.side_channels
                crossings = []
                for x in shown:
                    ln = view.line(x)
                    if ln is None:
                        continue
                    n = g.nodes[x]
                    for q in _points(fr.line.intersection(ln)):
                        mm = view.measure_on_piece(x, q)
                        crossings.append({"node": x, "m": mm, "dist": q.distance(fr.c),
                                          "to_end": min(mm - n.down_m, view.usable_up(x) - mm), "x": q.x, "y": q.y,
                                          "used": False})
                real = [k for k in crossings if k["to_end"] >= END_M and k["node"] in set(cands)]
                near = [t for t in net.touches if abs(t["m"] - t["M"]) < END_M]
                far_sides = {t["side"] for t in net.touches if abs(t["m"] - t["M"]) >= END_M}
                own_near = [t for t in near if _same_water(view, hp, t["node"])]
                one_side_crossed = GATE_REFUSES_ONE_SIDE and any(k["to_end"] >= END_M for k in crossings)
                if not net.straddles and not real and not one_side_crossed and not own_near:
                    continue                           # a tributary the line never meets: not a case
                case = Case(split_id=sid, label=sp.label, source=sp.source,
                            anchor_type=getattr(sp.anchor_type, "value", str(sp.anchor_type)),
                            host_blk=blk, M=M, gate="", members=net.members,
                            through_own=net.through_own, through_other=net.through_other,
                            side_channels=net.side_channels, touches=net.touches, crossings=crossings,
                            frame_line=list(fr.line.coords), frame=fr)
                if not net.straddles and (GATE_REFUSES_ONE_SIDE or not real):
                    # ONE-SIDED. A junction within END_M of the cut is re-homed to the side the rest of the
                    # channel meets the host on (with no other touch: the side its body lies on), so the
                    # cut cannot leave it straddling by a few metres (rule 5).
                    body = next(iter(far_sides)) if len(far_sides) == 1 else None
                    if body is None and not far_sides:
                        for x in net.side_channels or net.members:
                            body = _body_side(view, fr, x)
                            if body:
                                break
                    moved = []
                    if body:
                        for t in own_near:
                            e = g.edges[t["edge"]]
                            moved.append({"edge": (e.from_node, e.to_node, e.at_measure), "member": t["node"],
                                          "host": t["host"], "host_blk": g.nodes[t["host"]].blk or blk,
                                          "M": t["M"], "side": body, "split_id": sid,
                                          "changes": t["side"] != body})
                    if any(r["changes"] for r in moved):
                        case.gate = "rehomed: junction <10 m"
                    elif one_side_crossed:
                        case.gate = "refused: one side"
                    else:
                        case.gate = "junction at the cut (already on its side)"
                    case.rehomes = moved
                    rehomes.extend(moved)
                elif net.straddles and not net.through_own:
                    case.gate = "refused: another water"
                elif real:
                    case.gate = "cut"
                    best: dict[str, dict] = {}
                    for k in real:
                        if k["node"] in done:
                            continue
                        if k["node"] not in best or k["dist"] < best[k["node"]]["dist"]:
                            best[k["node"]] = k
                    for x in sorted(best):
                        k = best[x]
                        k["used"] = True
                        done.add(x)
                        p = SplitPoint(split_id=sid, blk=g.nodes[x].blk, route_measure=float(k["m"]), fid="",
                                       label=sp.label, anchor_type=sp.anchor_type, source=sp.source,
                                       concern=f"braid cut across {sid} at {round(k['dist'])} m from the host")
                        case.cut_points.append(p)
                        out_pts.append(p)
                    if not best:
                        case.gate = "cut (already cut by this split)"
                elif crossings:
                    case.gate = "refused: junction <10 m"     # the line meets it only at a piece's end
                else:
                    case.gate = "refused: line misses"
                cases.append(case)
    out_pts.sort(key=lambda p: (p.blk, p.route_measure, p.split_id))
    return SweepResult(points=out_pts, cases=cases, rehomes=rehomes)


# ---------------------------------------------------------------- after the cut


def _rebuild_adj(graph: StreamGraph) -> None:
    up: dict[str, list[int]] = defaultdict(list)
    down: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(graph.edges):
        up[e.to_node].append(i)
        down[e.from_node].append(i)
    graph.up_adj, graph.down_adj = dict(up), dict(down)


def _piece_at(graph: StreamGraph, by_blk: dict, blk: str, m: float, side: str) -> Optional[str]:
    """The piece of `blk` holding measure `m`; exactly at a boundary, the one on `side` ("lo" below)."""
    ps = by_blk.get(blk, ())
    for s in ps:
        n = graph.nodes[s]
        if n.down_m < m < n.up_m:
            return s
    for s in ps:
        n = graph.nodes[s]
        if side == "lo" and abs(n.up_m - m) < 1e-6:
            return s
        if side == "hi" and abs(n.down_m - m) < 1e-6:
            return s
    return None


def apply_rehomes(graph: StreamGraph, rehomes: list[dict]) -> int:
    """Move each junction within END_M of a cut to the host piece on its channel's side (after the cut)."""
    by_blk: dict[str, list[str]] = defaultdict(list)
    for nid, n in graph.nodes.items():
        if n.kind == NodeKind.stream and n.blk:
            by_blk[n.blk].append(nid)
    done = 0
    for r in sorted(rehomes, key=lambda r: (r["host_blk"], r["M"], r["edge"])):
        f, t, at = r["edge"]
        target = _piece_at(graph, by_blk, r["host_blk"], r["M"], r["side"])
        if target is None:
            continue
        member = r["member"]
        for i, e in enumerate(graph.edges):
            if e.kind == "continuation":
                continue
            if e.from_node == member and graph.nodes.get(e.to_node) is not None \
                    and graph.nodes[e.to_node].blk == r["host_blk"] and abs(e.at_measure - at) < 1e-6:
                graph.edges[i] = replace(e, to_node=target, at_measure=r["M"])
                done += 1
            elif e.to_node == member and graph.nodes.get(e.from_node) is not None \
                    and graph.nodes[e.from_node].blk == r["host_blk"] and abs(e.at_measure - at) < 1e-6:
                graph.edges[i] = replace(e, from_node=target)
                done += 1
    _rebuild_adj(graph)
    return done


def make_edges_consistent(graph: StreamGraph, geoms: dict, result: "SweepResult") -> list[dict]:
    """Rule 6, after the cut: every edge between a host piece and a piece of a braid this sweep cut joins two
    pieces on the SAME side of the cut. Each piece's side is exact — a host piece by the cut's measure, a
    braid piece by its crossing's measure, the crossing's own side read off the cut line — and where the two
    ends of an edge disagree its TO end stands (its measure is on that line) and its FROM end is re-homed to
    the nearest piece of its own line on the to end's side, by geometry, never by FWA's x/y. Returns what
    moved."""
    if not EDGES_REHOMED_BY_GEOMETRY:
        return []
    view = View(graph, geoms)
    g = graph
    moved: list[dict] = []
    for case in result.cases:
        if case.gate != "cut" or case.frame is None:
            continue
        fr = case.frame
        side: dict[str, str] = {}
        for s_ in view.by_blk.get(case.host_blk, ()):
            n = g.nodes[s_]
            side[s_] = "lo" if n.up_m <= case.M + 1e-6 else "hi" if n.down_m >= case.M - 1e-6 else "x"
        for cp in case.cut_points:
            below = None
            p = view.point_at(cp.blk, cp.route_measure - min(END_M / 2, 5.0))
            if p is not None:
                v = fr.side(p)
                below = "lo" if v < 0 else "hi" if v > 0 else None
            if below is None:
                continue
            above = "hi" if below == "lo" else "lo"
            for s_ in view.by_blk.get(cp.blk, ()):
                n = g.nodes[s_]
                side[s_] = below if n.up_m <= cp.route_measure + 1e-6 else above
        for nid in sorted(side):
            for i in sorted(view.adj.get(nid, ())):
                e = g.edges[i]
                if e.kind == "continuation":
                    continue
                a_, b_ = side.get(e.from_node), side.get(e.to_node)
                if a_ is None or b_ is None or "x" in (a_, b_) or a_ == b_:
                    continue
                f = g.nodes[e.from_node]
                p = view.point_on_piece(e.to_node, e.at_measure) if g.nodes[e.to_node].kind == NodeKind.stream else None
                cands = [s_ for s_ in view.by_blk.get(f.blk, ()) if side.get(s_) == b_]
                if p is None or not cands:
                    continue
                tgt = min(cands, key=lambda s_: ((view.line(s_).distance(p) if view.line(s_) is not None
                                                  else float("inf")), s_))
                g.edges[i] = replace(e, from_node=tgt)
                moved.append({"split_id": case.split_id, "edge": i, "from": e.from_node, "to": tgt,
                              "to_node": e.to_node})
                view.adj[e.from_node] = [j for j in view.adj.get(e.from_node, ()) if j != i]
                view.adj[tgt] = sorted(set(view.adj.get(tgt, ())) | {i})
    if moved:
        _rebuild_adj(graph)
    return moved


def rehome_host_edges(graph: StreamGraph, geoms: dict, points: list[SplitPoint]) -> list[dict]:
    """Rule 6 on the HOST, after the cut, for every split point: an edge leaving a host piece that meets the
    cut (the river flowing into a side channel's head) is placed by `_split_one` from FWA's x/y, which four
    curated cases carry 0.5-2 km off (South Atnarko among them) — so a one-sided network could be hung on
    the wrong side of the cut and read as straddling it. Each such edge is re-homed to the host piece its
    meeting point lies on BY GEOMETRY (the side channel's own line at the edge's measure, projected onto the
    host). A meeting point within END_M of the cut is a junction AT the cut and is left to rule 5."""
    if not EDGES_REHOMED_BY_GEOMETRY:
        return []
    view = View(graph, geoms)
    g = graph
    moved: list[dict] = []
    for sp in sorted(points, key=lambda x: (x.blk, x.route_measure, x.split_id)):
        blk, M = sp.blk, sp.route_measure
        ps = view.by_blk.get(blk, ())
        adj = [x for x in ps if abs(g.nodes[x].up_m - M) < 1e-3 or abs(g.nodes[x].down_m - M) < 1e-3]
        for hp in sorted(adj):
            for i in sorted(view.adj.get(hp, ())):
                e = g.edges[i]
                if e.from_node != hp or e.kind == "continuation":
                    continue
                t = g.nodes.get(e.to_node)
                if t is None or t.kind != NodeKind.stream or t.blk == blk:
                    continue
                # THE SAME MEASURE THE GATE JUDGED BY (`touch_measure`), so the graph after the cut is the graph
                # the gate predicted: a network it found one-sided cannot come out straddling
                m = touch_measure(view, e.to_node, i, hp, blk)
                if m is None or m != m or abs(m - M) < END_M:
                    continue
                tgt = _piece_at(g, view.by_blk, blk, m, "lo" if m < M else "hi")
                if not tgt or tgt == hp:
                    continue
                g.edges[i] = replace(e, from_node=tgt)
                view.adj[hp] = [j for j in view.adj.get(hp, ()) if j != i]
                view.adj[tgt] = sorted(set(view.adj.get(tgt, ())) | {i})
                moved.append({"split_id": sp.split_id, "edge": i, "from": hp, "to": tgt, "to_node": e.to_node,
                              "m": round(m, 1)})
    if moved:
        _rebuild_adj(graph)
    return moved


def residual_straddles(graph: StreamGraph, geoms: dict, points: list[SplitPoint]) -> list[dict]:
    """AFTER the cut: every own-water network that still touches both sides of a split's host cut, where
    adjacency across a boundary carrying that split's id is not adjacency (the reach's rule). The atlas-side
    twin of the reach gate (`straddles_the_cut` = 0)."""
    view = View(graph, geoms)
    g = graph
    out = []
    by_split: dict[str, list[SplitPoint]] = defaultdict(list)
    for sp in points:
        by_split[sp.split_id].append(sp)
    for sid in sorted(by_split):
        ref = f"split:{sid}"
        sps = by_split[sid]
        cut_blks = {s.blk for s in sps}
        host_all = set().union(*(set(view.by_blk.get(b, ())) for b in cut_blks))

        def severed(a: str, b: str) -> bool:
            na, nb = g.nodes.get(a), g.nodes.get(b)
            if na is None or nb is None or na.blk != nb.blk or not na.blk:
                return False
            bnd = na.upper_bound if abs(na.up_m - nb.down_m) < 1e-6 else (
                na.lower_bound if abs(na.down_m - nb.up_m) < 1e-6 else None)
            return bnd is not None and (bnd.boundary_id == ref or ref in (bnd.aliases or ()))

        for sp in sorted(sps, key=lambda s: (s.blk, s.route_measure)):
            hp = host_piece_at(view, sp.blk, sp.route_measure)
            c = view.point_at(sp.blk, sp.route_measure)
            if hp is None or c is None:
                continue
            orig = view.neighbours
            view.neighbours = lambda x, _o=orig: [(o, i) for o, i in _o(x) if not severed(x, o)]
            try:
                for net in networks(view, sp.blk, sp.route_measure, host_all, centre=c):
                    _side_by_own_cut(g, net, sps, sp.blk)
                    if judge(view, net, hp).straddles and net.through_own:
                        out.append({"split_id": sid, "blk": sp.blk, "M": sp.route_measure,
                                    "pieces": net.through_own})
            finally:
                view.neighbours = orig
    return out


def write_record(path, result: SweepResult, moved: list[dict], residual: list[dict], *, stage: str,
                 extra: Optional[dict] = None) -> None:
    """`braid_sweep.json` beside the atlas: every case the gate judged, what it cut, what it re-homed and
    what still straddles — the review record (rounded, sorted, no clock)."""
    import json
    from pathlib import Path

    def r(x):
        return round(float(x), 2)

    cases = []
    for c in result.cases:
        cases.append({
            "split_id": c.split_id, "label": c.label, "source": c.source, "anchor_type": c.anchor_type,
            "host_blk": c.host_blk, "M": r(c.M), "gate": c.gate, "members": c.members,
            "through_own": c.through_own, "through_other": c.through_other, "side_channels": c.side_channels,
            "touches": [{"node": t["node"], "host": t["host"], "m": r(t["m"]), "side": t["side"]}
                        for t in c.touches],
            "crossings": [{"node": k["node"], "m": r(k["m"]), "dist": r(k["dist"]), "to_end": r(k["to_end"]),
                           "x": r(k["x"]), "y": r(k["y"]), "used": k["used"]} for k in c.crossings],
            "frame_line": [[r(a), r(b)] for a, b in c.frame_line],
            "cut_points": [{"blk": p.blk, "m": r(p.route_measure)} for p in c.cut_points],
            "rehomes": [{"edge": list(h["edge"]), "member": h["member"], "side": h["side"]} for h in c.rehomes],
        })
    doc = {"stage": stage, "counts": result.counts(), "braid_cuts": len(result.points),
           "edges_rehomed_by_geometry": moved, "residual_straddles": residual, "cases": cases}
    if extra:
        doc.update(extra)
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(doc, indent=1, sort_keys=True, default=str))
