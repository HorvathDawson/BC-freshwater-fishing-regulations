"""The tributary walk — ONE implementation, in the graph package where it belongs.

WHY THIS FILE ABSORBED `atlas/reach/tributaries.py`.

There were two. This module held a set-subtraction walk for a single section; the reach
package held a refusing walk over a whole reach. A comment in `common/models/graph.py`
defended the split — "two genuinely different algorithms… the algorithms should differ" —
and that was true of the algorithms and false of the outcome:

  · this module had NO Strahler guard. That guard is what stopped "McLennan Creek including
    tributaries" absorbing 419,078 sections — 20.6% of British Columbia.
  · it had no confluence-mouth seeding, so it missed the creeks joining exactly at a cut
    (51 of 196,596 bounded sections).
  · it subtracted after the fact, which is wrong on a braid: a node can be an ancestor of
    both the mainstem above and a tributary joining inside, and subtraction drops it.
  · and it had NO PRODUCTION CALLER. Every symbol in it was reached only by tests and
    comments. Its passing suite was positive evidence for a walk nothing shipped, and it was
    the module two cross-check tests were comparing the real one against.

So the surviving implementation is the guarded one, and it lives here — a tributary walk is
a graph operation, not a property of how regulations happen to be resolved.

WHAT A TRIBUTARY IS, and why the guards are not optional. Tributary scope is relative to the
RULE'S EXTENT, not to the named river: "no fishing between A and B, including tributaries"
means the streams joining THAT STRETCH. The watershed-code prefix shortcut gets this wrong
in the expensive direction — on the Kootenay it adds the Moyie and Yahk and 3,205 km of
water joining BELOW the regulated reach, which is the app announcing a closure that does not
exist.
"""

from __future__ import annotations

from pipeline.atlas.graph.graph import ancestors
from pipeline.common.models import NodeKind, StreamGraph

#: Edges by which a river continues into its own next piece. Crossing one of these OUT of
#: the reach means leaving onto the mainstem above, which is not a tributary.
# Imported, not redeclared — see pipeline/common/models/graph.py.
from pipeline.common.models.graph import MAINSTEM_EDGE_KINDS

#: POLICY (user rulings 2026-09-24), named so a test can switch each off and watch it go red:
#: a rule's walk collects STREAMS only (the book, p80: "tributaries: all streams that contribute
#: to a larger stream or to a lake") — `expand`;
STREAMS_ONLY = True
#: a lake in the middle of the reach is the river passing through, its inflows tributaries —
#: `_lake_mid_reach`;
LAKE_MID_REACH_IS_THE_RIVER = True
#: a lake's outlet that carries its code across a divide goes with the lake — `code_runs`.
BIFURCATIONS_FOLLOW_THE_CODE = True
#: a lake the row names BESIDE its river, on one of that river's tributaries, does not stop the
#: walk up that tributary — `_on_a_tributary_of_the_reach`.
NAMED_LAKE_ON_A_TRIBUTARY_IS_CLIMBED = True


def lake_inlets(graph: StreamGraph, lake_id: str) -> frozenset[str]:
    """Streams flowing INTO a lake node (its incoming edges)."""
    return frozenset(graph.edges[ei].from_node for ei in graph.up_adj.get(lake_id, []))


def lake_outlets(graph: StreamGraph, lake_id: str) -> frozenset[str]:
    """Streams a lake node drains OUT into (its outgoing edges) — usually one."""
    return frozenset(graph.edges[ei].to_node for ei in graph.down_adj.get(lake_id, []))


def _through_blks(graph: StreamGraph, node_id: str) -> frozenset[str]:
    """The blue lines this node's own flow leaves by.

    For a LAKE this is the outlet's blk: a lake fed and drained by the same river is a
    lake *on* that river, and the inflow sharing the outlet's blk is the river continuing
    through — not a tributary of the lake. Without this, "Lake Koocanusa and its
    tributaries" swallowed the entire Kootenay River above it (36,990 sections instead of
    10,570).

    For a stream the edge KIND already says this (`continuation`), so this returns nothing
    and the kind check does the work.

    A LAKE THAT DRAINS STRAIGHT INTO ANOTHER LAKE has no stream outlet of its own — a lake node
    carries no blk — so the outlet's blue line is found by following the flow down through
    every lake it enters until a stream carries it on. Reading only the first node below
    returned NOTHING for such a lake, and a lake with no through-line has every inflow for a
    tributary: Kinbasket drains through Mica Dam straight into Lake Revelstoke, so "Kinbasket
    Lake's tributaries" took the Columbia above it — the very water its row prints "does not
    include" — and Upper Arrow (draining into Lower Arrow) took the Columbia up through both
    dams. The same holds for the parts of a lake cut in two: Kootenay Lake's main body drains
    into its own West Arm, and its walk climbed the Kootenay River to the border and beyond.
    """
    n = graph.nodes.get(node_id)
    if n is None or n.kind != NodeKind.lake:
        return frozenset()
    out: set[str] = set()
    seen = {node_id}
    stack = [node_id]
    while stack:
        cur = stack.pop()
        for ei in sorted(graph.down_adj.get(cur, [])):
            to = graph.edges[ei].to_node
            d = graph.nodes.get(to)
            if d is None:
                continue
            if d.kind == NodeKind.lake:
                if to not in seen:
                    seen.add(to)
                    stack.append(to)
            elif d.blk:
                out.add(d.blk)
    return frozenset(out)


def _on_a_tributary_of_the_reach(graph: StreamGraph, lake_id: str, through: frozenset[str],
                                 reach: frozenset[str], reach_blks: frozenset[str]) -> bool:
    """Is the reach lake `lake_id` a lake ON A TRIBUTARY of the reach's own river?

    A row may name a lake beside its river: "SUMALLO RIVER (includes "Cedar" Lake)" matches the
    Sumallo and Cedar Lake. Cedar Lake sits on Ferguson Creek, a tributary of the Sumallo. As a
    reach lake its through line (Ferguson) was skipped as "the reach's own flow continuing" — true
    of a lake row (Koocanusa must not take the Kootenay above it) and false here: Ferguson above
    the lake, and the 67 streams feeding it, are tributaries of the Sumallo, and were bound only
    while the walk reached the lake as a tributary lake (69 sections lost when the lake was added).

    The test: the lake's through line is not a line any reach STREAM lies on, and following the
    flow down from the lake reaches a reach STREAM. Reaching a reach LAKE instead (Kootenay Lake's
    main body draining into its own West Arm) is still one water, and the river threading it is
    still that water's own flow."""
    if not through or (through & reach_blks):
        return False
    seen = {lake_id}
    stack = [lake_id]
    while stack and len(seen) < 5_000:
        cur = stack.pop()
        for ei in sorted(graph.down_adj.get(cur, [])):
            to = graph.edges[ei].to_node
            if to in seen:
                continue
            seen.add(to)
            d = graph.nodes.get(to)
            if d is None:
                continue
            if to in reach:
                if d.kind == NodeKind.stream:
                    return True
                continue                        # another part of the same lake water
            stack.append(to)
    return False


def _lake_on_line(graph: StreamGraph, lake_id: str, blks: frozenset[str]) -> bool:
    """Is the upstream LAKE `lake_id` on the flow line `blks` — the river running through it?

    Asked of a lake that drains straight into a boundary lake. A lake node carries no blk, so
    the edge alone cannot say whether it is the river arriving (Upper Arrow into Lower Arrow,
    Kinbasket into Lake Revelstoke — reservoirs in a chain, one dam apart) or a tributary lake
    whose outlet happens to touch. The river's own inflow answers it: the lake is on the line
    when a stream on one of `blks` flows into it, directly or through further lakes above.
    """
    if not blks:
        return False
    seen = {lake_id}
    stack = [lake_id]
    while stack:
        cur = stack.pop()
        for ei in sorted(graph.up_adj.get(cur, [])):
            src = graph.edges[ei].from_node
            s = graph.nodes.get(src)
            if s is None:
                continue
            if s.kind == NodeKind.lake:
                if src not in seen:
                    seen.add(src)
                    stack.append(src)
            elif s.blk in blks:
                return True
    return False


def _lake_mid_reach(graph: StreamGraph, lake_id: str, reach: frozenset[str], below: str) -> bool:
    """Does the REACH carry on above the lake `lake_id` — is it a lake in the middle of it?

    A named river's registry item leaves out the lakes on its line (the Iskut's Tatogga,
    Eddontenajon and Kinaskan; the Williams Lake River's Williams Lake). Standing on the piece
    below such a lake, its `lake_out` edge is the river continuing — but the river continues
    INSIDE the reach, so the lake is the river passing through, and the streams entering it are
    tributaries (ruling 2026-09-24: 10,848 sections in 16 rules were lost, 2,155 of them the
    Iskut's).

    `below` is the reach piece the walk arrived from. The lake is mid-reach when a reach piece on
    THE SAME BLUE LINE flows into it (directly, or through further lakes above — two reservoirs back
    to back) from FURTHER UP that line: the river enters above and leaves below. Anything looser is
    wrong. A lake at the reach's TOP has no reach piece above it ("X River downstream of Y Lake"
    does not take Y Lake's inflows); and a lake at the reach's MOUTH that a braid of the river both
    enters and leaves is not in its middle — the Stellako's last braid runs out of Fraser Lake and
    back, and a looser test handed the Stellako every stream feeding Fraser Lake (2,393 sections).
    """
    b = graph.nodes.get(below)
    if b is None or not b.blk:
        return False
    seen = {lake_id}
    stack = [lake_id]
    while stack:
        cur = stack.pop()
        for ei in sorted(graph.up_adj.get(cur, [])):
            src = graph.edges[ei].from_node
            s = graph.nodes.get(src)
            if s is None:
                continue
            if src in reach and (src == below
                                 or (s.blk == b.blk and s.down_m >= b.up_m - 1.0)):
                return True                    # (one piece both entering and leaving: through it)
            if s.kind == NodeKind.lake and src not in seen and src not in reach:
                seen.add(src)
                stack.append(src)
    return False


def _wsc_below(code: str, parent: str) -> bool:
    """Is watershed code `code` at or below `parent` in the FWA hierarchy?"""
    return code == parent or code.startswith(parent + "-")


def code_runs(graph: StreamGraph) -> tuple[dict[str, tuple[str, ...]], frozenset[tuple[str, str]]]:
    """Bifurcations where the FLOW leaves the watershed the CODE puts the water in.

    Dewar Lake drains both ways: its main outlet south to the Williams Lake River, and a secondary
    channel north through a pond into Seven Mile Lake (South Hawks Creek, Hawks Creek). FWA codes
    the channel with Dewar Lake's own code — it is FWA's membership call that the channel belongs
    with the lake it leaves. By flow, a walk from the Hawks side climbed it (closed to sturgeon);
    by code it is the Williams Lake River's (catch and release). The user's ruling (2026-09-24):
    follow the code.

    A RUN starts at an outlet of a lake with two or more outlets and follows the flow down, one
    node at a time, until it reaches a node whose code the run's code does not descend from — the
    code divide. The run's pieces are the lake's; the edge that crosses the divide is a CUT.
    Returns `({lake: run node ids}, {(run tail, node below the divide)})`. A distributary that
    rejoins its own watershed crosses no divide and is not a run.

    Computed once per graph object and kept on it.
    """
    cached = getattr(graph, "_code_runs_cache", None)
    if cached is not None and cached[0] is graph.edges:
        return cached[1]
    runs: dict[str, tuple[str, ...]] = {}
    cuts: set[tuple[str, str]] = set()
    for nid in sorted(graph.down_adj):
        n = graph.nodes.get(nid)
        if n is None or n.kind != NodeKind.lake:
            continue
        outs = sorted({graph.edges[ei].to_node for ei in graph.down_adj.get(nid, [])} - {nid})
        if len(outs) < 2:
            continue
        main = _main_outlets(graph, outs)
        mine: list[str] = []
        for head in outs:
            if head in main:
                continue                       # the lake's own river: never moved by code
            run = [head]
            seen = {nid, head}
            cur = head
            while True:
                dn = sorted({graph.edges[ei].to_node for ei in graph.down_adj.get(cur, [])})
                if len(dn) != 1 or dn[0] in seen:
                    break
                nxt = dn[0]
                a = (graph.nodes[cur].wsc or "") if cur in graph.nodes else ""
                b = (graph.nodes[nxt].wsc or "") if nxt in graph.nodes else ""
                if a and b and not _wsc_below(a, b):
                    mine.extend(run)
                    cuts.add((cur, nxt))
                    break
                seen.add(nxt)
                run.append(nxt)
                cur = nxt
        if mine:
            runs[nid] = tuple(dict.fromkeys(mine))
    got = (runs, frozenset(cuts))
    try:
        setattr(graph, "_code_runs_cache", (graph.edges, got))
    except (AttributeError, TypeError):
        pass
    return got


#: FWA EDGE_TYPEs of MAIN flow (single line, in a wetland, construction lines of a double-line
#: river). An outlet carrying one of these is the lake's river, not a distributary.
_MAIN_FLOW_TYPES = frozenset({"1000", "1050", "1200", "1250"})


def _main_outlets(graph: StreamGraph, outs: list[str]) -> frozenset[str]:
    """The outlet(s) that are a lake's own river: the largest by Strahler order, and among equals
    those FWA draws as main flow. Only the OTHER outlets can be a code run.

    Without this the run followed a lake's main river too: Nanika Lake's outlet (the Nanika River,
    order 6) met a lake whose code the river does not descend from, and every walk from below lost
    the lake and all 491 sections above it; the white sturgeon rules lost 4,400. A bifurcation is a
    lake's SECONDARY outlet (Dewar Lake's side channel is FWA secondary flow, 1100/1150)."""
    def order(o):
        return (graph.nodes[o].stream_order or 0) if o in graph.nodes else 0
    top = max(order(o) for o in outs)
    best = [o for o in outs if order(o) == top]
    flow = [o for o in best
            if set(getattr(graph.nodes.get(o), "edge_types", ()) or ()) & _MAIN_FLOW_TYPES]
    return frozenset(flow or best)


#: POLICY (user ruling 2026-09-29): A CUT AT A CONFLUENCE NAMES THE JOINING WATER ONLY TO SAY WHERE
#: THE REACH ENDS. "No Fishing upstream of Morice/Bulkley River confluence" is about the Bulkley;
#: "No Fishing upstream of the Muchalat River (see river specific regulations for Muchalat River)"
#: is about the Gold. The river joining AT the cut is neither the reach nor one of its tributaries,
#: so it and everything above it are kept out of the walk (`confluence_joiners` +
#: `subtree`, applied by `reach.build`) — unless the row's own words take it in ("upstream of and
#: including Hemmingsen Creek"). Before this, the Bulkley's closure walked into the Morice and shut
#: 2,175 sections all year, the Duncan's the Lardeau (1,374), the Gold's the Muchalat: 36 rules,
#: 10,492 sections. Switch it off and those return.
CONFLUENCE_CUT_EXCLUDES_THE_JOINING_WATER = True

#: ...UNLESS THE JOINING WATER HAS NO ROW OF ITS OWN (user ruling 2026-09-30). Then nothing else in
#: the tables speaks for it, and it GOES WITH THE CUT, ON BOTH SIDES: Bannon Creek takes both of the
#: Chemainus's closures — "downstream of Bannon Creek, July 1-Sept 30" and "upstream of Bannon Creek,
#: Dec 1-Sept 30" — because the creek at the boundary belongs to neither half more than the other
#: and leaving it out of both left it under the zone alone. A joining water WITH a row of its own
#: (the Morice, the Lardeau, the Iltasyuko, the Muchalat, the Eve …; `outside.rowed_waters`) stays
#: out: its own row governs it. The rule's own words still decide first — "including X" takes it
#: in, "not including X" keeps it out — and signs the words put up- or downstream of the
#: confluence put it inside the reach (the Nass and the Meziadin).
#:
#: LICENSING FOLLOWS THE SAME TEST, and replaces the old "a designation always walks in" switch:
#: when in doubt the water is Classified. A joining water with no row inherits the designation
#: (Limonite Creek, whose whole river system is Classified); one whose own row prints no
#: designation does not (the Iltasyuko), and one whose own row prints its own keeps that.
CONFLUENCE_WATER_WITHOUT_A_ROW_GOES_WITH_THE_CUT = True

#: A POINT cut whose label puts it NEAR a named confluence ("fishing boundary signs near the Mobbs
#: Creek confluence") is a cut at that confluence when the named stream's mouth lies this close to
#: it, on its own blue line (`reach.build.confluence_excludes`). The Lardeau's signs stand 53 m below
#: the Mobbs Creek mouth.
CONFLUENCE_NEAR_M = 150.0


def confluence_joiners(graph: StreamGraph, blk: str, m: float, *, near: float = 1.0,
                       names: tuple[str, ...] = ()) -> frozenset[str]:
    """The mouths of streams on OTHER blue lines joining line `blk` within `near` metres of `m`.

    `names` (lower-cased) keeps only joiners whose display name is one of them — how a point cut's
    label ("… near the Mobbs Creek confluence") says which of the creeks around it it means. With no
    `names`, a joiner carrying the SAME watershed code as the line is a side channel of the river
    itself, not a joining water, and is left out."""
    out: set[str] = set()
    for nid in _nodes_on(graph, blk):
        n = graph.nodes[nid]
        if n.down_m - near > m or n.up_m + near < m:
            continue
        for ei in graph.up_adj.get(nid, []):
            e = graph.edges[ei]
            src = graph.nodes.get(e.from_node)
            if src is None or src.blk == blk or e.kind != "confluence" \
                    or e.at_measure is None or abs(e.at_measure - m) > near:
                continue
            if names:
                if (src.display_name or "").strip().lower() not in names:
                    continue
            elif src.wsc and n.wsc and src.wsc == n.wsc:
                continue
            out.add(e.from_node)
    return frozenset(out)


def _nodes_on(graph: StreamGraph, blk: str) -> tuple[str, ...]:
    """Every node on blue line `blk` — indexed once per graph object."""
    cached = getattr(graph, "_nodes_on_blk_cache", None)
    if cached is None or cached[0] is not graph.nodes:
        idx: dict[str, list[str]] = {}
        for nid, n in graph.nodes.items():
            if n.blk:
                idx.setdefault(n.blk, []).append(nid)
        cached = (graph.nodes, {k: tuple(sorted(v)) for k, v in idx.items()})
        try:
            setattr(graph, "_nodes_on_blk_cache", cached)
        except (AttributeError, TypeError):
            return cached[1].get(blk, ())
    return cached[1].get(blk, ())


def subtree(graph: StreamGraph, mouths, *, guarded: bool = True) -> frozenset[str]:
    """`mouths` and EVERYTHING upstream of them — a joining river, its own mainstem above its
    mouth included (which `tributaries_of_reach` would leave out as "the river continuing")."""
    out: set[str] = set()
    for m in sorted(mouths):
        out.add(m)
        out |= ancestors(graph, m, guarded=guarded)
    return frozenset(out)


def tributaries_of_reach(
    graph: StreamGraph,
    reach: set[str] | frozenset[str],
    *,
    blocked: set[str] | frozenset[str] = frozenset(),
    passed: set[str] | frozenset[str] = frozenset(),
    guarded: bool = True,
    window: tuple[str, float, float] | None = None,
) -> frozenset[str]:
    """Sections draining INTO `reach`, recursively. Excludes `reach` itself.

    `blocked` sections are neither returned nor traversed — that is how a
    `tributary_excludes` carve-out removes a stream *and everything above it*, which is what
    "except Burnt Bridge Creek upstream of Sitkatapa Creek" means.

    `passed` sections are traversed but never returned — a carve-out marked `walk_past`: the named
    water has its own row, its tributaries do not ("Elk River's tributaries … see separate listings
    for Fording R. downstream of Josephine Falls": the Fording is not the row's, the creeks
    feeding it still are).

    `guarded=False` drops the 2300 barrier stop; it exists only so tests can show the guard
    is doing something.

    This is the TOPOLOGICAL answer: lakes that drain in are in it (a carve-out blocks them with
    everything else above). What a RULE collects is `expand`, which keeps the streams only.

    A lake on the river's line with the reach carrying on above it (`_lake_mid_reach`) is walked
    as the river passing through: its other inflows are tributaries, it is not one itself, and
    its inflow on the river's own line is still the river. A lake's secondary outlet that carries
    the lake's code across a divide (`code_runs`) goes with the lake, not with the flow.
    """
    reach = frozenset(reach)
    out: set[str] = set()
    seen: set[str] = set(reach)
    stack: list[str] = sorted(reach)          # sorted: determinism, not correctness
    online: dict[str, str] = {}               # lakes mid-reach -> the reach piece below them
    reach_blks = frozenset(n.blk for r in reach if (n := graph.nodes.get(r)) is not None
                           and n.kind == NodeKind.stream and n.blk)
    runs, cuts = code_runs(graph) if BIFURCATIONS_FOLLOW_THE_CODE else ({}, frozenset())

    # A stream joining EXACTLY at the reach's lower cut belongs to the reach. When a
    # curator anchors a cut on a confluence — "upstream of the confluence with Slesse
    # Creek" — that creek's mouth sits on the point. FWA may attach it to the piece BELOW
    # the cut, in which case the walk would never see it and the named water would be
    # missing from the very reach it defines. Rare (51 of 196,596 bounded sections) and
    # entirely concentrated on confluence-anchored cuts, which is where it matters most.
    for seed in sorted(_mouths_at_lower_bound(graph, reach, window)):
        if seed not in seen and seed not in blocked:
            n = graph.nodes.get(seed)
            if n is not None and not (guarded and n.is_barrier):
                seen.add(seed)
                out.add(seed)
                stack.append(seed)

    while stack:
        node = stack.pop()
        at_boundary = node in reach or node in online
        node_n = graph.nodes.get(node)
        is_lake = at_boundary and node_n is not None and node_n.kind == NodeKind.lake
        through = _through_blks(graph, node) if is_lake else frozenset()
        if through and node in reach and NAMED_LAKE_ON_A_TRIBUTARY_IS_CLIMBED \
                and _on_a_tributary_of_the_reach(graph, node, through, reach, reach_blks):
            through = frozenset()             # its inflow on that line is a tributary too
        # A lake the walk reached (not the reach's own water, not the river passing through)
        # brings the runs FWA codes to it, with whatever joins them.
        if not at_boundary:
            for r in runs.get(node, ()):
                if r in seen or r in blocked:
                    continue
                rn = graph.nodes.get(r)
                if rn is None or (guarded and rn.is_barrier):
                    continue
                seen.add(r)
                out.add(r)
                stack.append(r)
        for ei in graph.up_adj.get(node, []):
            e = graph.edges[ei]
            src = e.from_node
            if (src, node) in cuts:
                continue                       # the code divide: that run is another basin's

            # Leaving the reach through its own top. "The reach's own flow continuing"
            # takes two forms, and which one applies depends on what you are standing on:
            #
            #   STREAM boundary — the `continuation` / `lake_out` edge above it.
            #   LAKE boundary   — the inflow on the same blue line as the lake's OUTflow,
            #                     i.e. the river running through the lake — arriving as a
            #                     stream on that line, or as the next LAKE up it (a chain
            #                     of reservoirs, `_lake_on_line`).
            #
            # Applied ONLY at the boundary: the same edge kind inside a tributary is that
            # tributary's own continuation and must be followed.
            if at_boundary and src not in reach:
                sn = graph.nodes.get(src)
                src_is_lake = sn is not None and sn.kind == NodeKind.lake
                if is_lake:
                    if sn is not None and sn.blk and sn.blk in through:
                        continue
                    if src_is_lake and _lake_on_line(graph, src, through):
                        if src not in seen and src not in blocked and LAKE_MID_REACH_IS_THE_RIVER \
                                and node in online \
                                and _lake_mid_reach(graph, src, reach, online[node]):
                            seen.add(src)              # the next lake of a chain, mid-reach
                            online[src] = online[node]
                            stack.append(src)
                        continue
                elif e.kind in MAINSTEM_EDGE_KINDS:
                    if src_is_lake and src not in seen and src not in blocked \
                            and LAKE_MID_REACH_IS_THE_RIVER \
                            and _lake_mid_reach(graph, src, reach, node):
                        seen.add(src)                  # the river passing through a lake
                        online[src] = node
                        stack.append(src)
                    continue
            if src in seen or src in blocked:
                continue
            n = graph.nodes.get(src)
            if n is None:
                continue
            if guarded and n.is_barrier:
                continue                       # the canal, and everything above it
            if guarded and _breaks_strahler(n, graph.nodes.get(node)) \
                    and not _is_main_outlet(graph, src, node):
                continue                       # a bigger river cannot be a tributary
            seen.add(src)
            out.add(src)
            stack.append(src)

    return frozenset(out - frozenset(passed))


def _mouths_at_lower_bound(graph: StreamGraph, reach: frozenset[str],
                           window: tuple[str, float, float] | None = None) -> set[str]:
    """Tributary mouths sitting exactly on the reach's lower cut.

    Looked for on the piece BELOW the cut: the confluence is at the same route measure, so
    FWA may hang the mouth on either side of it. Only tributaries are taken — anything on
    the reach's own blue line is the mainstem, not a joining stream.

    `window` is the `(blk, lo, hi)` the extent ACTUALLY resolved to, handed over by
    `resolve_extent`. Prefer it: re-deriving the cut from node bounds means two places have
    to agree about where the reach starts, and they can drift. Node bounds remain the
    fallback for a reach that has no single window (a `whole` extent, or a `between`
    spanning two blue lines).
    """
    out: set[str] = set()
    if window is not None:
        w_blk, w_lo, _w_hi = window
        for nid in reach:
            n = graph.nodes.get(nid)
            if n is None or n.blk != w_blk:
                continue
            out |= _mouths_at(graph, nid, n, w_lo, reach)
        return out
    for nid in reach:
        n = graph.nodes.get(nid)
        if n is None or n.lower_bound is None or not n.blk:
            continue
        m = n.lower_bound.route_measure
        out |= _mouths_at(graph, nid, n, m, reach)
    return out


def _mouths_at(graph: StreamGraph, nid: str, n, m: float,
               reach: frozenset[str]) -> set[str]:
    """Tributary mouths on the piece immediately BELOW `nid`, at route measure `m`.

    THE STRAHLER GUARD APPLIES HERE TOO. Everything joining at the confluence is a sibling of
    the reach, and one of those siblings is the river the reach FLOWS INTO — it arrives on its
    own blue line, by a `confluence` edge, at exactly this measure, so every other test here
    passes it. The walk below rejects that case with `_breaks_strahler` ("a bigger river cannot
    be a tributary"); the seeding did not, and so a rule reaching the mouth of its own water
    picked up the receiving river.

    That is how "No Fishing downstream of the main logging road bridge, May 1-31" on the
    CHEHALIS — a `downstream_of` whose window runs to measure 0, i.e. to the mouth — bound two
    sections of the HARRISON, which is the river the Chehalis empties into.
    """
    out: set[str] = set()
    for ei in graph.down_adj.get(nid, []):
        below = graph.edges[ei].to_node
        for fi in graph.up_adj.get(below, []):
            f = graph.edges[fi]
            src = graph.nodes.get(f.from_node)
            if (f.from_node not in reach and src is not None and src.blk != n.blk
                    and f.kind == "confluence" and abs(f.at_measure - m) < 0.5
                    and not _breaks_strahler(src, n)):
                out.add(f.from_node)
    return out


def _is_main_outlet(graph: StreamGraph, lake_id: str, out_id: str) -> bool:
    """Is `out_id` the MAIN outlet of the lake `lake_id` — the lake's own river leaving it?

    A lake node's order aggregates every inflow, so it can run ahead of the first piece of its
    own outlet: Dester Lake is order 4 and Meldrum Creek below it order 2, with EQUAL watershed
    codes, and the order guard read "a bigger river into a smaller one" and cut off the lake and
    everything above it (upper Meldrum Creek and its tributaries) from every walk that reached
    it from below.

    Only the main outlet is exempt, never a lake as such. Of the 386 lake-source edges the guard
    refuses in the province, ~360 are a SECONDARY outlet — the lake drains by two or more
    streams and this is the smaller — and climbing a distributary into its lake would hand a
    small creek the lake's whole catchment. The main outlet is the one that carries the lake's
    own watershed code and is larger than every other stream the lake drains into (or the only
    one). An outlet whose code differs (Burnaby Lake "draining" into Still Creek, 100-019698 vs
    100-019698-999999) is not the lake's line, and the guard stands.
    """
    lake = graph.nodes.get(lake_id)
    out = graph.nodes.get(out_id)
    if lake is None or out is None or lake.kind != NodeKind.lake or out.kind == NodeKind.lake:
        return False
    if not lake.wsc or lake.wsc != out.wsc:
        return False
    others = {graph.edges[ei].to_node for ei in graph.down_adj.get(lake_id, [])} - {out_id}
    mine = out.stream_order or 0
    for o in others:
        on = graph.nodes.get(o)
        if on is None or on.kind == NodeKind.lake:
            continue
        if (on.stream_order or 0) >= mine:
            return False                        # not larger than every other outlet
    return True


def _breaks_strahler(upstream, downstream) -> bool:
    """True when `upstream` is a strictly LARGER river than the piece it flows into AND the
    watershed code has no opinion.

    The WSC hierarchy is the authority: if `upstream`'s code extends `downstream`'s, FWA is
    saying this is a genuine tributary and Strahler must not override it. That happens 93
    times — nearly all lakes, whose order aggregates every inflow and so runs ahead of
    their own outlet's first piece.

    Where the codes are EQUAL, WSC is silent by design (equal codes are how real side
    channels pass), and that silence is exactly where the floodplain artifacts hide. There,
    order decides.

    Unknown orders are never judged — absence of evidence is not evidence of an artifact.
    """
    if upstream is None or downstream is None:
        return False
    a, b = upstream.stream_order, downstream.stream_order
    if a is None or b is None or a <= b:
        return False
    up_wsc, dn_wsc = upstream.wsc or "", downstream.wsc or ""
    if up_wsc and dn_wsc and up_wsc != dn_wsc and up_wsc.startswith(dn_wsc):
        return False                    # WSC says genuine tributary — it wins
    return True


def expand(
    graph: StreamGraph,
    reach: set[str] | frozenset[str],
    *,
    only: bool = False,
    excluded: set[str] | frozenset[str] = frozenset(),
    passed: set[str] | frozenset[str] = frozenset(),
    guarded: bool = True,
    window: tuple[str, float, float] | None = None,
    registry=None,
) -> frozenset[str]:
    """The section set a tributary-scoped rule actually covers.

    `only=True` is `tributaries_only` — the tributaries WITHOUT the mainstem, which is a real
    regulation shape ("no fishing in tributaries above Holt Creek", 44 rules). The reach is
    still what defines *which* tributaries; it is just not itself in the answer.

    TRIBUTARIES ARE STREAMS. The book's glossary (p80): "tributaries: all streams that contribute
    to a larger stream or to a lake". So the walk's lakes are climbed through — a creek feeding a
    tributary lake is still a tributary — but never collected (user ruling 2026-09-24; 433 rules
    bound ~232,000 lake sections this way before it). The reach itself is kept whole: a row that
    names a lake binds it through its own extents. A WATERSHED is an area (`area:basin:`), not a
    walk, and keeps its lakes.

    WHAT IS A STREAM is the registry's answer (`reach.water_kind.kind_of`, given `registry`): a
    river's own polygon on a tributary (the Little Rancheria's 42) is collected with the river;
    a slough's polygons are its slough. The walk's TOPOLOGY still climbs by node kind — how flow
    passes through a polygon, not what the water is.
    """
    from pipeline.atlas.reach.water_kind import kind_of
    reach = frozenset(reach)
    excluded = frozenset(excluded)
    tribs = frozenset(
        s for s in tributaries_of_reach(graph, reach, blocked=excluded, passed=passed,
                                        guarded=guarded, window=window)
        if not STREAMS_ONLY or kind_of(graph, registry, s) == "stream")
    base = frozenset() if only else (reach - excluded)
    return base | tribs


# --------------------------------------------------------------------------------------- #
# The older names, kept — as the SAME walk over a one-section reach.
#
# These were separate implementations. `tributaries_between` subtracted the mainstem-above
# closure from the section's ancestors; `lake_tributaries` subtracted the through-river's.
# Both are special cases of "what joins THESE sections", which is what the walk above
# answers, so they are now spellings of it rather than second opinions.
#
# THIS CHANGES THREE LAKES, deliberately. Measured over a 200-lake sample, the old
# subtraction and the guarded walk agreed on 197 and differed on 3 — and the differences ran
# both ways (+1, +8, -11). The walk is the one with the Strahler guard, the confluence-mouth
# seeding, and no braid-subtraction bug, so where they differed the walk is the answer we
# want. What is gone with them is the pair of cross-check tests that compared the two: a
# function cannot cross-check itself, and the disagreement they were reporting was between
# production and a module nothing shipped.
# --------------------------------------------------------------------------------------- #

def tributaries_between(graph: StreamGraph, section_id: str,
                        guarded: bool = True) -> frozenset[str]:
    """Everything joining ONE section, excluding the river continuing above it."""
    return tributaries_of_reach(graph, {section_id}, guarded=guarded)


def lake_tributaries(graph: StreamGraph, lake_id: str,
                     guarded: bool = True) -> frozenset[str]:
    """A lake's own tributaries, excluding the river running THROUGH it.

    Lake Koocanusa swallowed the whole Kootenay above it — 36,990 sections instead of
    10,570 — before that exclusion existed.
    """
    return tributaries_of_reach(graph, {lake_id}, guarded=guarded)


def tributary_node_ids(graph: StreamGraph, node_id: str,
                       guarded: bool = True) -> frozenset[str]:
    """EVERYTHING upstream, mainstem included — `ancestors`, under an older name.

    NOT a spelling of `tributaries_of_reach`, which is what I first made it. That walk
    excludes the river continuing above the reach; this one does not, and the difference is
    the whole catchment above a section. The name is the trap — "tributary node ids" reads
    like the tributaries and means the ancestors — so it says so here rather than in a
    caller's head.
    """
    return frozenset(ancestors(graph, node_id, guarded=guarded))


def with_tributaries(graph: StreamGraph, section_ids, guarded: bool = True) -> frozenset[str]:
    """The base waters AND everything joining them — an `[Includes Tributaries]` set."""
    base = set(section_ids)
    return frozenset(base | tributaries_of_reach(graph, base, guarded=guarded))


# Node lookups, not walks — carried over unchanged from the module this absorbed.

def piece_above(graph: StreamGraph, blk: str, boundary_label: str):
    """The stream piece of ``blk`` whose LOWER bound is the boundary labelled ``boundary_label``
    — i.e. the reach 'upstream of <label>' after a split/lake/border boundary. Returns its
    node_id (or None). This is how a reg 'X upstream of Y' resolves to a concrete node."""
    for nid, n in graph.nodes.items():
        if (n.kind == NodeKind.stream and n.blk == blk and n.lower_bound
                and n.lower_bound.label == boundary_label):
            return nid
    return None


def sections_in_reach(graph: StreamGraph, blk: str, m_lo: float, m_hi: float,
                      eps: float = 1e-6) -> frozenset[str]:
    """Stream-piece sections of ``blk`` whose span lies within [m_lo, m_hi] — for a range reg
    'X from A to C' (spans any lakes/sections between A and C). Bound measures come from a
    section's structured lower_bound/upper_bound.route_measure."""
    return frozenset(nid for nid, n in graph.nodes.items()
                     if n.kind == NodeKind.stream and n.blk == blk
                     and n.down_m >= m_lo - eps and n.up_m <= m_hi + eps)


def reach_except(graph: StreamGraph, base_ids, except_ids, guarded: bool = True) -> frozenset[str]:
    """The exact 'Includes Tributaries EXCEPT …' set: the tributary closure of the base waters
    MINUS the tributary closures of the excepted upstream reaches.

    Models 'ATNARKO/BELLA COOLA RIVERS [Includes Tributaries] EXCEPT: Burnt Bridge Cr. upstream of
    Sitkatapa Cr., Hunlen Cr. upstream of Hunlen Falls, Young Cr. upstream of Hwy 20'. Each EXCEPT
    is the upper piece a split already made (resolve via ``piece_above``); its exclusion set is
    that piece's own tributary closure. Pure set difference — the splits do all the structural
    work upstream (sectionizer), so nothing here re-walks geometry."""
    return frozenset(with_tributaries(graph, base_ids, guarded)
                     - with_tributaries(graph, except_ids, guarded))
