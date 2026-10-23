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

import functools
from dataclasses import dataclass, field

from pipeline.atlas.graph.graph import ancestors
from pipeline.common.models import NodeKind, StreamGraph
from pipeline.common import fwa_edge_types
from pipeline.common.utils import wsc

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
#: A WALK CONTINUES ACROSS WATER OUTSIDE B.C. (user ruling 2026-10-06, FIX A3/B4): the pieces past
#: the border carry no rule (`classify.OUTSIDE_BC_SUBTRACTED`), but a tributary walk goes on through
#: them and resumes where the stream comes back into B.C. FWA computes NO stream order past the
#: border — 50 of the 60 order-0 stream pieces of the live build are outside B.C., the rest are the
#: B.C. stubs of lines drawn mostly outside — so order 0 on a piece OUTSIDE B.C. (`out_of_bc`) is
#: UNKNOWN, as None is, and the Strahler guard never judges it (`_breaks_strahler`). Read as a real
#: order it made every re-entering piece "a bigger river" than the order-0 piece below it and stopped
#: 43 walks at the border: the Chilliwack tributary blk 356233151 lost Chilliwack/Vedder r1 above its
#: 424 m in Washington. INSIDE B.C. an order-0 stub is still judged as 0 (code review A-3): the guard
#: also keeps a reach's mouth seeding (`_mouths_at`) from taking the bigger river a stub flows into —
#: the Chehalis/Harrison defect class.
ORDER_ZERO_IS_UNKNOWN = True


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
        main = lake_main_outlet(graph, nid)
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
                if wsc.relation(a, b) not in (wsc.SAME, wsc.BELOW, wsc.UNKNOWN):
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


def lake_main_outlet(graph: StreamGraph, lake_id: str) -> frozenset[str]:
    """THE LAKE'S OWN RIVER LEAVING IT — the one owner of that answer (T117). The lake's outlet(s) of
    the largest Strahler order, and among equals those FWA draws as main flow
    (`fwa_edge_types.is_main_flow`); empty for a lake with no outlet.

    Two readers, which used to answer it two ways:
      * `code_runs`: only the OTHER outlets can be a code run. Without this the run followed a lake's
        main river too: Nanika Lake's outlet (the Nanika River, order 6) met a lake whose code the river
        does not descend from, and every walk from below lost the lake and all 491 sections above it;
        the white sturgeon rules lost 4,400. A bifurcation is a lake's SECONDARY outlet (Dewar Lake's
        side channel is FWA secondary flow, 1100/1150).
      * `_is_main_outlet` (the order guard's one exemption) asked "strictly larger than every other
        STREAM outlet", so where two outlets tied on order NEITHER was the lake's river — while
        `code_runs` took the main-flow one. Now both read this, and the tie goes to main flow.
    Empty for a node that is not a lake."""
    lake = graph.nodes.get(lake_id)
    outs = sorted({graph.edges[ei].to_node for ei in graph.down_adj.get(lake_id, [])} - {lake_id})
    if lake is None or lake.kind != NodeKind.lake or not outs:
        return frozenset()

    def order(o):
        return (graph.nodes[o].stream_order or 0) if o in graph.nodes else 0
    top = max(order(o) for o in outs)
    best = [o for o in outs if order(o) == top]
    flow = [o for o in best if fwa_edge_types.is_main_flow(graph.nodes.get(o))]
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

#: A TRIBUTARY WALK NEVER ENTERS THE ENTRY'S OWN WATER (2026-10-10). Every section of an item the
#: ENTRY matches is governed by the rule's EXTENT — where the rule's words put it — never by the walk
#: (AGENTS 13: "a rule's extents are its own"; RULINGS T9: a joining water is "neither the reach nor
#: one of its tributaries"). A rule naming one stretch of a row's water binds that stretch; the rest
#: of the row's water above or beside its cut is not a tributary of it, whatever FWA's codes say.
#: The walk is given that water as `own`: stepping from the reach STRAIGHT into it is the water going
#: on past the cut, so it is never walked (it and everything above it stay out); met further up a
#: true tributary (a lake the row names on one of its river's creeks) it is walked through and never
#: collected, like a `walk_past` carve-out.
#: The Chilliwack/Vedder row: FWA codes the Chilliwack as a tributary of the Vedder (100-064535-057628
#: -998041) and hangs its mouth on the Vedder's top piece by a `confluence` edge, so once the Vedder
#: Crossing cut took that piece into "downstream of Vedder Crossing Bridge" the six rules r4–r9 walked
#: up the Chilliwack and every stream feeding it — 1,820 sections for a 13 km stretch of the Vedder.
#: Switch it off and they return.
#: A SIDE CHANNEL IS THE REACH'S RIVER, NOT "OWN WATER PAST THE CUT": a piece carrying the SAME watershed
#: code as a reach piece is the river itself beside its line (the rule `confluence_joiners` already
#: uses), and the walk keeps it and what feeds it as before. Without this the Kootenay's "Idaho border
#: to Kootenay Lake" lost the Old Kootenay River Channel and 736 sections across the Creston flats draining
#: through it; the Morice, Atnarko, Zymoetz and Chowade lost side channels of their own stretch.
THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER = True


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


# --------------------------------------------------------------------------------------- #
# ONE ENTRY CHECK (T102 part B, owner 2026-10-22: "make the check logic (with the edge type check
# etc) a single function used in multiple spots"). Every rule that decides whether a walk may step
# from one water up into the water flowing into it lives in `may_enter`: the walk's every step, every
# mouth seeded at a cut, every piece of a lake's code run, and every other walk that climbs a water
# (`subtree`, T117) ask it, and each refusal comes back as one of the named reasons below — so a
# diagnostic, the T57 sweep and a person reading either share one vocabulary.
# --------------------------------------------------------------------------------------- #

#: `may_enter`'s verdicts. None is "enter, and collect it".
CARVED_OUT = "carved_out"              # `blocked`: a carve-out, or a confluence cut's joining water
CODE_DIVIDE = "code_divide"            # the edge a lake's code run crosses into another basin (`code_runs`)
RIVER_CONTINUING = "river_continuing"  # the reach's own river going on past its top, or through its lake
OWN_WATER = "own_water"                # the entry's own water past the cut (`THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER`)
NOT_IN_GRAPH = "not_in_graph"          # an edge to a node the graph does not hold
BARRIER = "barrier"                    # a 2300 connector (a canal), and so everything above it
RECEIVING_RIVER = "receiving_river"    # a code strictly above the reach's (`THE_WALK_NEVER_ENTERS_A_PARENT`)
BIGGER_RIVER = "bigger_river"          # the order guard, where the code cannot say
#: NOT a refusal: a lake mid-reach (`_lake_mid_reach`) is entered as the river passing through it —
#: walked, never collected.
PASSING_THROUGH = "river_passing_through"

#: Every refusal, in the order `may_enter` asks.
REFUSALS = (CARVED_OUT, CODE_DIVIDE, RIVER_CONTINUING, OWN_WATER, NOT_IN_GRAPH, BARRIER,
            RECEIVING_RIVER, BIGGER_RIVER)

#: How a step is taken — what `may_enter` asks of it:
FLOW = "flow"   # a walk step up a flow edge: everything
SEED = "seed"   # a mouth at the reach's cut (`_mouths_at`), the walk's first step: no "river continuing"
#                 (the seed is on another blue line, by a confluence)
RUN = "run"     # a piece of a lake's code run (`code_runs`): FWA's membership, not a flow step — the order
#                 guard does not judge it, and a run is never at the reach's boundary
UP = "up"       # everything above a water (`subtree`): its own mainstem is in, so no "river continuing"


@dataclass
class Reach:
    """What a walk carries — `may_enter`'s context. Built by `reach_context`.

    `reach` the sections the walk starts from; `codes` their watershed codes; `blocked` never entered;
    `own` the entry's own water (`THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER`); `guarded` the barrier and
    order guards (False only in tests); `cuts` the code runs' divides; `online` the lakes the walk entered
    as the river passing through, each to the reach piece below it (filled by the walk)."""
    graph: StreamGraph
    reach: frozenset
    codes: frozenset
    blocked: frozenset = frozenset()
    own: frozenset = frozenset()
    guarded: bool = True
    cuts: frozenset = frozenset()
    reach_blks: frozenset = frozenset()
    online: dict = field(default_factory=dict)
    _through: dict = field(default_factory=dict)

    def at_boundary(self, node: str) -> bool:
        return node in self.reach or node in self.online

    def through(self, lake: str) -> frozenset:
        """The blue lines the boundary lake `lake`'s own river leaves by — empty for a lake on a
        tributary of the reach's river (`_on_a_tributary_of_the_reach`). Once per lake."""
        got = self._through.get(lake)
        if got is None:
            got = _through_blks(self.graph, lake)
            if got and lake in self.reach and NAMED_LAKE_ON_A_TRIBUTARY_IS_CLIMBED \
                    and _on_a_tributary_of_the_reach(self.graph, lake, got, self.reach,
                                                     self.reach_blks):
                got = frozenset()             # its inflow on that line is a tributary too
            self._through[lake] = got
        return got


def reach_context(graph: StreamGraph, reach, *, blocked=frozenset(), own=frozenset(),
                  guarded: bool = True) -> Reach:
    """The `Reach` a walk from `reach` carries. A piece of `own` sharing a reach piece's watershed
    code is a side channel of the reach's own river, not own water past the cut."""
    reach = frozenset(reach)
    codes = _reach_codes(graph, reach)
    if THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER and own:
        own = frozenset(s for s in own if s not in reach and not (
            (n := graph.nodes.get(s)) is not None and n.wsc and n.wsc in codes))
    else:
        own = frozenset()
    _runs, cuts = code_runs(graph) if BIFURCATIONS_FOLLOW_THE_CODE else ({}, frozenset())
    reach_blks = frozenset(n.blk for r in reach if (n := graph.nodes.get(r)) is not None
                           and n.kind == NodeKind.stream and n.blk)
    return Reach(graph=graph, reach=reach, codes=codes, blocked=frozenset(blocked), own=own,
                 guarded=guarded, cuts=cuts, reach_blks=reach_blks)


def may_enter(ctx: Reach, src: str, dst: str, edge=None, *, via: str = FLOW) -> str | None:
    """May a walk carrying `ctx` step from `dst` up into `src` (which flows into it, by `edge`)?
    None: yes, and `src` is collected. Otherwise the reason, one of `REFUSALS` — or
    `PASSING_THROUGH`: entered as the river passing through a lake mid-reach, never collected.

    THE ONE PLACE the walk's entry rules live, asked in this order:
      `CARVED_OUT`        `src` is blocked;
      `CODE_DIVIDE`       `edge` is where a lake's code run leaves its basin (`code_runs`);
      `RIVER_CONTINUING`  at the reach's boundary (`FLOW` only): the reach's own river going on — a
                          mainstem edge (`MAINSTEM_EDGE_KINDS`) above a stream, the inflow on a boundary
                          lake's through line or the next lake up it (`_through_blks`, `_lake_on_line`);
                          unless the lake carries the reach on above it (`PASSING_THROUGH`);
      `OWN_WATER`         at the boundary, the entry's own water past the cut;
      `NOT_IN_GRAPH`, `BARRIER` (guarded);
      `RECEIVING_RIVER`, `BIGGER_RIVER`  the code, then the order where the code cannot say (`_refusal`;
                          a code run's piece is not judged by order).
    """
    g = ctx.graph
    if src in ctx.blocked:
        return CARVED_OUT
    if edge is not None and (edge.from_node, edge.to_node) in ctx.cuts:
        return CODE_DIVIDE
    boundary = ctx.at_boundary(dst)
    if via == FLOW and boundary and src not in ctx.reach:
        why = _continuing(ctx, src, dst, edge)
        if why is not None:
            return why
    if boundary and src in ctx.own:
        return OWN_WATER
    n = g.nodes.get(src)
    if n is None:
        return NOT_IN_GRAPH
    if ctx.guarded and n.is_barrier:
        return BARRIER                          # the canal, and everything above it
    return _refusal(g, src, dst, ctx.codes, guarded=ctx.guarded, by_order=via != RUN)


def _continuing(ctx: Reach, src: str, dst: str, edge) -> str | None:
    """Leaving the reach through its own top — `may_enter`'s `RIVER_CONTINUING`. "The reach's own
    flow continuing" takes two forms, and which one applies depends on what `dst` is:

      STREAM boundary — the `continuation` / `lake_out` edge above it.
      LAKE boundary   — the inflow on the same blue line as the lake's OUTflow, i.e. the river
                        running through the lake — arriving as a stream on that line, or as the
                        next LAKE up it (a chain of reservoirs, `_lake_on_line`).

    Asked ONLY at the boundary: the same edge kind inside a tributary is that tributary's own
    continuation and must be followed. A lake with the reach carrying on above it is the river
    passing through (`LAKE_MID_REACH_IS_THE_RIVER`)."""
    g = ctx.graph
    sn = g.nodes.get(src)
    src_is_lake = sn is not None and sn.kind == NodeKind.lake
    dn = g.nodes.get(dst)
    if dn is not None and dn.kind == NodeKind.lake:
        through = ctx.through(dst)
        if sn is not None and sn.blk and sn.blk in through:
            return RIVER_CONTINUING
        if src_is_lake and _lake_on_line(g, src, through):
            if LAKE_MID_REACH_IS_THE_RIVER and dst in ctx.online \
                    and _lake_mid_reach(g, src, ctx.reach, ctx.online[dst]):
                return PASSING_THROUGH        # the next lake of a chain, mid-reach
            return RIVER_CONTINUING
        return None
    if edge is not None and edge.kind in MAINSTEM_EDGE_KINDS:
        if src_is_lake and LAKE_MID_REACH_IS_THE_RIVER and _lake_mid_reach(g, src, ctx.reach, dst):
            return PASSING_THROUGH            # the river passing through a lake
        return RIVER_CONTINUING
    return None


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
    own: set[str] | frozenset[str] = frozenset(),
) -> frozenset[str]:
    """Sections draining INTO `reach`, recursively. Excludes `reach` itself.

    `own` is the rest of the ENTRY'S OWN water (`THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER`): never
    returned; never walked when the reach joins it directly (the water going on past the cut);
    walked through, uncollected, when met above a true tributary. A piece sharing a reach piece's
    watershed code is a side channel of the reach's own river and is not `own`.

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

    Whether a step is taken is `may_enter`'s alone; this function only keeps the books.
    """
    ctx = reach_context(graph, reach, blocked=blocked, own=own, guarded=guarded)
    out: set[str] = set()
    seen: set[str] = set(ctx.reach)
    stack: list[str] = sorted(ctx.reach)      # sorted: determinism, not correctness
    runs = code_runs(graph)[0] if BIFURCATIONS_FOLLOW_THE_CODE else {}

    # A stream joining EXACTLY at the reach's lower cut belongs to the reach. When a
    # curator anchors a cut on a confluence — "upstream of the confluence with Slesse
    # Creek" — that creek's mouth sits on the point. FWA may attach it to the piece BELOW
    # the cut, in which case the walk would never see it and the named water would be
    # missing from the very reach it defines. Rare (51 of 196,596 bounded sections) and
    # entirely concentrated on confluence-anchored cuts, which is where it matters most.
    # Each seed has passed `may_enter` inside `_mouths_at`: a seed is the walk's first step (T102).
    for seed in sorted(_mouths_at_lower_bound(graph, ctx.reach, window, ctx)):
        if seed not in seen:
            seen.add(seed)
            out.add(seed)
            stack.append(seed)

    while stack:
        node = stack.pop()
        # A lake the walk reached (not the reach's own water, not the river passing through)
        # brings the runs FWA codes to it, with whatever joins them.
        if not ctx.at_boundary(node):
            for r in runs.get(node, ()):
                if r in seen or may_enter(ctx, r, node, via=RUN) is not None:
                    continue
                seen.add(r)
                out.add(r)
                stack.append(r)
        for ei in graph.up_adj.get(node, []):
            e = graph.edges[ei]
            src = e.from_node
            if src in seen:
                continue
            why = may_enter(ctx, src, node, e)
            if why == PASSING_THROUGH:
                seen.add(src)                  # walked as the river, never collected
                ctx.online[src] = ctx.online.get(node, node)
                stack.append(src)
            elif why is None:
                seen.add(src)
                out.add(src)
                stack.append(src)

    return frozenset(out - frozenset(passed) - ctx.own)


def _mouths_at_lower_bound(graph: StreamGraph, reach: frozenset[str],
                           window: tuple[str, float, float] | None = None,
                           ctx: Reach | None = None) -> set[str]:
    """Tributary mouths sitting exactly on the reach's lower cut.

    Looked for on the piece BELOW the cut: the confluence is at the same route measure, so
    FWA may hang the mouth on either side of it. Only tributaries are taken — anything on
    the reach's own blue line is the mainstem, not a joining stream.

    `window` is the `(blk, lo, hi)` the extent ACTUALLY resolved to, handed over by
    `resolve_extent`. Prefer it: re-deriving the cut from node bounds means two places have
    to agree about where the reach starts, and they can drift. Node bounds remain the
    fallback for a reach that has no single window (a `whole` extent, or a `between`
    spanning two blue lines).

    `ctx` is the walk's `Reach` (built from `reach` alone when not given): each mouth must pass
    `may_enter` with it.
    """
    out: set[str] = set()
    if ctx is None:
        ctx = reach_context(graph, reach)
    if window is not None:
        w_blk, w_lo, _w_hi = window
        for nid in reach:
            n = graph.nodes.get(nid)
            if n is None or n.blk != w_blk:
                continue
            out |= _mouths_at(graph, nid, n, w_lo, reach, ctx)
        return out
    for nid in reach:
        n = graph.nodes.get(nid)
        if n is None or n.lower_bound is None or not n.blk:
            continue
        m = n.lower_bound.route_measure
        out |= _mouths_at(graph, nid, n, m, reach, ctx)
    return out


def _mouths_at(graph: StreamGraph, nid: str, n, m: float,
               reach: frozenset[str], ctx: Reach | None = None) -> set[str]:
    """Tributary mouths on the piece immediately BELOW `nid`, at route measure `m`.

    THE STRAHLER GUARD APPLIES HERE TOO. Everything joining at the confluence is a sibling of
    the reach, and one of those siblings is the river the reach FLOWS INTO — it arrives on its
    own blue line, by a `confluence` edge, at exactly this measure, so every other test here
    passes it. The walk below rejects that case with its order guard ("a bigger river cannot
    be a tributary"); the seeding did not, and so a rule reaching the mouth of its own water
    picked up the receiving river.

    That is how "No Fishing downstream of the main logging road bridge, May 1-31" on the
    CHEHALIS — a `downstream_of` whose window runs to measure 0, i.e. to the mouth — bound two
    sections of the HARRISON, which is the river the Chehalis empties into.

    The Strahler guard did not end that (T90): the two Harrison pieces are SIDE CHANNELS, order
    2 against the Chehalis's 5, and were reached another way — see `MOUTH_SEEDS_ONLY_AT_THE_CUT`.
    A seed is the walk's first step, so it takes the walk's own test, `may_enter` (T102): the
    receiving river by its code (`THE_WALK_NEVER_ENTERS_A_PARENT`), a bigger river by its order
    where the code cannot say, a carve-out, the entry's own water, a canal.
    """
    out: set[str] = set()
    if ctx is None:
        ctx = reach_context(graph, reach)
    downs = [graph.edges[ei] for ei in graph.down_adj.get(nid, [])]
    if MOUTH_SEEDS_ONLY_AT_THE_CUT:
        downs = [e for e in downs
                 if (b := graph.nodes.get(e.to_node)) is not None and b.blk == n.blk]
        if not downs or any(e.to_node in reach for e in downs):
            return out                 # the reach's mouth, or its line goes on below it in the reach
    for e in downs:
        for fi in graph.up_adj.get(e.to_node, []):
            f = graph.edges[fi]
            src = graph.nodes.get(f.from_node)
            if (f.from_node not in reach and src is not None and src.blk != n.blk
                    and f.kind == "confluence" and abs(f.at_measure - m) < 0.5
                    and may_enter(ctx, f.from_node, nid, f, via=SEED) is None):
                out.add(f.from_node)
    return out


#: TRIBUTARY MOUTHS ARE SEEDED ONLY AT A CUT, FROM THE PIECE BELOW IT ON THE REACH'S OWN LINE (T90,
#: 2026-10-22). The seeding exists for a cut anchored on a confluence ("upstream of the confluence
#: with Slesse Creek") whose creek FWA hung on the piece BELOW the cut; the measure it compares is
#: the cut's, on the reach's line. So: only the reach piece AT the cut seeds (not one whose line goes
#: on below it inside the reach), only through the piece below it ON ITS LINE (never a distributary
#: leaving it for another line), and never at the reach's natural MOUTH, where nothing of its line
#: lies below and everything arriving is joining the receiving river, not the reach — the no-window
#: path (`_mouths_at_lower_bound` without a window) already skipped a piece with no lower bound.
#: Before this, every reach piece on the window's line compared the window's lower measure with the
#: measures of WHATEVER line it flowed into. The Chehalis piece above the mouth piece
#: (356364028:3592) splits into a Harrison side channel (355992873:0) at 865 m on THAT channel; a
#: second channel (355992877:0) joins the first at its measure 0.0 — and the "downstream of the
#: bridge" window also starts at 0, the Chehalis's mouth. So the second channel was seeded and the
#: walk climbed the first. At a mouth the window's 0 matched water joining the RECEIVING river at
#: the receiving river's own measure 0, kilometres away. Switch it off and both come back.
MOUTH_SEEDS_ONLY_AT_THE_CUT = True

#: THE WALK NEVER ENTERS A PARENT (T102, owner 2026-10-22: "only use the wsc check parent/child to
#: handle it during the walk — no walking into a parent"). A water whose watershed code is a STRICT
#: ANCESTOR of the reach's ("100-077501" the Harrison over "100-077501-094860" the Chehalis) is the
#: river the reach drains into, or one of its side channels — which carry its code at a small order
#: and so pass the order guard. The walk never steps into one, at any order, and so never climbs
#: what feeds it. A seed is the walk's first step and asks the same (`_mouths_at`), and so does a
#: lake's code run (`code_runs`: a run flowing down through the receiving river leaves it out), which is why
#: T90's seeding-only `THE_RECEIVING_RIVER_IS_NEVER_A_SEED` is gone. A code EQUAL to one of the
#: reach's is the reach's own river beside its line (a side channel), never a parent — on a reach of
#: two rivers coded one inside the other (the Vedder and the Chilliwack) the outer river is the
#: reach's own. Where a code is missing, or WSC is silent (equal codes: 28,359 of the live graph's
#: edges the order guard refuses are equal-code floodplain pieces), the order guard decides.
THE_WALK_NEVER_ENTERS_A_PARENT = True


def _reach_codes(graph: StreamGraph, reach) -> frozenset[str]:
    """The watershed codes the reach's own pieces carry."""
    return frozenset(n.wsc for r in reach if (n := graph.nodes.get(r)) is not None and n.wsc)


@functools.lru_cache(maxsize=1024)
def _strict_ancestors(codes: frozenset[str]) -> frozenset[str]:
    """Every strict ancestor of any of `codes` that is not itself one of them."""
    return frozenset(a for c in codes for a in wsc.ancestors(c)) - codes


def _receives(src, codes: frozenset[str]) -> bool:
    """Does `src` carry the code of a water the reach drains into (a strict ancestor of a reach
    code, equal to none)?"""
    a = getattr(src, "wsc", "") or ""
    return bool(a and codes and a in _strict_ancestors(codes))


def _refusal(graph: StreamGraph, src: str, dst: str, codes: frozenset[str], *,
             guarded: bool = True, by_order: bool = True) -> str | None:
    """The CODE-AND-ORDER half of `may_enter` (which asks it last): why a walk may not step from
    `dst` up into `src` (which flows into it) — or None. Called by `may_enter` only.

    The code is asked first:
      `receiving_river` — `src`'s code is a strict ancestor of the reach's
        (`THE_WALK_NEVER_ENTERS_A_PARENT`); not a guard, so `guarded=False` keeps it;
      a code extending `dst`'s is a genuine tributary, whatever its order (`_code_says_tributary`);
      `bigger_river` — otherwise (a code missing, or equal codes: WSC silent), the order guard: a
        strictly larger river is no tributary, unless it is a lake's own main outlet
        (`_is_main_outlet`). `guarded=False` drops it; so does `by_order=False` (a code run's piece).
    """
    s, d = graph.nodes.get(src), graph.nodes.get(dst)
    if THE_WALK_NEVER_ENTERS_A_PARENT and _receives(s, codes):
        return RECEIVING_RIVER
    if not by_order or _code_says_tributary(s, d):
        return None
    if guarded and _order_breaks(s, d) and not _is_main_outlet(graph, src, dst):
        return BIGGER_RIVER
    return None


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
    small creek the lake's whole catchment. The main outlet is `lake_main_outlet` — the one owner of
    "which outlet is the lake's river" — and must carry the lake's own watershed code: an outlet whose
    code differs (Burnaby Lake "draining" into Still Creek, 100-019698 vs 100-019698-999999) is not the
    lake's line, and the guard stands.
    """
    lake = graph.nodes.get(lake_id)
    out = graph.nodes.get(out_id)
    if lake is None or out is None or lake.kind != NodeKind.lake or out.kind == NodeKind.lake:
        return False
    if not lake.wsc or lake.wsc != out.wsc:
        return False
    return out_id in lake_main_outlet(graph, lake_id)


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

    Unknown orders are never judged — absence of evidence is not evidence of an artifact. Order 0
    on a piece outside B.C. is unknown too (`ORDER_ZERO_IS_UNKNOWN`: FWA computes none past the
    border); inside B.C. it is judged as 0.
    """
    return _order_breaks(upstream, downstream) and not _code_says_tributary(upstream, downstream)


def _code_says_tributary(upstream, downstream) -> bool:
    """Does `upstream`'s watershed code extend `downstream`'s — FWA saying "genuine tributary"?
    Then order is never judged: the code wins."""
    if upstream is None or downstream is None:
        return False
    return wsc.is_strictly_below(upstream.wsc, downstream.wsc)


def _order_breaks(upstream, downstream) -> bool:
    """The ORDER half of the guard: `upstream` is a strictly larger river than `downstream`."""
    if upstream is None or downstream is None:
        return False
    a, b = upstream.stream_order, downstream.stream_order
    if ORDER_ZERO_IS_UNKNOWN and ((a == 0 and getattr(upstream, "out_of_bc", False))
                                  or (b == 0 and getattr(downstream, "out_of_bc", False))):
        return False                    # FWA has no order past the border: unknown there
    return not (a is None or b is None or a <= b)


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
    own: set[str] | frozenset[str] = frozenset(),
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
                                        guarded=guarded, window=window, own=own)
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
