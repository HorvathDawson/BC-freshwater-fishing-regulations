"""Reach-scoped tributary expansion — the highest-stakes logic in the pipeline.

THE TWO CROSS-CHECKS THAT USED TO LIVE HERE ARE GONE, and they were never telling the truth.
They compared `tributaries_of_reach` against `lake_tributaries` and `tributaries_between` —
two functions in `atlas/graph/tributaries.py`, a module with no production caller. Both were
red, and for a reason that was not a defect: they passed `guarded=False` on one side, and
that flag switches off the through-mainstem exclusion in the reach walk while the older
functions drop it unconditionally, so every lake with a river running through it disagreed
by that river's entire catchment. Skeezer Lake, 112 sections against 61.

Those two functions are now spellings of `tributaries_of_reach` over a one-section reach, so
a cross-check between them would compare a function to itself. What was being cross-checked
is gone; what it was checking against was never shipped.

Getting this wrong means telling someone water is closed when it is open, or open when it
is closed, over potentially hundreds of thousands of sections. Every guard below exists
because a real leak was measured, and each has a named regression.

Synthetic fixtures first (they can express a topology exactly), then real-data regressions
marked `slow`.
"""

from __future__ import annotations

import dataclasses

import pytest

from pipeline.common.models import (
    BoundaryKind, FlowEdge, NodeKind, SectionBoundary, StreamGraph, StreamNode,
)
from pipeline.atlas.graph.tributaries import (
    _breaks_strahler, _mouths_at_lower_bound, expand, tributaries_of_reach,
)
from pipeline.common.curated import GENERATED
from pipeline.tests.conftest import need, ATLAS_HINT


def _n(nid, *, order=1, blk=None, barrier=False, kind=NodeKind.stream, lower=None):
    return StreamNode(node_id=nid, kind=kind, blk=blk or nid.split(":")[0],
                      down_m=0.0, up_m=100.0, length_m=100.0,
                      stream_order=order, lower_bound=lower,
                      edge_types=("2300",) if barrier else ())


def _g(nodes, edges):
    """edges: [(from, to, kind)] or [(from, to, kind, at_measure)] — `from` flows INTO `to`."""
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=e[0], to_node=e[1], kind=e[2],
                        at_measure=(e[3] if len(e) > 3 else 0.0)) for e in edges]
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


# --------------------------------------------------------------------------- #
# The core question: what belongs to a REACH, not to the river
# --------------------------------------------------------------------------- #

@pytest.fixture
def river():
    """A mainstem of three pieces, with a tributary on each, plus water above the top.

        above  (mainstem, order 5)      <- NOT a tributary of the reach
          |  continuation
        m_top   <- trib_top                   reach
          |  continuation
        m_mid   <- trib_mid  <- trib_mid_up   reach     (trib of a trib)
          |  continuation
        m_low   <- trib_low                   reach
    """
    nodes = [_n("m_low", order=5), _n("m_mid", order=5), _n("m_top", order=5),
             _n("above", order=5), _n("above_trib", order=2),
             _n("trib_low", order=2), _n("trib_mid", order=2),
             _n("trib_mid_up", order=1), _n("trib_top", order=2)]
    edges = [("m_mid", "m_low", "continuation"),
             ("m_top", "m_mid", "continuation"),
             ("above", "m_top", "continuation"),
             ("above_trib", "above", "confluence"),
             ("trib_low", "m_low", "confluence"),
             ("trib_mid", "m_mid", "confluence"),
             ("trib_mid_up", "trib_mid", "continuation"),
             ("trib_top", "m_top", "confluence")]
    return _g(nodes, edges)


REACH = {"m_low", "m_mid", "m_top"}


def test_tributaries_joining_inside_the_reach_are_included(river):
    assert tributaries_of_reach(river, REACH) >= {"trib_low", "trib_mid", "trib_top"}


def test_the_mainstem_ABOVE_the_reach_is_excluded(river):
    """The single most important exclusion: the river carrying on above the regulated
    stretch is not a tributary OF that stretch."""
    got = tributaries_of_reach(river, REACH)
    assert "above" not in got


def test_water_joining_the_mainstem_ABOVE_the_reach_is_excluded(river):
    """It drains through the reach, but you would be fishing outside the closure."""
    assert "above_trib" not in tributaries_of_reach(river, REACH)


def test_the_walk_is_recursive_a_tributary_of_a_tributary_counts(river):
    assert "trib_mid_up" in tributaries_of_reach(river, REACH)


def test_the_reach_itself_is_never_returned_as_its_own_tributary(river):
    assert not (tributaries_of_reach(river, REACH) & REACH)


def test_a_continuation_INSIDE_a_tributary_is_still_followed(river):
    """The mainstem rule applies only at the reach boundary. `trib_mid_up` joins
    `trib_mid` by a continuation edge; refusing it everywhere would truncate every
    tributary at its first piece."""
    assert "trib_mid_up" in tributaries_of_reach(river, REACH)


def test_a_shorter_reach_gets_fewer_tributaries(river):
    """Scope is relative to the extent (doc 10 ③): the whole point."""
    whole = tributaries_of_reach(river, REACH)
    just_low = tributaries_of_reach(river, {"m_low"})
    assert "trib_low" in just_low
    assert "trib_mid" not in just_low and "trib_top" not in just_low
    assert just_low < whole


# --------------------------------------------------------------------------- #
# Guards
# --------------------------------------------------------------------------- #

def test_a_2300_barrier_stops_the_walk_and_is_itself_excluded():
    """Spike S2: blocking 2300 canal nodes took the Columbia/Kootenay leak 86 -> 0."""
    g = _g([_n("main", order=5), _n("canal", order=3, barrier=True), _n("beyond", order=3)],
           [("canal", "main", "confluence"), ("beyond", "canal", "continuation")])
    guarded = tributaries_of_reach(g, {"main"})
    assert guarded == frozenset()
    assert tributaries_of_reach(g, {"main"}, guarded=False) == {"canal", "beyond"}


def test_a_bigger_river_is_not_a_tributary_of_a_smaller_one():
    """The McLennan Creek leak: FWA records the Fraser (order 10) flowing INTO McLennan
    Creek (order 4) on the floodplain, which absorbed 419,078 sections — 20.6% of BC."""
    g = _g([_n("creek", order=4), _n("bigriver", order=10), _n("bigriver_up", order=10),
            _n("realtrib", order=2)],
           [("bigriver", "creek", "confluence"),
            ("bigriver_up", "bigriver", "continuation"),
            ("realtrib", "creek", "confluence")])
    got = tributaries_of_reach(g, {"creek"})
    assert got == {"realtrib"}
    assert tributaries_of_reach(g, {"creek"}, guarded=False) == {
        "bigriver", "bigriver_up", "realtrib"}


def test_the_watershed_code_OVERRIDES_the_order_guard():
    """The two guards must never contradict. Where WSC says "this code extends mine, it IS
    a descendant", that is FWA's own hierarchy and it wins — even if the order looks
    backwards. 93 real edges depend on this, nearly all lakes, whose order aggregates every
    inflow and so runs ahead of their own outlet's first piece."""
    g = _g([_n("outlet_piece", order=1, blk="M"), _n("lake:9", order=5, blk="", kind=NodeKind.lake)],
           [("lake:9", "outlet_piece", "confluence")])
    g.nodes["outlet_piece"] = StreamNode(
        node_id="outlet_piece", kind=NodeKind.stream, blk="M", down_m=0.0, up_m=100.0,
        length_m=100.0, stream_order=1, wsc="100-458399")
    g.nodes["lake:9"] = StreamNode(
        node_id="lake:9", kind=NodeKind.lake, blk="", down_m=0.0, up_m=0.0, length_m=0.0,
        stream_order=5, wsc="100-458399-277002")          # a DESCENDANT code
    assert "lake:9" in tributaries_of_reach(g, {"outlet_piece"})


def test_the_order_guard_still_fires_when_the_code_is_SILENT():
    """Equal codes are exactly where the floodplain artifacts hide — FWA gives a big
    river's side channels the river's own code, which is how the Fraser reached McLennan
    Creek."""
    g = _g([_n("creek", order=4, blk="C"), _n("bigriver", order=10, blk="B")],
           [("bigriver", "creek", "confluence")])
    g.nodes["creek"] = StreamNode(node_id="creek", kind=NodeKind.stream, blk="C",
                                  down_m=0.0, up_m=100.0, length_m=100.0,
                                  stream_order=4, wsc="100")
    g.nodes["bigriver"] = StreamNode(node_id="bigriver", kind=NodeKind.stream, blk="B",
                                     down_m=0.0, up_m=100.0, length_m=100.0,
                                     stream_order=10, wsc="100")   # SAME code
    assert tributaries_of_reach(g, {"creek"}) == frozenset()


def test_equal_order_is_allowed_only_a_strict_increase_is_blocked():
    g = _g([_n("a", order=4), _n("b", order=4)], [("b", "a", "confluence")])
    assert tributaries_of_reach(g, {"a"}) == {"b"}


def test_unknown_stream_order_is_not_judged():
    """Absence of evidence is not evidence of an artifact."""
    b = StreamNode(node_id="b", kind=NodeKind.stream, blk="b", down_m=0.0, up_m=100.0,
                   length_m=100.0, stream_order=None)
    g = _g([_n("a", order=4), b], [("b", "a", "confluence")])
    assert tributaries_of_reach(g, {"a"}) == {"b"}


def test_a_self_edge_does_not_make_a_section_its_own_tributary():
    """470 self-edges exist in the real graph."""
    g = _g([_n("a", order=3)], [("a", "a", "confluence")])
    assert tributaries_of_reach(g, {"a"}) == frozenset()


def test_a_lake_above_the_reach_is_treated_as_mainstem_not_tributary():
    g = _g([_n("below", order=5), _n("lake:1", order=5, kind=NodeKind.lake),
            _n("feeder", order=2)],
           [("lake:1", "below", "lake_out"), ("feeder", "lake:1", "confluence")])
    assert tributaries_of_reach(g, {"below"}) == frozenset()


def test_a_cycle_terminates():
    g = _g([_n("a", order=3), _n("b", order=3), _n("c", order=3)],
           [("b", "a", "confluence"), ("c", "b", "confluence"), ("b", "c", "confluence")])
    assert tributaries_of_reach(g, {"a"}) == {"b", "c"}


# --------------------------------------------------------------------------- #
# tributaries_only, and carve-outs
# --------------------------------------------------------------------------- #

def test_tributaries_only_drops_the_mainstem_but_keeps_scoping_by_it(river):
    """44 rules are 'no fishing in tributaries above X'. The reach still decides WHICH
    tributaries; it is just not itself in the answer."""
    got = expand(river, REACH, only=True)
    assert not (got & REACH)
    assert {"trib_low", "trib_mid", "trib_mid_up", "trib_top"} <= got
    assert "above" not in got and "above_trib" not in got


def test_includes_tributaries_keeps_the_mainstem(river):
    got = expand(river, REACH, only=False)
    assert REACH <= got
    assert "trib_low" in got


def test_an_exclusion_removes_the_stream_AND_everything_above_it(river):
    """'EXCEPT Burnt Bridge Creek upstream of Sitkatapa Creek' must not leave the creek's
    own catchment behind."""
    got = expand(river, REACH, excluded={"trib_mid"})
    assert "trib_mid" not in got
    assert "trib_mid_up" not in got, "the walk must not descend through excluded water"
    assert "trib_low" in got


def test_an_exclusion_blocks_traversal_rather_than_subtracting_afterwards():
    """Subtracting after the fact would keep a catchment that drains ONLY through the
    excluded stream."""
    g = _g([_n("main", order=5), _n("gate", order=2), _n("behind", order=1)],
           [("gate", "main", "confluence"), ("behind", "gate", "continuation")])
    assert tributaries_of_reach(g, {"main"}) == {"gate", "behind"}
    assert tributaries_of_reach(g, {"main"}, blocked={"gate"}) == frozenset()


def test_expansion_is_deterministic(river):
    runs = [tuple(sorted(expand(river, REACH))) for _ in range(5)]
    assert len(set(runs)) == 1


# --------------------------------------------------------------------------- #
# A tributary sitting exactly ON the cut
# --------------------------------------------------------------------------- #

def _cut(m=500.0):
    return SectionBoundary(boundary_id="split:x", kind=BoundaryKind.split,
                           route_measure=m, label="x")


def test_a_creek_joining_AT_the_cut_belongs_to_upstream_of():
    """Curators anchor cuts ON confluences — "upstream of the confluence with Slesse
    Creek". FWA may hang that creek's mouth on the piece BELOW the cut, in which case a
    plain upstream walk never sees it and the water that NAMES the reach is missing from
    it."""
    g = _g([_n("upper", order=5, blk="M", lower=_cut()), _n("lower", order=5, blk="M"),
            _n("at_point", order=2, blk="T"), _n("at_point_up", order=1, blk="T")],
           [("upper", "lower", "continuation", 500.0),
            ("at_point", "lower", "confluence", 500.0),      # mouth on the LOWER piece
            ("at_point_up", "at_point", "continuation", 0.0)])
    got = tributaries_of_reach(g, {"upper"})
    assert "at_point" in got
    assert "at_point_up" in got, "and everything above it comes too"


def test_a_creek_joining_elsewhere_below_the_cut_stays_out():
    """The rule is 'at the cut', not 'anywhere below it'."""
    g = _g([_n("upper", order=5, blk="M", lower=_cut()), _n("lower", order=5, blk="M"),
            _n("other", order=2, blk="T")],
           [("upper", "lower", "continuation", 500.0),
            ("other", "lower", "confluence", 120.0)])
    assert "other" not in tributaries_of_reach(g, {"upper"})


def test_the_mainstem_below_the_cut_is_never_pulled_in_by_this():
    """Only a DIFFERENT blue line counts as joining; the reach's own line below the cut is
    mainstem however close the measure is."""
    g = _g([_n("upper", order=5, blk="M", lower=_cut()), _n("lower", order=5, blk="M"),
            _n("below2", order=5, blk="M")],
           [("upper", "lower", "continuation", 500.0),
            ("below2", "lower", "confluence", 500.0)])
    assert tributaries_of_reach(g, {"upper"}) == frozenset()


# --------------------------------------------------------------------------- #
# Lakes — a lake is a node, and "the river through it" is not its tributary
# --------------------------------------------------------------------------- #

@pytest.fixture
def lake():
    """A river running THROUGH a lake, plus a side creek and a side lake.

        river_up (blk R)                side_creek (blk S)     side_lake
             |  lake_in                       |  lake_in          |  lake_out
             +------------> lake:1 <----------+<------------------+
                              |  lake_out
                          river_dn (blk R)          <- same blue line as river_up
    """
    nodes = [_n("lake:1", order=6, blk="", kind=NodeKind.lake),
             _n("river_up", order=6, blk="R"), _n("river_up2", order=6, blk="R"),
             _n("river_dn", order=6, blk="R"),
             _n("side_creek", order=2, blk="S"), _n("side_creek_up", order=1, blk="S"),
             _n("side_lake", order=2, blk="", kind=NodeKind.lake)]
    edges = [("lake:1", "river_dn", "lake_out"),
             ("river_up", "lake:1", "lake_in"),
             ("river_up2", "river_up", "continuation"),
             ("side_creek", "lake:1", "lake_in"),
             ("side_creek_up", "side_creek", "continuation"),
             ("side_lake", "lake:1", "lake_out")]
    return _g(nodes, edges)


def test_the_river_running_THROUGH_a_lake_is_not_its_tributary(lake):
    """Lake Koocanusa swallowed the whole Kootenay above it — 36,990 sections instead of
    10,570 — before this rule existed. The inflow sharing the OUTflow's blue line is the
    river continuing through, not water joining."""
    got = tributaries_of_reach(lake, {"lake:1"})
    assert "river_up" not in got
    assert "river_up2" not in got, "and nor is anything above it"


def test_a_lake_keeps_its_genuine_side_tributaries(lake):
    got = tributaries_of_reach(lake, {"lake:1"})
    assert {"side_creek", "side_creek_up"} <= got


def test_a_side_lake_draining_in_is_a_tributary_of_the_lake(lake):
    """It arrives on a different blue line from the outflow, so it is joining, not
    continuing. The stream-boundary `lake_out` rule must not fire here — which is why the
    boundary rule is chosen by the NODE's kind, not the edge's."""
    assert "side_lake" in tributaries_of_reach(lake, {"lake:1"})


def test_a_lake_ABOVE_a_stream_reach_is_still_mainstem(lake):
    """The mirror case: standing on a stream, a lake above it is the river continuing."""
    assert tributaries_of_reach(lake, {"river_dn"}) == frozenset()


def test_a_reach_spanning_the_lake_keeps_the_river_above_out(lake):
    got = tributaries_of_reach(lake, {"lake:1", "river_dn"})
    assert "river_up" not in got and "river_up2" not in got
    assert {"side_creek", "side_lake"} <= got


# --------------------------------------------------------------------------- #
# A chain of reservoirs — lakes draining straight into lakes, one dam apart
# --------------------------------------------------------------------------- #

@pytest.fixture
def chain():
    """Kinbasket -> Revelstoke -> Upper Arrow -> Lower Arrow, in miniature: three lakes on one
    river R, each draining STRAIGHT into the next (no stream between — a dam), plus a
    tributary lake T that also drains straight in, fed by its own creek.

        river_up (R) <- river_up_trib
             | lake_in
          lake:top  <- side_top (S1)
             | lake_out                      (a dam: lake to lake)
          lake:mid  <- side_mid (S2)
             | lake_out
          lake:low  <- side_low (S3)
             | lake_out  ^  lake_out
          river_dn (R)   trib_lake <- trib_in (T)
    """
    L = NodeKind.lake
    nodes = [_n("lake:top", order=8, blk="", kind=L), _n("lake:mid", order=8, blk="", kind=L),
             _n("lake:low", order=8, blk="", kind=L), _n("trib_lake", order=3, blk="", kind=L),
             _n("river_up", order=7, blk="R"), _n("river_up_trib", order=3, blk="U"),
             _n("river_dn", order=8, blk="R"),
             _n("side_top", order=3, blk="S1"), _n("side_mid", order=3, blk="S2"),
             _n("side_low", order=3, blk="S3"), _n("trib_in", order=3, blk="T")]
    edges = [("river_up_trib", "river_up", "confluence"),
             ("river_up", "lake:top", "lake_in"),
             ("side_top", "lake:top", "lake_in"),
             ("lake:top", "lake:mid", "lake_out"),
             ("side_mid", "lake:mid", "lake_in"),
             ("lake:mid", "lake:low", "lake_out"),
             ("side_low", "lake:low", "lake_in"),
             ("trib_in", "trib_lake", "lake_in"),
             ("trib_lake", "lake:low", "lake_out"),
             ("lake:low", "river_dn", "lake_out")]
    return _g(nodes, edges)


def test_the_walk_stops_at_the_next_lake_up_the_chain(chain):
    """LOWER ARROW'S TRIBUTARIES held every section of Upper Arrow's, Lake Revelstoke's and
    Kinbasket's (50,722): the lake above arrives by a lake-to-lake edge, a lake carries no blk,
    so the through-river test never matched and the whole chain was walked as a tributary."""
    got = tributaries_of_reach(chain, {"lake:low"})
    assert got == {"side_low", "trib_lake", "trib_in"}
    assert "lake:mid" not in got and "side_mid" not in got
    assert not {"lake:top", "side_top", "river_up", "river_up_trib"} & got


def test_a_lake_draining_into_a_lake_still_has_a_through_river(chain):
    """KINBASKET drains through Mica Dam straight into Lake Revelstoke, so it has no stream
    outlet of its own; its through-line is found down the chain (R, below lake:low). Without
    it, the river arriving at the top of the chain is a 'tributary' — the Columbia above
    Kinbasket, which Kinbasket's row prints it does NOT include."""
    got = tributaries_of_reach(chain, {"lake:top"})
    assert got == {"side_top"}
    got = tributaries_of_reach(chain, {"lake:mid"})
    assert got == {"side_mid"}, "the lake above is the river arriving, not a tributary"


def test_a_tributary_lake_draining_straight_in_is_still_a_tributary(chain):
    """The lake-to-lake rule is decided by the river's LINE, never by the edge: trib_lake is
    fed on T, not R, so it is water joining lake:low and stays in, with its creek."""
    assert {"trib_lake", "trib_in"} <= tributaries_of_reach(chain, {"lake:low"})


def test_a_stream_reach_bounded_by_a_lake_does_not_climb_into_it(chain):
    """A `between` / `upstream_of` reach ends at its upper cut. Standing on the stream below a
    dam, the lake above is the river continuing (`lake_out` is a mainstem edge) — the walk
    stops there, so a reach cut at a dam collects only what joins the reach itself."""
    assert tributaries_of_reach(chain, {"river_dn"}) == frozenset()
    assert tributaries_of_reach(chain, {"river_up"}) == {"river_up_trib"}


def test_the_whole_chain_as_one_reach_takes_every_side_tributary(chain):
    """A row that names every lake in the chain is about all of them: each lake's own side
    water is in, the river above the top lake is not."""
    got = tributaries_of_reach(chain, {"lake:top", "lake:mid", "lake:low"})
    assert got == {"side_top", "side_mid", "side_low", "trib_lake", "trib_in"}


# --------------------------------------------------------------------------- #
# Real-data regressions
# --------------------------------------------------------------------------- #

@pytest.fixture(scope="module")
def real(request):
    import sys
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    need(request, "atlas", GENERATED.build() / "graph.pkl", ATLAS_HINT)
    sys.path.insert(0, str(root / "curation-review/backend"))
    import reuse
    return reuse._graph(), reuse._registry()


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_two_guards_never_contradict_on_real_data(real):
    """Every edge the order guard blocks must be one the watershed code is SILENT on
    (equal codes) or would itself have blocked. If WSC calls something a descendant, the
    order guard must defer."""
    g, _reg = real
    contradictions = 0
    for nid, n in g.nodes.items():
        if n.stream_order is None:
            continue
        for ei in g.up_adj.get(nid, []):
            e = g.edges[ei]
            src = g.nodes.get(e.from_node)
            if src is None or src.stream_order is None or e.from_node == nid:
                continue
            if not _breaks_strahler(src, n):
                continue
            up, dn = src.wsc or "", n.wsc or ""
            if up and dn and up != dn and up.startswith(dn):
                contradictions += 1
    assert contradictions == 0, f"{contradictions} edges where the guards disagree"


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_kootenay_does_not_gain_moyie_or_yahk(real):
    """Doc 10 ③: the watershed-code prefix shortcut over-includes the Kootenay by 3,205 km
    with waters that join BELOW the regulated reach."""
    g, reg = real
    tribs = tributaries_of_reach(g, set(reg["gnis:14097"].section_ids))
    names = {g.nodes[s].display_name for s in tribs if g.nodes.get(s)}
    assert "Moyie River" not in names
    assert "Yahk River" not in names


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_mclennan_creek_does_not_absorb_the_fraser(real):
    """A small floodplain creek must not inherit 20.6% of BC."""
    g, reg = real
    tribs = tributaries_of_reach(g, set(reg["gnis:26380"].section_ids))
    assert len(tribs) < 1000, f"McLennan Creek expanded to {len(tribs):,} sections"
    assert "Fraser River" not in {g.nodes[s].display_name for s in tribs if g.nodes.get(s)}


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_fraser_keeps_its_own_large_catchment(real):
    """The guards must be narrow: the Fraser legitimately drains a fifth of the province,
    and a guard that also cut this one would be cutting real water."""
    g, reg = real
    # 385,140 on the current build. The threshold was 400,000, calibrated BEFORE the
    # three-hop leaf prune removed unnamed headwater sections — which were disproportionately
    # the far fringe of a sweep this size, so a smaller number here is the prune working
    # rather than the walk breaking. Set below the measurement, not at it: this test is a
    # floor guarding against a guard that cuts real water, and pinning it to today's exact
    # count would make it fail on any future prune for the same non-reason.
    assert len(tributaries_of_reach(g, set(reg["gnis:39325"].section_ids))) > 300_000


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_lake_koocanusa_does_not_swallow_the_kootenay(real):
    """A lake fed and drained by the same river must not inherit that river's catchment."""
    g, reg = real
    sid = reg["wbk:328961702"].section_ids[0]
    assert len(tributaries_of_reach(g, {sid})) < 15_000




@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_op_only_decides_the_REACH_the_trib_rule_is_the_same(real):
    """`whole` / `upstream_of` / `downstream_of` / `between` do not each need their own
    tributary logic. They differ only in which sections end up in the reach; the walk then
    answers the same question — what joins THESE sections.

    So the halves must nest inside the whole, and must not overlap: a creek joining above
    the cut cannot also join below it.
    """
    from pipeline.atlas.reach import extent as R
    g, reg = real
    iid = "gnis:12227"                                   # Cowichan River
    cut = next(b.id for b in reg[iid].boundaries if b.kind == "split")

    def tribs(ex):
        got = R.resolve_extent(reg, g, [iid], ex)
        return set(expand(g, set(got["sections"]), only=True))

    whole = tribs({"op": "whole", "splits": []})
    up = tribs({"op": "upstream_of", "splits": [cut]})
    down = tribs({"op": "downstream_of", "splits": [cut]})

    assert up and down, "both halves should have tributaries on this river"
    assert up < whole and down < whole, "each half is a strict subset of the whole"
    assert not (up & down), "a tributary cannot join both above and below one cut"




@pytest.mark.needs_atlas
@pytest.mark.slow
def test_everything_returned_is_reachable_without_crossing_the_reach_boundary(real):
    """The invariant the walk is built on, checked independently of any primitive.

    Every section returned must be reachable from the reach by upstream steps that never
    leave it through its own top. Verified by re-deriving the frontier here rather than
    calling the implementation: if the walk ever admitted something it should not, the two
    derivations disagree.
    """
    import random
    from pipeline.atlas.graph.tributaries import MAINSTEM_EDGE_KINDS
    g, reg = real
    random.seed(13)
    pool = [s for it in list(reg.values())[:3000] for s in it.section_ids]

    for sid in random.sample(pool, 60):
        if sid not in g.nodes:
            continue
        reach = {sid}
        got = tributaries_of_reach(g, reach, guarded=False)

        # independent BFS with the same boundary rule, written differently
        allowed, frontier = set(), [sid]
        seen = set(reach)
        while frontier:
            cur = frontier.pop()
            for ei in g.up_adj.get(cur, []):
                e = g.edges[ei]
                if cur in reach and e.kind in MAINSTEM_EDGE_KINDS and e.from_node not in reach:
                    continue
                if e.from_node in seen:
                    continue
                seen.add(e.from_node)
                allowed.add(e.from_node)
                frontier.append(e.from_node)

        unreachable = got - allowed - _mouths_at_lower_bound(g, frozenset(reach))
        assert not unreachable, f"{sid}: returned unreachable {sorted(unreachable)[:3]}"


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_reservoir_chain_walks_stop_at_the_next_dam(real):
    """Lower Arrow's tributaries contained every section of Upper Arrow's, Lake Revelstoke's
    and Kinbasket's; Kinbasket's took the Columbia above it, which its row excludes in print."""
    g, reg = real
    lower, upper = "lake:328961720", "lake:328961689"
    revelstoke, kinbasket = "lake:329484653", "lake:328961767"
    columbia = set(reg["gnis:37414"].section_ids)
    low = tributaries_of_reach(g, {lower})
    assert not {upper, revelstoke, kinbasket} & low
    assert not tributaries_of_reach(g, {upper}) & tributaries_of_reach(g, {revelstoke})
    kin = tributaries_of_reach(g, {kinbasket})
    assert not kin & columbia, "Columbia River upstream of Kinbasket Reservoir"
    assert kin, "and it keeps its own tributaries"


# --------------------------------------------------------------------------- #
# A lake draining into its own, smaller-ORDER outlet — the lake-chain cut
# --------------------------------------------------------------------------- #
#
# A lake node's Strahler order aggregates every inflow, so it can run AHEAD of the first piece
# of its own outlet: Dester Lake is order 4 and Meldrum Creek below it is order 2, with EQUAL
# watershed codes (100-378118). Equal codes are where the order guard decides, so it read the
# lake as "a bigger river flowing into a smaller one" and refused the edge — and everything
# above the lake (upper Meldrum Creek, 40 of its 51 sections, and all its tributaries) fell out
# of every watershed walk that reached it from below: 155 sections of the Region 5 spring
# closure and 289 of the white sturgeon rules.
#
# The fix is NOT "lakes are exempt". 386 lake-source edges are refused province-wide and ~360
# of them are a lake's SECONDARY outlet (a distributary: the lake has two or more outlets and
# this one is the smaller). Climbing a secondary outlet into the lake would hand a small creek
# the lake's whole catchment — the McLennan failure again. Only the lake's MAIN outlet (the one
# carrying the lake's own watershed code, larger than any other outlet) is the lake's river.

@pytest.fixture
def dester():
    """
        trib_up (order 1, code W-1)
          | confluence
        meldrum_up (order 4, blk M, code W)
          | lake_in
        lake:dester (order 4, code W)        <- the lake runs ahead of its outlet's order
          | lake_out
        meldrum_low (order 2, blk M, code W)
          | confluence
        fraser (order 8, blk F, code 100)    <- the reach
    """
    L = NodeKind.lake
    g = _g([_n("fraser", order=8, blk="F"), _n("meldrum_low", order=2, blk="M"),
            _n("lake:dester", order=4, blk="", kind=L), _n("meldrum_up", order=4, blk="M"),
            _n("trib_up", order=1, blk="T")],
           [("meldrum_low", "fraser", "confluence"),
            ("lake:dester", "meldrum_low", "lake_out"),
            ("meldrum_up", "lake:dester", "lake_in"),
            ("trib_up", "meldrum_up", "confluence")])
    for nid, wsc in (("fraser", "100"), ("meldrum_low", "100-378118"),
                     ("lake:dester", "100-378118"), ("meldrum_up", "100-378118"),
                     ("trib_up", "100-378118-301646")):
        g.nodes[nid] = dataclasses.replace(g.nodes[nid], **{"wsc": wsc})
    return g


def test_a_lake_is_a_tributary_of_its_own_smaller_order_outlet(dester):
    """The walk from the Fraser climbs Meldrum Creek, through Dester Lake, to the creek and the
    tributaries above it. Refusing the lake cut off everything above it."""
    got = tributaries_of_reach(dester, {"fraser"})
    assert got == {"meldrum_low", "lake:dester", "meldrum_up", "trib_up"}


def test_a_secondary_outlet_still_does_not_climb_into_its_lake(dester):
    """The narrowness of the fix. Give the lake a second, BIGGER outlet: meldrum_low is now a
    distributary, and the order guard keeps the lake — and its catchment — out of a walk that
    arrives up the small channel."""
    g = dester
    g.nodes["main_out"] = dataclasses.replace(_n("main_out", order=5, blk="X"), wsc="100-378118")
    g.edges.append(FlowEdge(from_node="lake:dester", to_node="main_out", kind="lake_out",
                            at_measure=0.0))
    g.down_adj.setdefault("lake:dester", []).append(len(g.edges) - 1)
    got = tributaries_of_reach(g, {"fraser"})
    assert got == {"meldrum_low"}


def test_an_outlet_with_another_watershed_code_is_not_the_lakes_river(dester):
    """Burnaby Lake 'drains' into Still Creek in FWA with a code that is not the lake's
    (100-019698-999999 against 100-019698). The code says it is not the lake's line, so the
    order guard stands."""
    g = dester
    g.nodes["meldrum_low"] = dataclasses.replace(g.nodes["meldrum_low"], **{"wsc": "100-378118-9"})
    assert tributaries_of_reach(g, {"fraser"}) == {"meldrum_low"}


def test_a_bigger_river_still_cannot_be_a_tributary_of_a_creek(dester):
    """The McLennan guard is untouched for streams: only a LAKE's main outlet is exempt."""
    g = _g([_n("creek", order=4, blk="C"), _n("bigriver", order=10, blk="B")],
           [("bigriver", "creek", "confluence")])
    for nid in ("creek", "bigriver"):
        g.nodes[nid] = dataclasses.replace(g.nodes[nid], **{"wsc": "100"})
    assert tributaries_of_reach(g, {"creek"}) == frozenset()


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_dester_lake_and_upper_meldrum_creek_are_in_the_fraser_walk(real):
    """The real case: Meldrum Creek (blk 380887762) drains Dester Lake (lake:329370943, order 4)
    by a first piece of order 2, then joins the Fraser. A walk from the river Meldrum Creek
    joins must reach the lake and the creek above it."""
    g, _reg = real
    lake, below, above = "lake:329370943", "380887762:18262", "380887762:20554"
    assert g.nodes[lake].stream_order > g.nodes[below].stream_order
    # follow Meldrum Creek down to the first piece of another river: that is the receiving river
    cur, seen = below, set()
    while g.nodes[cur].blk in ("380887762", ""):       # the creek, and any lake on it
        seen.add(cur)
        nxt = [g.edges[ei].to_node for ei in g.down_adj.get(cur, [])]
        assert nxt, f"{cur}: Meldrum Creek ends before it joins anything"
        cur = nxt[0]
    got = tributaries_of_reach(g, {cur})
    assert below in got
    assert lake in got, "Dester Lake is cut off from the walk"
    assert above in got, "upper Meldrum Creek is cut off from the walk"


# --------------------------------------------------------------------------- #
# FIX A3/B4 (user ruling 2026-10-06): a walk continues across water outside B.C.
# --------------------------------------------------------------------------- #

def _border_trib():
    """The Chilliwack tributary blk 356233151 (live atlas): mouth piece :20 in B.C. (order 0 — its
    fid is drawn mostly in Washington), :43 the 424 m in Washington (order 0, `out_of_bc`: FWA
    computes no order past the border), :467 back in B.C. (order 1)."""
    nodes = [_n("river", order=5), _n("t:20", order=0, blk="t"),
             dataclasses.replace(_n("t:43", order=0, blk="t"), out_of_bc=True),
             _n("t:467", order=1, blk="t")]
    edges = [("t:20", "river", "confluence"), ("t:43", "t:20", "continuation"),
             ("t:467", "t:43", "continuation")]
    return _g(nodes, edges)


def test_a_walk_continues_across_water_outside_bc_and_resumes_inside():
    """The Chilliwack tributary blk 356233151 (live atlas): its mouth piece :20 (in B.C., order 0 —
    its fid is drawn mostly in Washington), :43 the 424 m in Washington (order 0: FWA computes no
    order past the border), :467 back in B.C. (order 1). The walk from the river goes up all three;
    the pieces outside carry no rule because the reach builder subtracts them afterwards
    (`classify.OUTSIDE_BC_SUBTRACTED`), never because the walk stopped. Read as an order, 0 made
    :467 'a bigger river' than :43 and the walk stopped at the border."""
    from pipeline.atlas.graph import tributaries as T
    g = _border_trib()
    assert expand(g, {"river"}) == {"river", "t:20", "t:43", "t:467"}
    assert not _breaks_strahler(g.nodes["t:467"], g.nodes["t:43"])
    assert T.ORDER_ZERO_IS_UNKNOWN


def test_order_zero_is_unknown_only_outside_bc(monkeypatch):
    """MUTATION: with order 0 read as an order the re-entering piece is lost; a real bigger river
    above a smaller piece is still refused."""
    from pipeline.atlas.graph import tributaries as T
    g = _border_trib()
    monkeypatch.setattr(T, "ORDER_ZERO_IS_UNKNOWN", False)
    assert "t:467" not in expand(g, {"river"}), "the defect: the walk stopped at the border"
    monkeypatch.setattr(T, "ORDER_ZERO_IS_UNKNOWN", True)
    assert "t:467" in expand(g, {"river"})
    assert _breaks_strahler(_n("big", order=4), _n("small", order=2))


def test_an_order_zero_stub_inside_bc_does_not_seed_the_river_it_joins():
    """Code review A-3: a reach that is a B.C. order-0 stub must not pick up a bigger river arriving
    at its cut (`_mouths_at`). Order 0 is unknown only OUTSIDE B.C.; inside it is judged. MUTATION:
    mark the stub `out_of_bc` and the river (order 5) is seeded. (Built as a CUT since T90: at a
    reach's mouth nothing is seeded at all.)"""
    from pipeline.atlas.graph.tributaries import _mouths_at
    def world(stub_out):
        stub = dataclasses.replace(_n("s:0", order=0, blk="s"), out_of_bc=stub_out)
        nodes = [stub, _n("s:below", order=0, blk="s"), _n("H:0", order=5, blk="H")]
        return _g(nodes, [("s:0", "s:below", "continuation", 0.0),
                          ("H:0", "s:below", "confluence", 0.0)])
    g = world(False)
    assert _mouths_at(g, "s:0", g.nodes["s:0"], 0.0, frozenset({"s:0"})) == set()
    g = world(True)
    assert _mouths_at(g, "s:0", g.nodes["s:0"], 0.0, frozenset({"s:0"})) == {"H:0"}


# --------------------------------------------------------------------------- #
# T90 — the Chehalis "downstream of the main logging road bridge, May 1-31" closure bound two
# HARRISON side channels (live sids 59983, 59984 = 355992873:0, 355992877:0, both off_stem)
# --------------------------------------------------------------------------- #

CHEHALIS_WSC, HARRISON_WSC = "100-077501-094860", "100-077501"


def _w(nid, *, order, wsc, down_m=0.0):
    return dataclasses.replace(_n(nid, order=order), wsc=wsc, down_m=down_m)


def _chehalis(*, codes=True):
    """The live atlas around the Chehalis mouth (2026-10-22 bundle), reduced to what the seeding
    reads. `codes=False` blanks every watershed code, so only the topology tells the pieces apart.

        C:3592 (Chehalis, order 5, reach)  --confluence @865.4-->  HS1:0 (Harrison side channel, order 2)
           |  continuation @3592.9                                   ^  confluence @0.0
        C:0    (Chehalis, order 5, reach — its mouth)               HS2:0 (Harrison side channel, order 2)
           |  confluence @8413.3                                     |  confluence @8603.0
        H:2297 (Harrison, order 7)  <---------------------------------+

    The reach is "downstream of the bridge": its window on the Chehalis runs to measure 0.
    """
    c = CHEHALIS_WSC if codes else ""
    h = HARRISON_WSC if codes else ""
    nodes = [_w("C:0", order=5, wsc=c), _w("C:3592", order=5, wsc=c, down_m=3592.9),
             _w("HS1:0", order=2, wsc=h), _w("HS2:0", order=2, wsc=h),
             _w("H:2297", order=7, wsc=h, down_m=2297.9)]
    edges = [("C:3592", "C:0", "continuation", 3592.9),
             ("C:3592", "HS1:0", "confluence", 865.4),    # a distributary into a Harrison channel
             ("HS2:0", "HS1:0", "confluence", 0.0),       # ...where a sibling sits at measure 0
             ("HS1:0", "HS2:0", "confluence", 720.1),
             ("C:0", "H:2297", "confluence", 8413.3),
             ("HS2:0", "H:2297", "confluence", 8603.0),
             ("H:2297", "HS2:0", "confluence", 916.6)]
    return _g(nodes, edges)


CHEHALIS_REACH = frozenset({"C:0", "C:3592"})
CHEHALIS_WINDOW = ("C", 0.0, 5362.5)


def test_t90_the_chehalis_reach_binds_no_harrison_side_channel():
    """Live: `chehalis_river.r2` bound 355992877:0 by the mouth seeding and 355992873:0 by the
    walk above it. The window's lower measure (0, the mouth) was compared with the measure of an
    edge on ANOTHER blue line — a Harrison side channel's — where 0 means that channel's mouth."""
    g = _chehalis()
    assert _mouths_at_lower_bound(g, CHEHALIS_REACH, CHEHALIS_WINDOW) == set()
    assert tributaries_of_reach(g, CHEHALIS_REACH, window=CHEHALIS_WINDOW) == frozenset()


def test_t90_only_the_piece_AT_the_cut_seeds_and_in_its_own_line_s_measures(monkeypatch):
    """MUTATION (`MOUTH_SEEDS_ONLY_AT_THE_CUT`): with the codes silent, only the topology can say
    C:3592 is not at the cut — its own line goes on below it INSIDE the reach — and that the
    sibling's 0.0 is a measure on the side channel's line, not the Chehalis's. Off, the two live
    sections come back exactly."""
    from pipeline.atlas.graph import tributaries as T
    g = _chehalis(codes=False)
    assert tributaries_of_reach(g, CHEHALIS_REACH, window=CHEHALIS_WINDOW) == frozenset()
    monkeypatch.setattr(T, "MOUTH_SEEDS_ONLY_AT_THE_CUT", False)
    assert tributaries_of_reach(g, CHEHALIS_REACH, window=CHEHALIS_WINDOW) == {"HS1:0", "HS2:0"}


def test_t90_a_receiving_river_s_channel_at_the_cut_is_not_seeded(monkeypatch):
    """A channel carrying the RECEIVING river's own code is a side channel of that river: its order
    is small, so the Strahler guard passes it. Met at a cut it is still never the reach's
    tributary; a creek of the reach's own river at the same point is. MUTATION
    (`THE_RECEIVING_RIVER_IS_NEVER_A_SEED`): off, the Harrison channel is seeded."""
    from pipeline.atlas.graph import tributaries as T
    g = _g([_w("C:0", order=5, wsc=CHEHALIS_WSC),
            _w("C:3592", order=5, wsc=CHEHALIS_WSC, down_m=3592.9),
            _w("HS3:0", order=2, wsc=HARRISON_WSC),
            _w("K:0", order=2, wsc=CHEHALIS_WSC + "-073263")],      # a creek of the Chehalis
           [("C:3592", "C:0", "continuation", 3592.9),
            ("HS3:0", "C:0", "confluence", 3592.9),
            ("K:0", "C:0", "confluence", 3592.9)])
    reach, window = frozenset({"C:3592"}), ("C", 3592.9, 5362.5)
    assert _mouths_at_lower_bound(g, reach, window) == {"K:0"}
    monkeypatch.setattr(T, "THE_RECEIVING_RIVER_IS_NEVER_A_SEED", False)
    assert _mouths_at_lower_bound(g, reach, window) == {"HS3:0", "K:0"}


def test_t90_a_reach_running_to_its_mouth_seeds_nothing(monkeypatch):
    """A `downstream_of` window runs to measure 0, the mouth. Everything arriving there joins the
    RECEIVING river (the Harrison takes the Chehalis at 8413.3 on its own line). MUTATION
    (`MOUTH_SEEDS_ONLY_AT_THE_CUT`): off, the window's 0 is compared with the Harrison's measures and
    seeds a creek joining the Harrison at the Harrison's own mouth, kilometres away."""
    from pipeline.atlas.graph import tributaries as T
    g = _g([_w("C:0", order=5, wsc=CHEHALIS_WSC), _w("H:2297", order=7, wsc=HARRISON_WSC),
            _w("far:0", order=2, wsc=HARRISON_WSC + "-000100"),
            _w("near:0", order=2, wsc=HARRISON_WSC + "-094861")],
           [("C:0", "H:2297", "confluence", 8413.3),
            ("far:0", "H:2297", "confluence", 0.0),
            ("near:0", "H:2297", "confluence", 8413.3)])
    reach, window = frozenset({"C:0"}), ("C", 0.0, 3592.9)
    assert _mouths_at_lower_bound(g, reach, window) == set()
    assert tributaries_of_reach(g, reach, window=window) == frozenset()
    monkeypatch.setattr(T, "MOUTH_SEEDS_ONLY_AT_THE_CUT", False)
    assert _mouths_at_lower_bound(g, reach, window) == {"far:0"}


def test_t90_a_distributary_at_the_cut_is_not_a_way_in(monkeypatch):
    """At a cut, only the piece below it ON THE REACH'S LINE is looked at. A distributary leaving
    the cut piece for a side channel, with a creek joining that channel at the cut's measure (a
    measure on another line), is not "at the cut". MUTATION (`MOUTH_SEEDS_ONLY_AT_THE_CUT`)."""
    from pipeline.atlas.graph import tributaries as T
    g = _g([_w("C:0", order=5, wsc=""), _w("C:3592", order=5, wsc="", down_m=3592.9),
            _w("SC:0", order=2, wsc=""), _w("x:0", order=1, wsc="")],
           [("C:3592", "C:0", "continuation", 3592.9),
            ("C:3592", "SC:0", "confluence", 10.0),
            ("x:0", "SC:0", "confluence", 3592.9)])
    reach, window = frozenset({"C:3592"}), ("C", 3592.9, 5362.5)
    assert _mouths_at_lower_bound(g, reach, window) == set()
    monkeypatch.setattr(T, "MOUTH_SEEDS_ONLY_AT_THE_CUT", False)
    assert _mouths_at_lower_bound(g, reach, window) == {"x:0"}


# --------------------------------------------------------------------------- #
# A tributary walk never enters the ENTRY'S OWN water (2026-10-10)
# --------------------------------------------------------------------------- #

@pytest.fixture
def vedder():
    """The Chilliwack/Vedder shape: one row, two rivers on two blue lines.

        chil_up  <- chil_trib_up                  (own water, upper)
          |  continuation
        chil_low <- chil_trib                     (own water — FWA codes it a TRIBUTARY of the Vedder)
          |  confluence (equal order, child code)
        v_top                                     reach  ("downstream of Vedder Crossing Bridge")
          |  continuation
        v_low    <- v_trib <- v_trib_up           reach, with a true tributary
          |
        side  <- creek_above_lake <- own_lake <- ... a true tributary carrying a lake the row names
    """
    def n(nid, order, wsc):
        return dataclasses.replace(_n(nid, order=order), wsc=wsc)
    nodes = [n("v_low", 6, "100-1"), n("v_top", 6, "100-1"),
             n("chil_low", 6, "100-1-9"), n("chil_up", 6, "100-1-9"),
             n("chil_trib", 2, "100-1-9-3"), n("chil_trib_up", 1, "100-1-9-3"),
             n("v_trib", 2, "100-1-4"), n("v_trib_up", 1, "100-1-4"),
             n("side", 2, "100-1-5"),
             dataclasses.replace(_n("own_lake", order=2, kind=NodeKind.lake), wsc="100-1-5"),
             n("creek_above_lake", 1, "100-1-5")]
    edges = [("v_top", "v_low", "continuation"),
             ("chil_low", "v_top", "confluence"),
             ("chil_up", "chil_low", "continuation"),
             ("chil_trib", "chil_low", "confluence"),
             ("chil_trib_up", "chil_up", "confluence"),
             ("v_trib", "v_low", "confluence"),
             ("v_trib_up", "v_trib", "continuation"),
             ("side", "v_low", "confluence"),
             ("own_lake", "side", "lake_out"),
             ("creek_above_lake", "own_lake", "lake_in")]
    return _g(nodes, edges)


V_REACH = {"v_low", "v_top"}
V_OWN = {"v_low", "v_top", "chil_low", "chil_up", "own_lake"}   # every section the ENTRY matches


def test_the_walk_never_enters_the_entrys_own_water_past_the_cut(vedder):
    """The Chilliwack joins the Vedder's top piece by a `confluence` edge, and FWA codes it a
    tributary — but it is the row's own water above the cut, governed by its own rules' extents.
    "Downstream of Vedder Crossing Bridge, including tributaries" binds the Vedder's creeks, never the
    Chilliwack nor the streams feeding it."""
    got = tributaries_of_reach(vedder, V_REACH, own=V_OWN)
    assert not got & {"chil_low", "chil_up", "chil_trib", "chil_trib_up"}
    assert {"v_trib", "v_trib_up", "side"} <= got


def test_own_water_met_above_a_true_tributary_is_walked_through_not_collected(vedder):
    """A lake the row names on one of its river's creeks is the row's (its extent's), never the
    walk's — but the creek feeding it is still a tributary of the reach."""
    got = tributaries_of_reach(vedder, V_REACH, own=V_OWN)
    assert "own_lake" not in got
    assert "creek_above_lake" in got


def test_mutation_without_the_rule_the_walk_climbs_the_chilliwack(vedder, monkeypatch):
    """Switch the principle off and the regression is back: the walk takes the row's own upstream
    river and every stream feeding it (1,820 sections on the live atlas)."""
    import pipeline.atlas.graph.tributaries as T
    monkeypatch.setattr(T, "THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER", False)
    got = T.tributaries_of_reach(vedder, V_REACH, own=V_OWN)
    assert {"chil_low", "chil_up", "chil_trib", "chil_trib_up"} <= got


def test_the_expander_hands_the_walk_every_item_the_entry_matches(vedder, monkeypatch):
    """`reach.build._expander` builds `own` from the ENTRY's `matched` items — not only the item the
    extent names (r4–r9 name none; their scope is the cut)."""
    from types import SimpleNamespace
    from pipeline.atlas.reach.build import _expander
    import pipeline.atlas.graph.tributaries as T
    registry = {"gnis:vedder": SimpleNamespace(section_ids=("v_low", "v_top")),
                "gnis:chilliwack": SimpleNamespace(section_ids=("chil_low", "chil_up"))}
    entry = {"entry_id": "e", "matched": ["gnis:chilliwack", "gnis:vedder"], "rules": []}
    rule = {"rule_id": "r7", "extents": []}
    covered = list(entry["matched"])
    expand = _expander(vedder, registry, covered, rule, entry)
    got = expand(frozenset(V_REACH))
    assert not got & {"chil_low", "chil_up", "chil_trib", "chil_trib_up"}
    assert {"v_low", "v_top", "v_trib", "v_trib_up"} <= got
    monkeypatch.setattr(T, "THE_WALK_NEVER_ENTERS_THE_ENTRYS_OWN_WATER", False)
    expand = _expander(vedder, registry, covered, rule, entry)
    assert {"chil_trib", "chil_trib_up"} <= expand(frozenset(V_REACH))


def test_a_side_channel_of_the_reachs_own_river_is_not_own_water_past_the_cut(vedder):
    """A piece of the row's water carrying the reach's OWN watershed code is a side channel beside the
    stretch (the Old Kootenay River Channel across the Creston flats), not the water going on past a
    cut: the walk keeps it and the creeks draining through it."""
    def n(nid, order, wsc):
        return dataclasses.replace(_n(nid, order=order), wsc=wsc)
    g = _g(list(vedder.nodes.values()) + [n("v_side", 1, "100-1"), n("side_creek", 1, "100-1-7")],
           [(e.from_node, e.to_node, e.kind) for e in vedder.edges]
           + [("v_side", "v_low", "confluence"), ("side_creek", "v_side", "confluence")])
    got = tributaries_of_reach(g, V_REACH, own=V_OWN | {"v_side"})
    assert {"v_side", "side_creek"} <= got
    assert not got & {"chil_low", "chil_trib"}
