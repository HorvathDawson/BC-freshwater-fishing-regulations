"""T1053 (owner 2026-11-24 12:33, "Climb through every head lake"): a rule bound to its WHOLE water walks up
through the lake its water flows out of — Eagle Creek ✱ flows out of Dick Lake, and Dick Creek above the lake is
Eagle Creek's tributary (`tributaries.A_WHOLE_WATERS_WALK_CLIMBS_ITS_HEAD_LAKE`, `reach.build.climbs_head_lakes`).

The lake is walked, never collected (T1); a stretch rule, a lake the water flows INTO, and every other guard of the
walk stay as they were, and a head lake not climbed is reported with the walk's typed reason (W46)."""

from __future__ import annotations

import pytest

from pipeline.atlas.graph import tributaries as T
from pipeline.atlas.graph.tributaries import expand, head_lake_verdicts, head_lakes, tributaries_of_reach
from pipeline.atlas.reach import build as reach_build
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode

L = NodeKind.lake


def _n(nid, *, order=1, blk=None, kind=NodeKind.stream, wsc="", at=0.0):
    return StreamNode(node_id=nid, kind=kind, blk="" if kind == L else (blk or nid),
                      down_m=at, up_m=at + 100.0, length_m=100.0, stream_order=order, wsc=wsc)


def _g(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=0.0) for a, b, k in edges]
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


@pytest.fixture
def eagle():
    """Eagle Creek (E, two pieces) flows out of Dick Lake; Dick Creek (D, with a creek of its own) and the line above
    the lake on Eagle's own blue line (an unnamed piece, E at 300 m) feed the lake; a creek joins Eagle below.

        dick_up (DU) -> dick_creek (D) --lake_in--> lake:dick <--lake_in-- upper (E, 300 m)
                                                       | lake_out
                                                     e_top (E, 100 m)
                                                       | continuation
                                                     e_low (E)  <--confluence-- low_trib (B)
    """
    nodes = [_n("e_low", order=3, blk="E", wsc="900-1"), _n("e_top", order=3, blk="E", at=100.0, wsc="900-1"),
             _n("lake:dick", order=3, kind=L, wsc="900-1"), _n("upper", order=2, blk="E", at=300.0, wsc="900-1"),
             _n("dick_creek", order=2, blk="D", wsc="900-1-5"), _n("dick_up", order=1, blk="DU", wsc="900-1-5-2"),
             _n("low_trib", order=1, blk="B", wsc="900-1-1")]
    edges = [("e_top", "e_low", "continuation"), ("lake:dick", "e_top", "lake_out"),
             ("upper", "lake:dick", "lake_in"), ("dick_creek", "lake:dick", "lake_in"),
             ("dick_up", "dick_creek", "confluence"), ("low_trib", "e_low", "confluence")]
    return _g(nodes, edges)


WATER = frozenset({"e_low", "e_top"})


def test_as_built_the_walk_stops_at_the_head_lake(eagle):
    """No `climb_from` (a stretch rule, or any caller that does not ask): the lake is the river going on."""
    assert tributaries_of_reach(eagle, WATER) == {"low_trib"}


def test_a_whole_water_walk_climbs_its_head_lake(eagle):
    got = tributaries_of_reach(eagle, WATER, climb_from=WATER)
    assert got == {"low_trib", "lake:dick", "dick_creek", "dick_up", "upper"}


def test_the_head_lake_is_walked_never_collected(eagle):
    """T1: tributaries are STREAMS. The lake keeps its own row (or the zone's rules); only what feeds it is the rule's."""
    assert expand(eagle, WATER, climb_from=WATER) == WATER | {"low_trib", "dick_creek", "dick_up", "upper"}


def test_the_stream_on_the_outlets_line_above_the_lake_is_a_tributary_too(eagle):
    """Nothing of the rule's own water lies above its head lake, so the line above the lake is no "river going on"."""
    assert "upper" in expand(eagle, WATER, climb_from=WATER)


def test_head_lakes_names_the_lake_and_the_piece_below(eagle):
    heads = head_lakes(eagle, WATER)
    assert list(heads) == ["lake:dick"] and heads["lake:dick"][0] == "e_top"


def test_a_lake_the_water_flows_into_is_never_its_head(eagle):
    """A rule on `e_top` alone whose water also holds `upper` (the item goes on above the lake): the lake is the water
    passing through (or going on), not its head — the walk does not climb it."""
    assert head_lakes(eagle, {"e_top"}, {"upper"}) == {}
    assert "dick_creek" not in tributaries_of_reach(eagle, {"e_top"}, own={"upper"}, climb_from={"e_top"})


def test_a_head_lake_of_the_entrys_own_water_stays_its_extents(eagle):
    """"Dick Lake and Eagle Creek": a rule bound to the creek only does not walk the row's own lake (rule 63)."""
    v = head_lake_verdicts(eagle, WATER, climb_from=WATER, own={"lake:dick"})
    assert v == [{"lake": "lake:dick", "below": "e_top", "climbed": False, "reason": T.OWN_WATER}]
    assert "dick_creek" not in tributaries_of_reach(eagle, WATER, own={"lake:dick"}, climb_from=WATER)


def test_a_carve_out_above_the_lake_still_blocks(eagle):
    got = tributaries_of_reach(eagle, WATER, blocked={"dick_creek"}, climb_from=WATER)
    assert not {"dick_creek", "dick_up"} & got and "upper" in got


def test_a_climbed_lake_is_reported_climbed(eagle):
    assert head_lake_verdicts(eagle, WATER, climb_from=WATER) == [
        {"lake": "lake:dick", "below": "e_top", "climbed": True, "reason": None}]


def test_a_secondary_outlet_still_may_not_climb_into_its_lake():
    """The creek is the lake's SECONDARY outlet (order 1 against the lake's 4, the main outlet M carries its river):
    climbing would hand a small creek the lake's whole catchment. The order guard stands: `bigger_river`, reported."""
    g = _g([_n("creek", order=1, blk="C", wsc="900-1"), _n("lake:big", order=4, kind=L, wsc="900-1"),
            _n("main", order=4, blk="M", wsc="900-1"), _n("feeder", order=3, blk="F", wsc="900-1-7")],
           [("lake:big", "creek", "lake_out"), ("lake:big", "main", "lake_out"), ("feeder", "lake:big", "lake_in")])
    assert tributaries_of_reach(g, {"creek"}, climb_from={"creek"}) == frozenset()
    assert head_lake_verdicts(g, {"creek"}, climb_from={"creek"}) == [
        {"lake": "lake:big", "below": "creek", "climbed": False, "reason": T.BIGGER_RIVER}]


def test_the_receiving_river_guard_stands_at_a_head_lake():
    """FWA codes the creek leaving the lake as a tributary of the river the lake lies on (the lake carries the river's
    code): the lake is the receiving river's, never climbed (the Chehalis/Harrison class, `THE_WALK_NEVER_ENTERS_A_PARENT`)."""
    g = _g([_n("creek", order=2, blk="C", wsc="100-200-300"), _n("lake:on_y", order=2, kind=L, wsc="100-200"),
            _n("y_up", order=5, blk="Y", wsc="100-200")],
           [("lake:on_y", "creek", "lake_out"), ("y_up", "lake:on_y", "lake_in")])
    assert tributaries_of_reach(g, {"creek"}, climb_from={"creek"}) == frozenset()
    assert head_lake_verdicts(g, {"creek"}, climb_from={"creek"})[0]["reason"] == T.RECEIVING_RIVER


def test_mutation_the_policy_switch_restores_the_stop(eagle, monkeypatch):
    monkeypatch.setattr(T, "A_WHOLE_WATERS_WALK_CLIMBS_ITS_HEAD_LAKE", False)
    assert tributaries_of_reach(eagle, WATER, climb_from=WATER) == {"low_trib"}


# ------------------------------------------------------------------ which rules climb (reach.build)

@pytest.mark.parametrize("rule, climbs", [
    ({"extents": [{"op": "whole"}]}, True),
    ({"extents": [{"op": "whole", "item_id": "gnis:9405"}, {"op": "whole", "item_id": "gnis:1"}]}, True),
    ({"extents": [{"op": "downstream_of", "splits": ["x"]}]}, False),        # a stretch ending at the lake
    ({"extents": [{"op": "whole"}, {"op": "between", "splits": ["a", "b"]}]}, False),
    ({"extents": []}, False),
    ({"extents": [{"op": "whole"}], reach_build.JOINING_WATER_STAYS_OUT_KEY: True}, False),   # a DFO place
])
def test_only_a_book_rule_on_its_whole_water_climbs(rule, climbs):
    assert reach_build.climbs_head_lakes(rule) is climbs


# ------------------------------------------------------------------ a walk covers a pond only as a piece of its stream

class _Item:
    def __init__(self, kind, sections):
        self.kind, self.section_ids = kind, list(sections)


def _ponds():
    """A river (R) with a creek (C) joining; the creek runs through two polygons: `lake:oxbow`, which the registry lists
    as a piece of the creek (an oxbow / slough pond, the existing process: T153, Q124), and `lake:own`, a lake that is a
    water of its own (a lake item). Above them `c_up`."""
    nodes = [_n("river", order=4, blk="R"), _n("c_low", order=2, blk="C"), _n("lake:oxbow", order=2, kind=L),
             _n("c_mid", order=2, blk="C", at=200.0), _n("lake:own", order=2, kind=L), _n("c_up", order=1, blk="C", at=400.0)]
    edges = [("c_low", "river", "confluence"), ("lake:oxbow", "c_low", "lake_out"), ("c_mid", "lake:oxbow", "lake_in"),
             ("lake:own", "c_mid", "lake_out"), ("c_up", "lake:own", "lake_in")]
    reg = {"gnis:1": _Item("stream", ["c_low", "lake:oxbow", "c_mid", "c_up"]), "wbk:2": _Item("lake", ["lake:own"])}
    return _g(nodes, edges), reg


def test_a_walk_covers_a_pond_only_where_the_registry_lists_it_as_a_piece_of_the_stream():
    """Owner 2026-11-24 (~15:30): "only the polygon ponds that we have listed as piece of stream. (for example oxbows or
    slough lakes etc etc ) make sense? using the process we already have"; and "tributary rules dont apply to the lake
    either only streams". The creek's listed pond is the creek; a lake that is a water of its own is walked through,
    never covered."""
    g, reg = _ponds()
    got = expand(g, {"river"}, only=True, registry=reg)
    assert got == {"c_low", "lake:oxbow", "c_mid", "c_up"}
    assert "lake:own" in tributaries_of_reach(g, {"river"})          # walked through


def test_mutation_without_the_registry_every_polygon_is_a_lake():
    """The pin: what decides is the registry's listing. Asked without it (the node kind alone) the listed pond is a lake
    and is not covered — so the listing, not the polygon, is what the walk follows."""
    g, _reg = _ponds()
    assert "lake:oxbow" not in expand(g, {"river"}, only=True)
