"""T468: a lake row's through-river above the lake is its tributary when the river has no row of its own.

Owner, 2026-11-07: "powell lake is closed including tributaries in november but powell river (above the
lake) is not showing as closed". FWA draws the Powell River down through Powell Lake on ONE blue line
(blk 360879873), so the river feeding the lake's head shares the lake's OUTLET line and the walk refused
it as "the river continuing through" (`tributaries._through_blks`, the Koocanusa rule). Every other
inflow took the closure; the Powell River and its 640 sections above the lake did not.

The shape here is Powell's: lake L drains out by R:0 (the lower Powell River) and is fed by R:up on the
same line (the upper Powell River, with a creek RT of its own) and by a side creek C.
"""

from __future__ import annotations

import pytest

from pipeline.atlas.graph import tributaries as T
from pipeline.atlas.reach.build import build_reach
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem

S, L = NodeKind.stream, NodeKind.lake


def _n(nid, kind=S, *, blk="", lo=0.0, hi=100.0, name="", wsc="", order=2):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wsc=wsc, display_name=name, down_m=lo,
                      up_m=hi, length_m=hi - lo, stream_order=order)


@pytest.fixture
def powell():
    nodes = [
        _n("R:0", blk="R", lo=0, hi=1000, name="Powell River", wsc="900-1", order=6),
        _n("lake:L", L, name="Powell Lake", wsc="900-1", order=6),
        _n("R:up", blk="R", lo=50000, hi=60000, name="Powell River", wsc="900-1", order=5),
        _n("RT", blk="RT", name="", wsc="900-1-000800", order=2),
        _n("C", blk="C", name="Side Creek", wsc="900-1-000300", order=2),
    ]
    edges = [("lake:L", "R:0", "lake_out", 1000), ("R:up", "lake:L", "lake_in", 50000),
             ("RT", "R:up", "confluence", 55000), ("C", "lake:L", "lake_in", 20000)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {"wbk:L": RegistryItem(id="wbk:L", name="Powell Lake", kind="lake", section_ids=("lake:L",)),
           "gnis:R": RegistryItem(id="gnis:R", name="Powell River", kind="stream",
                                  section_ids=("R:0", "R:up"))}
    return g, reg


LAKE_ROW = {"entry_id": "r2:powell_lake@2-12", "matched": ["wbk:L"], "includes_tributaries": True}
CLOSURE = {"rule_id": "powell_lake.r1", "verbatim": "No Fishing Nov 1-Mar 31",
           "extents": [{"op": "whole"}]}


def _bind(world, *, entry=LAKE_ROW, rule=CLOSURE, owned=None):
    g, reg = world
    b, d = build_reach(entry, rule, reg, g, owned=owned or {})
    return set(b.sections), [x.payload for x in d if x.kind == "through_river"]


def test_the_walk_alone_refuses_the_through_river(powell):
    """The cause, pinned: at the lake, the inflow on the outlet's line is `RIVER_CONTINUING`."""
    g, _ = powell
    assert T.through_inlets(g, {"lake:L"}, "lake:L") == {"R:up"}
    assert T.expand(g, {"lake:L"}) == {"lake:L", "C"}


def test_an_unrowed_through_river_goes_with_the_lake(powell):
    got, diags = _bind(powell)
    assert got == {"lake:L", "C", "R:up", "RT"}, "the Powell River above the lake and its creek"
    assert "R:0" not in got, "the river BELOW the lake is downstream, never a tributary"
    assert diags == [{"lake": "lake:L", "inlet": "R:up", "water": "Powell River", "own_row": [],
                      "negated": False, "joined": 2}]


def test_a_through_river_with_a_row_of_its_own_stays_out(powell):
    """Koocanusa / Kinbasket: the river above has rows of its own, and they govern it."""
    got, diags = _bind(powell, owned={"gnis:R": ("r2:powell_river@2-12",)})
    assert got == {"lake:L", "C"}
    assert diags[0]["own_row"] == ["r2:powell_river@2-12"] and diags[0]["joined"] == 0


def test_the_rules_words_keep_it_out(powell):
    rule = dict(CLOSURE, verbatim="No Fishing, but not including the Powell River")
    got, _ = _bind(powell, rule=rule)
    assert not got & {"R:up", "RT"}


def test_a_lake_at_the_top_of_a_river_reach_still_ends_it(powell):
    """Only the ENTRY'S OWN lake: a river row whose reach stops at a lake does not take its inflows."""
    g, reg = powell
    entry = {"entry_id": "r2:powell_river@2-12", "matched": ["gnis:R"], "includes_tributaries": True}
    rule = {"rule_id": "powell_river.r1", "verbatim": "x",
            "extents": [{"op": "whole", "item_id": "gnis:R"}, {"op": "whole", "item_id": "wbk:L"}]}
    b, d = build_reach(entry, rule, reg, g, owned={})
    assert not [x for x in d if x.kind == "through_river"]


def test_mutation_pin(powell, monkeypatch):
    monkeypatch.setattr(T, "A_LAKE_ROWS_UNROWED_THROUGH_RIVER_IS_A_TRIBUTARY", False)
    got, diags = _bind(powell)
    assert got == {"lake:L", "C"} and diags == []
