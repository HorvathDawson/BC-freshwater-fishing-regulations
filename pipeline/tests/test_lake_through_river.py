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


# ---------------------------------------------------------------- T743: a DFO lake place (Redsand Lake)
# "Redsand Lake, including tributaries" (DFO Region 6 B(ii), mark 2625) is the curated non-FWA lake wbk:-1 on the
# Kitsumkalum mainstem, 2 km below Kitsumkalum Lake. A DFO place is resolved with no table of rows (`owned` None), so
# T468 read its through-river as unrowed and took the Kitsumkalum above it with Kitsumkalum Lake's whole basin: 2,639
# sections, 3,086 km, for a 40 ha lake. The same shape as Powell's; the rule carries D5's DFO key.

DFO_ROW = {"entry_id": "6:redsand-lake:including-tributaries", "matched": ["wbk:L"], "includes_tributaries": True}


def _dfo_rule(**kw):
    from pipeline.atlas.reach.build import JOINING_WATER_STAYS_OUT_KEY
    return {"rule_id": "6:redsand-lake:including-tributaries#0", "extents": [{"op": "whole", "item_id": "wbk:L"}],
            "includes_tributaries": True, JOINING_WATER_STAYS_OUT_KEY: True, **kw}


def test_a_dfo_lake_places_through_river_stays_out(powell):
    g, reg = powell
    b, d = build_reach(DFO_ROW, _dfo_rule(), reg, g)          # owned None: how `reach.dfo_salmon` resolves a place
    got = set(b.sections)
    assert got == {"lake:L", "C"}, "the lake and its own inflows; never the river above it and its basin"
    assert [x.payload for x in d if x.kind == "through_river"] == [
        {"lake": "lake:L", "inlet": "R:up", "water": "Powell River", "own_row": [], "negated": False, "joined": 0,
         "stays_out": True}]


def test_a_dfo_lake_place_whose_words_name_the_river_takes_it(powell):
    g, reg = powell
    b, _ = build_reach(DFO_ROW, _dfo_rule(verbatim="including tributaries, including the Powell River"), reg, g)
    assert {"R:up", "RT"} <= set(b.sections)


def test_a_lake_row_without_the_dfo_key_is_unchanged(powell):
    """The synopsis row (no DFO key) still takes its unrowed through-river (T468)."""
    got, _ = _bind(powell)
    assert {"R:up", "RT"} <= got


def test_dfo_through_river_mutation_pin(powell, monkeypatch):
    from pipeline.atlas.reach import build as B
    monkeypatch.setattr(B, "DFO_THROUGH_RIVER_STAYS_OUT", False)
    g, reg = powell
    b, _ = build_reach(DFO_ROW, _dfo_rule(), reg, g)
    assert {"R:up", "RT"} <= set(b.sections), "False = every DFO through-river goes with the lake, as before"
