"""T1177, Upper Arrow: A RIVER STRETCH IN A LAKE'S TRIBUTARIES ROW IS THE LAKE'S OWN UPPER END
(`build.A_RIVER_STRETCH_IN_A_LAKE_ROW_IS_THE_LAKES_OWN_UPPER_END`).

Owner, 2026-11-27: "it should climb all the way to kinbasket lake i think?", confirmed "yes it doesnt matter anyways
because lake revelstoke has same regs for its tributaries but yes this is what we do" (RULINGS T19). "UPPER ARROW LAKE'S
TRIBUTARIES" binds the lake and the drawdown area (the Columbia from the Hwy 1 bridge down to the lake polygon, "that
water known as either Upper Arrow Lake or Columbia River"): the lake's head is the bridge, so the river above it is the
lake's through-river inflow, and the walk climbs through the reservoir above to the next lake row's printed exception.

The world: lake UA fed by the drawdown stretch DD (Columbia, its own item), creek D joining DD; above the bridge the river
C:up with creek J, fed by reservoir RV (lake_out) with its creek RVc; RV is fed by reservoir KB (lake_out) with its creek
KBc; KB is fed by the river above it, C:top, with creek CL. KB's own row prints "Does not include the river upstream".
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import build as B
from pipeline.atlas.reach.build import build_reach, lake_row_stops
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem

S, L = NodeKind.stream, NodeKind.lake


def _n(nid, kind=S, *, blk="", lo=0.0, hi=100.0, name="", wsc="", order=2):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wsc=wsc, display_name=name, down_m=lo,
                      up_m=hi, length_m=hi - lo, stream_order=order)


@pytest.fixture
def arrow():
    nodes = [
        _n("U:0", blk="C", lo=0, hi=1000, name="Big River", wsc="300-1", order=7),
        _n("lake:UA", L, name="Upper Lake", wsc="300-1", order=7),
        _n("DD", blk="C", lo=50000, hi=60000, name="Big River", wsc="300-1", order=7),
        _n("D", blk="D", name="Drawdown Creek", wsc="300-1-000400", order=2),
        _n("C:up", blk="C", lo=60000, hi=70000, name="Big River", wsc="300-1", order=7),
        _n("J", blk="J", name="Jordan Creek", wsc="300-1-000600", order=3),
        _n("lake:RV", L, name="Middle Reservoir", wsc="300-1", order=7),
        _n("RVc", blk="RVc", wsc="300-1-000700", order=2),
        _n("lake:KB", L, name="Top Reservoir", wsc="300-1", order=7),
        _n("KBc", blk="KBc", wsc="300-1-000800", order=2),
        _n("C:top", blk="C", lo=200000, hi=300000, name="Big River", wsc="300-1", order=6),
        _n("CL", blk="CL", name="Head Lake Creek", wsc="300-1-000900", order=2),
    ]
    edges = [("lake:UA", "U:0", "lake_out", 1000), ("DD", "lake:UA", "lake_in", 50000),
             ("D", "DD", "confluence", 55000), ("C:up", "DD", "continuation", 60000),
             ("J", "C:up", "confluence", 65000), ("lake:RV", "C:up", "lake_out", 70000),
             ("RVc", "lake:RV", "lake_in", 80000), ("lake:KB", "lake:RV", "lake_out", 100000),
             ("KBc", "lake:KB", "lake_in", 150000), ("C:top", "lake:KB", "lake_in", 200000),
             ("CL", "C:top", "confluence", 250000)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {"wbk:UA": RegistryItem(id="wbk:UA", name="Upper Lake", kind="lake", section_ids=("lake:UA",)),
           "wbk:RV": RegistryItem(id="wbk:RV", name="Middle Reservoir", kind="lake", section_ids=("lake:RV",)),
           "wbk:KB": RegistryItem(id="wbk:KB", name="Top Reservoir", kind="lake", section_ids=("lake:KB",)),
           "gnis:DD": RegistryItem(id="gnis:DD", name="Big River drawdown", kind="stream", section_ids=("DD",)),
           "gnis:TOP": RegistryItem(id="gnis:TOP", name="Big River above", kind="stream", section_ids=("C:top",)),
           "gnis:U": RegistryItem(id="gnis:U", name="Big River below", kind="stream", section_ids=("U:0",))}
    return g, reg


ROW = {"entry_id": "r4:upper_lake_s_tributaries@4-1", "matched": ["wbk:UA"]}
RULE = {"rule_id": "upper_lakes_tributaries.r1", "verbatim": "Bull trout catch and release", "tributaries_only": True,
        "extents": [{"op": "whole"}, {"op": "whole", "item_id": "gnis:DD"}]}
#: the top reservoir's row, printing "Does not include Big River upstream of Top Reservoir" (Kinbasket's shape)
TOP = {"entry_id": "r4:top_reservoir_s_tributaries@4-1", "matched": ["wbk:KB"],
       "rules": [{"rule_id": "top_reservoir_tributaries.r1", "verbatim": "Bull trout catch and release",
                  "tributaries_only": True, "extents": [{"op": "whole"}],
                  "tributary_excludes": [{"op": "whole", "item_id": "gnis:TOP"}]}]}


def _bind(world, entry=ROW, rule=RULE):
    g, reg = world
    b, d = build_reach(entry, rule, reg, g, stops=lake_row_stops([TOP], reg, g))
    return set(b.sections), d


def test_the_lake_row_climbs_from_its_drawdown_head_to_the_next_rows_exception(arrow):
    """The river above the stretch's top is the lake's inflow: it and its creek, through the middle reservoir, into the
    top reservoir's creeks, and never past the top reservoir's printed exception (the river above it and its creek)."""
    got, d = _bind(arrow)
    assert got == {"D", "C:up", "J", "RVc", "KBc"}
    assert not got & {"C:top", "CL"}, "nothing above the top reservoir's printed exception"
    assert [x.payload["inlet"] for x in d if x.kind == "through_river"] == ["C:up"]
    assert [x.payload for x in d if x.kind == "lake_stop"] == [
        {"rows": [["r4:top_reservoir_s_tributaries@4-1", "top_reservoir_tributaries.r1"]]}]


def test_upper_end_mutation_pin(arrow, monkeypatch):
    """False = the walk stops at the stretch's top (the bridge): only the creek joining the drawdown stretch."""
    monkeypatch.setattr(B, "A_RIVER_STRETCH_IN_A_LAKE_ROW_IS_THE_LAKES_OWN_UPPER_END", False)
    got, _ = _bind(arrow)
    assert got == {"D"}


@pytest.mark.parametrize("on", [True, False])
def test_a_river_rows_stretch_never_climbs_above_its_top(arrow, monkeypatch, on):
    """"A stream is not a tributary of itself": the same stretch as a RIVER row's (its entry's water a stream) stops at its
    top, the policy on or off."""
    monkeypatch.setattr(B, "A_RIVER_STRETCH_IN_A_LAKE_ROW_IS_THE_LAKES_OWN_UPPER_END", on)
    river = {"entry_id": "r4:big_river@4-1", "matched": ["gnis:DD"]}
    rule = {"rule_id": "big_river.r1", "verbatim": "x", "tributaries_only": True, "extents": [{"op": "whole"}]}
    got, d = _bind(arrow, river, rule)
    assert got == {"D"} and not [x for x in d if x.kind == "through_river"]


def test_a_lake_rows_stretch_below_its_lake_is_no_upper_end(arrow, monkeypatch):
    """A stretch that does not drain into the row's lake (the river below it) is no upper end: the policy adds nothing
    (the lake's own through-river inflow is `EVERY_INFLOW_OF_A_RULES_LAKE_IS_ITS_TRIBUTARY`'s, the same either way)."""
    rule = {**RULE, "extents": [{"op": "whole"}, {"op": "whole", "item_id": "gnis:U"}]}
    on, d_on = _bind(arrow, rule=rule)
    monkeypatch.setattr(B, "A_RIVER_STRETCH_IN_A_LAKE_ROW_IS_THE_LAKES_OWN_UPPER_END", False)
    off, d_off = _bind(arrow, rule=rule)
    assert on == off
    assert [x.payload["inlet"] for x in d_on if x.kind == "through_river"] == ["DD"]
