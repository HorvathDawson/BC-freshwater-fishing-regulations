"""T1177, Whiteswan: WATER JOINING THE ROW'S OWN STREAM ABOVE ITS LAKE IS THE LAKE'S TRIBUTARY
(`build.WATER_JOINING_THE_ROWS_OWN_STREAM_ABOVE_ITS_LAKE_IS_THE_LAKES_TRIBUTARY`).

Owner, 2026-11-27 ~11:00, with the map in front of him (RULINGS T19, E26): "Include them: No Fishing all year
(Recommended)": the creeks feeding Whiteswan's outlet item's line ABOVE the lake are inflows of the lake, closed like its
other inlets. The general statement: everything flowing into a lake is its tributary; a stream is not a tributary of
itself (the own stream keeps its own rules, and a river row never climbs).

The world is Whiteswan's shape: one blue line W, cut at the falls (W:0 below, W:599 from the falls up to the lake), the
lake L on it, and the same line above the lake (W:up), all one stream item (the row's own water). Creek K joins W:up;
Moose Lake M drains into W:up by its stream MS, fed by creek MT; inlet creek I flows straight into the lake.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import build as B
from pipeline.atlas.reach.build import build_reach
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem

S, L = NodeKind.stream, NodeKind.lake


def _n(nid, kind=S, *, blk="", lo=0.0, hi=100.0, name="", wsc="", order=2):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wsc=wsc, display_name=name, down_m=lo,
                      up_m=hi, length_m=hi - lo, stream_order=order)


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


@pytest.fixture
def whiteswan():
    nodes = [
        _n("W:0", blk="W", lo=0, hi=599, wsc="300-1", order=3),
        _n("W:599", blk="W", lo=599, hi=3000, wsc="300-1", order=3),
        _n("lake:L", L, name="White Lake", wsc="300-1", order=3),
        _n("W:up", blk="W", lo=9000, hi=10100, wsc="300-1", order=3),
        _n("K", blk="K", wsc="300-1-000500", order=1),
        _n("MS", blk="MS", wsc="300-1-000700", order=2),
        _n("lake:M", L, name="Moose Lake", wsc="300-1-000700", order=2),
        _n("MT", blk="MT", wsc="300-1-000700-000100", order=1),
        _n("I", blk="I", name="Inlet Creek", wsc="300-1-000300", order=1),
    ]
    edges = [("W:599", "W:0", "continuation", 599), ("lake:L", "W:599", "lake_out", 3000),
             ("W:up", "lake:L", "lake_in", 9000), ("K", "W:up", "confluence", 9500),
             ("MS", "W:up", "confluence", 9800), ("lake:M", "MS", "lake_out", 200), ("MT", "lake:M", "lake_in", 50),
             ("I", "lake:L", "lake_in", 5000)]
    reg = {"wbk:L": RegistryItem(id="wbk:L", name="White Lake", kind="lake", section_ids=("lake:L",)),
           "wbk:M": RegistryItem(id="wbk:M", name="Moose Lake", kind="lake", section_ids=("lake:M",)),
           "wsc:W": RegistryItem(id="wsc:W", name="White Lake outlet", kind="stream",
                                 section_ids=("W:0", "W:599", "W:up")),
           "gnis:I": RegistryItem(id="gnis:I", name="Inlet Creek", kind="stream", section_ids=("I",)),
           "gnis:R": RegistryItem(id="gnis:R", name="White River", kind="stream", section_ids=("W:0", "W:599")),
           "gnis:O": RegistryItem(id="gnis:O", name="White Lake outlet", kind="stream", section_ids=("W:599",))}
    return _graph(nodes, edges), reg


#: Whiteswan's row: matched the outlet item (its line runs through the lake); r5 the lake whole, tributaries only
ROW = {"entry_id": "r4:white_lake_s_inlet_outlet_streams@4-1", "matched": ["wsc:W"]}
INLETS = {"rule_id": "white_lakes_inlet_outlet_streams.r5", "verbatim": "No Fishing", "tributaries_only": True,
          "extents": [{"op": "whole", "item_id": "wbk:L"}]}


def _bind(world, entry=ROW, rule=INLETS):
    g, reg = world
    b, d = build_reach(entry, rule, reg, g)
    return set(b.sections), d


def test_what_joins_the_own_stream_above_the_lake_is_the_lakes_inlet(whiteswan):
    """The creek and Moose Lake's streams feeding the own line above the lake are bound; the own line (W:up) is never the
    walk's (r1's own water), no lake is collected, nothing below the lake is reached."""
    got, d = _bind(whiteswan)
    assert got == {"I", "K", "MS", "MT"}
    assert [x.payload for x in d if x.kind == B.OWN_STREAM_CLIMBED] == [
        {"lake": "lake:L", "inlet": "W:up", "water": "(unnamed)", "joined": 3}]


def test_own_stream_mutation_pin(whiteswan, monkeypatch):
    """False = the own water holds what joins it out of the walk (AGENTS 63 alone): only the lake's direct inlet."""
    monkeypatch.setattr(B, "WATER_JOINING_THE_ROWS_OWN_STREAM_ABOVE_ITS_LAKE_IS_THE_LAKES_TRIBUTARY", False)
    got, d = _bind(whiteswan)
    assert got == {"I"} and not [x for x in d if x.kind == B.OWN_STREAM_CLIMBED]


@pytest.mark.parametrize("on", [True, False])
def test_a_river_row_through_its_mid_river_lake_does_not_change(whiteswan, monkeypatch, on):
    """A river row (its reach holds streams) through a lake on its line: "a stream is not a tributary of itself". Its
    whole-water walk is the same with the policy on or off: the river's stretch below the lake; the lake at its top keeps
    what is above it as the river going on (the own line above, W:up, stays the row's own water)."""
    monkeypatch.setattr(B, "WATER_JOINING_THE_ROWS_OWN_STREAM_ABOVE_ITS_LAKE_IS_THE_LAKES_TRIBUTARY", on)
    row = {"entry_id": "r4:white_river@4-1", "matched": ["wsc:W"]}
    stretch = {"rule_id": "white_river.r1", "verbatim": "No Fishing", "includes_tributaries": True,
               "extents": [{"op": "whole", "item_id": "gnis:R"}]}
    got, d = _bind(whiteswan, row, stretch)
    assert got == {"W:0", "W:599"} and not [x for x in d if x.kind == B.OWN_STREAM_CLIMBED]
    whole = {"rule_id": "white_river.r2", "verbatim": "No Fishing", "includes_tributaries": True,
             "extents": [{"op": "whole"}]}
    got, d = _bind(whiteswan, row, whole)
    assert "W:up" in got and not [x for x in d if x.kind == B.OWN_STREAM_CLIMBED]
    assert got == _expected_whole_river(whiteswan, monkeypatch)


def _expected_whole_river(world, monkeypatch):
    with monkeypatch.context() as m:
        m.setattr(B, "WATER_JOINING_THE_ROWS_OWN_STREAM_ABOVE_ITS_LAKE_IS_THE_LAKES_TRIBUTARY", False)
        got, _ = _bind(world, {"entry_id": "r4:white_river@4-1", "matched": ["wsc:W"]},
                       {"rule_id": "white_river.r2", "verbatim": "No Fishing", "includes_tributaries": True,
                        "extents": [{"op": "whole"}]})
    return got


@pytest.mark.parametrize("on", [True, False])
def test_an_outlet_only_row_does_not_climb(whiteswan, monkeypatch, on):
    """A row on the outlet below the lake ("the outlet stream, incl. tributaries"): its reach is a stream, so the policy
    never fires, on or off. As a stretch (no head-lake climb) nothing above the lake is its; bound to its whole water it
    climbs its head lake by T1053 (RULINGS T18), the same with the policy on or off."""
    g, reg = whiteswan
    row = {"entry_id": "r4:white_lake_outlet@4-1", "matched": ["gnis:O"]}
    rule = {"rule_id": "white_lake_outlet.r1", "verbatim": "No Fishing", "tributaries_only": True,
            "extents": [{"op": "whole"}]}
    monkeypatch.setattr(B, "WATER_JOINING_THE_ROWS_OWN_STREAM_ABOVE_ITS_LAKE_IS_THE_LAKES_TRIBUTARY", on)
    b, d = build_reach(row, rule, reg, g, head_lakes=False)
    assert not set(b.sections) & {"I", "K", "MS", "MT", "W:up"}
    assert not [x for x in d if x.kind == B.OWN_STREAM_CLIMBED]
    b, d = build_reach(row, rule, reg, g)
    assert set(b.sections) == {"I", "K", "MS", "MT", "W:up"}, "T1053's head lake, never this policy"
    assert not [x for x in d if x.kind == B.OWN_STREAM_CLIMBED]


def test_a_carve_out_on_the_own_stream_still_holds(whiteswan):
    """A printed exception keeps water out: a carve-out on the own line above the lake blocks it and what joins it."""
    g, reg = whiteswan
    reg = {**reg, "gnis:U": RegistryItem(id="gnis:U", name="Upper line", kind="stream", section_ids=("W:up",))}
    rule = {**INLETS, "tributary_excludes": [{"op": "whole", "item_id": "gnis:U"}]}
    got, d = _bind((g, reg), rule=rule)
    assert got == {"I"} and not [x for x in d if x.kind == B.OWN_STREAM_CLIMBED]


def _add(world, nodes, edges, items):
    g, reg = world
    for n in nodes:
        g.nodes[n.node_id] = n
    for a, b, k, m in edges:
        g.edges.append(FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m))
        i = len(g.edges) - 1
        g.up_adj.setdefault(b, []).append(i)
        g.down_adj.setdefault(a, []).append(i)
    return g, {**reg, **items}


def test_the_rules_words_keep_the_own_stream_out_too(whiteswan):
    """A printed exception is a carve-out OR the rule's words ("not including Snow Creek", `_rule_excludes`): the own
    stream so named is not climbed, and that is reported (`negated`)."""
    g, reg = whiteswan
    g.nodes["W:up"] = _n("W:up", blk="W", lo=9000, hi=10100, name="Snow Creek", wsc="300-1", order=3)
    rule = {**INLETS, "verbatim": "No Fishing, not including Snow Creek"}
    got, d = _bind((g, reg), rule=rule)
    assert got == {"I"}
    assert [x.payload for x in d if x.kind == B.OWN_STREAM_CLIMBED] == [
        {"lake": "lake:L", "inlet": "W:up", "water": "Snow Creek", "joined": 0, "negated": True}]


def test_two_own_streams_into_one_lake_are_both_climbed(whiteswan):
    """The row's own water holds a second stream into the lake (V, with creek KV): both are walked past, what joins each
    is the lake's, neither own stream is collected; one report each."""
    world = _add(whiteswan, [_n("V", blk="V", lo=0, hi=2000, wsc="300-1-000200", order=2),
                             _n("KV", blk="KV", wsc="300-1-000200-000100", order=1)],
                 [("V", "lake:L", "lake_in", 4000), ("KV", "V", "confluence", 1000)],
                 {"gnis:V": RegistryItem(id="gnis:V", name="Vee Creek", kind="stream", section_ids=("V",))})
    got, d = _bind(world, {**ROW, "matched": ["wsc:W", "gnis:V"]})
    assert got == {"I", "K", "MS", "MT", "KV"}
    assert sorted(x.payload["inlet"] for x in d if x.kind == B.OWN_STREAM_CLIMBED) == ["V", "W:up"]
