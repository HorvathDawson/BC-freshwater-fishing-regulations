"""T1177 work item 10: ONE TRIBUTARY LOGIC — every inflow of a lake the rule binds is its tributary, the walk climbs
through every lake above, and only a printed exception keeps water out.

Owner, 2026-11-26 17:10 (docs/OWNER-RULINGS.md, "The tributary principle and chains"): "everything in is a tributary and
only shouldnt be if an exception is listed saying otherwise"; "climb through everything (a stream is not a trib of itself
tho so if stream a from x to y includes tribs then upstream of x is not included. lakes include all ...)". O2 (Upper
Chehalis River): "tributaries have a closure so in this case it needs to override"; O7 (Connor Lake's tributaries): "it
needs to apply to all tributaries of the lake".

The world is a reservoir chain on one blue line D, the shape of Lake Revelstoke under Kinbasket Lake (Mica Dam) and of
John Hart Lake under (Lower) Campbell Lake: the lower lake LL drains out by D:0 and is fed straight by the upper lake UL
(lake into lake); UL is fed by the river R:up on the same line, with a creek RT of its own; each lake has a creek of its
own (C1 into LL, C2 into UL). The river has a row of its own.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.graph import tributaries as T
from pipeline.atlas.reach import build as B
from pipeline.atlas.reach.build import build_reach, lake_row_stops
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem

S, L = NodeKind.stream, NodeKind.lake


def _n(nid, kind=S, *, blk="", lo=0.0, hi=100.0, name="", wsc="", order=2):
    return StreamNode(node_id=nid, kind=kind, blk=blk, wsc=wsc, display_name=name, down_m=lo,
                      up_m=hi, length_m=hi - lo, stream_order=order)


@pytest.fixture
def chain():
    nodes = [
        _n("D:0", blk="D", lo=0, hi=1000, name="Down River", wsc="900-1", order=6),
        _n("lake:LL", L, name="Lower Lake", wsc="900-1", order=6),
        _n("lake:UL", L, name="Upper Lake", wsc="900-1", order=6),
        _n("R:up", blk="D", lo=50000, hi=60000, name="Down River", wsc="900-1", order=5),
        _n("RT", blk="RT", wsc="900-1-000800", order=2),
        _n("C1", blk="C1", name="Low Creek", wsc="900-1-000300", order=2),
        _n("C2", blk="C2", name="High Creek", wsc="900-1-000500", order=2),
    ]
    edges = [("lake:LL", "D:0", "lake_out", 1000), ("lake:UL", "lake:LL", "lake_out", 30000),
             ("R:up", "lake:UL", "lake_in", 50000), ("RT", "R:up", "confluence", 55000),
             ("C1", "lake:LL", "lake_in", 20000), ("C2", "lake:UL", "lake_in", 40000)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {"wbk:LL": RegistryItem(id="wbk:LL", name="Lower Lake", kind="lake", section_ids=("lake:LL",)),
           "wbk:UL": RegistryItem(id="wbk:UL", name="Upper Lake", kind="lake", section_ids=("lake:UL",)),
           "gnis:D": RegistryItem(id="gnis:D", name="Down River", kind="stream", section_ids=("D:0",)),
           "gnis:R": RegistryItem(id="gnis:R", name="Up River", kind="stream", section_ids=("R:up",)),
           "gnis:O": RegistryItem(id="gnis:O", name="Outlet Creek", kind="stream", section_ids=("D:0",))}
    return g, reg


#: the river and the upper lake each have a row of their own
OWNED = {"gnis:R": ("r4:up_river@4-1",), "wbk:UL": ("r4:upper_lake_s_tributaries@4-1",)}
LOWER = {"entry_id": "r4:lower_lake_s_tributaries@4-1", "matched": ["wbk:LL"]}
LOWER_RULE = {"rule_id": "lower_lake_s_tributaries.r1", "verbatim": "Bull trout catch and release",
              "tributaries_only": True, "extents": [{"op": "whole"}]}
#: the upper lake's own tributaries row, printing "Does not include Up River upstream" (Kinbasket's shape)
UPPER = {"entry_id": "r4:upper_lake_s_tributaries@4-1", "matched": ["wbk:UL"],
         "rules": [{"rule_id": "upper_lake_s_tributaries.r1", "verbatim": "Bull trout catch and release",
                    "tributaries_only": True, "extents": [{"op": "whole"}],
                    "tributary_excludes": [{"op": "whole", "item_id": "gnis:R"}]}]}


def _bind(world, entry=LOWER, rule=LOWER_RULE, *, stops=None):
    g, reg = world
    b, d = build_reach(entry, rule, reg, g, owned=OWNED, stops=stops)
    return set(b.sections), d


def test_a_lake_rows_walk_climbs_the_whole_chain_with_the_river_that_has_its_own_row(chain):
    """"climb through everything ... lakes include all": the lower lake's tributaries take the upper lake's creek and
    the river feeding the upper lake (a row of its own) with its creek; no lake is covered (T1)."""
    got, d = _bind(chain)
    assert got == {"C1", "C2", "R:up", "RT"}
    [row] = [x.payload for x in d if x.kind == "through_river"]
    assert row["inlet"] == "lake:UL" and row["joined"] == 3, "the upper lake's creek, the river and its creek"


def test_the_upper_lake_rows_printed_exception_stops_the_lower_walk_there(chain):
    """The narrower reading, built while the rulings do not decide it (Kinbasket's "Does not include Columbia River
    upstream of Kinbasket Reservoir" against Lake Revelstoke's and Lower Arrow Lake's walks): the upper row's carve-out
    holds for every walk through its lake. Reported as `lake_stop`."""
    g, reg = chain
    stops = lake_row_stops([UPPER], reg, g)
    assert [(s.entry_id, s.lakes, s.excluded) for s in stops] == [
        ("r4:upper_lake_s_tributaries@4-1", frozenset({"lake:UL"}), frozenset({"R:up", "RT"}))]
    got, d = _bind(chain, stops=stops)
    assert got == {"C1", "C2"}
    assert [x.payload for x in d if x.kind == "lake_stop"] == [
        {"rows": [["r4:upper_lake_s_tributaries@4-1", "upper_lake_s_tributaries.r1"]]}]


def test_a_stop_never_touches_its_own_rows_walk_nor_the_excepted_waters_own(chain):
    g, reg = chain
    stops = lake_row_stops([UPPER], reg, g)
    river = {"entry_id": "r4:up_river@4-1", "matched": ["gnis:R"], "includes_tributaries": True}
    got, d = _bind(chain, river, {"rule_id": "up_river.r1", "verbatim": "x", "extents": [{"op": "whole"}]},
                   stops=stops)
    assert got == {"R:up", "RT"} and not [x for x in d if x.kind == "lake_stop"]


def test_stop_mutation_pin(chain, monkeypatch):
    """False = the literal reading: the lower row's walk climbs past the upper row's exception."""
    g, reg = chain
    monkeypatch.setattr(B, "A_LAKES_PRINTED_EXCEPTION_STOPS_EVERY_WALK_THROUGH_IT", False)
    got, _ = _bind(chain, stops=lake_row_stops([UPPER], reg, g))
    assert got == {"C1", "C2", "R:up", "RT"}


def test_only_a_lake_tributaries_rows_carve_out_is_a_stop(chain):
    """A river row's carve-out is no stop: the stop is a LAKE's row (`tributaries_only`, every water it binds a lake)."""
    g, reg = chain
    river_row = {"entry_id": "r4:down_river@4-1", "matched": ["gnis:D"],
                 "rules": [{"rule_id": "down_river.r1", "verbatim": "x", "tributaries_only": True,
                            "extents": [{"op": "whole"}], "tributary_excludes": [{"op": "whole", "item_id": "gnis:R"}]}]}
    assert lake_row_stops([river_row], reg, g) == ()


def test_every_inflow_policy_mutation_pin(chain, monkeypatch):
    """False = T468's rule: the next lake up the line, which has a row of its own (and the river above it), stays out of
    the lower lake's walk."""
    monkeypatch.setattr(T, "EVERY_INFLOW_OF_A_RULES_LAKE_IS_ITS_TRIBUTARY", False)
    got, _ = _bind(chain)
    assert got == {"C1"}


def test_an_inlet_streams_row_binds_every_inflow_of_its_lake_by_the_walk(chain):
    """O7 for "X Lake's inlet & outlet streams" (Whitetail Lake): the row is matched to its outlet; its inlet rule names
    the lake by `item_id` (`tributaries_only`), and every inflow of the lake is bound by the walk — the lake on its line
    and what feeds it included — never a hand list. Any lake the rule binds counts, not only the entry's own."""
    entry = {"entry_id": "r4:lower_lake_s_inlet_outlet_streams@4-1", "matched": ["gnis:O"]}
    rule = {"rule_id": "lower_lakes_inlet_outlet_streams.r2", "verbatim": "No Fishing", "tributaries_only": True,
            "extents": [{"op": "whole", "item_id": "wbk:LL"}]}
    got, _ = _bind(chain, entry, rule)
    assert got == {"C1", "C2", "R:up", "RT"}
    assert "D:0" not in got, "the outlet is the row's other rule's (its own water), never the inlets' walk"


def test_a_stretch_rule_still_stops_at_the_lake_at_its_top(chain):
    """A stream is not a tributary of itself: "Down River downstream of Lower Lake, incl. tributaries" never takes what
    feeds the lake at its top (RULINGS T18: a stretch rule does not climb)."""
    entry = {"entry_id": "r4:down_river@4-1", "matched": ["gnis:D"], "includes_tributaries": True}
    rule = {"rule_id": "down_river.r1", "verbatim": "x",
            "extents": [{"op": "whole"}, {"op": "between", "splits": ["a", "b"]}]}
    # a two-extent rule is no whole-water rule (`climbs_head_lakes`); the cut extent is unresolved here, the whole binds
    got, _ = _bind(chain, entry, rule)
    assert got == {"D:0"}
