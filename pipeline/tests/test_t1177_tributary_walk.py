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


def _area(reg, iid, secs):
    reg = dict(reg)
    reg[iid] = RegistryItem(id=iid, name="Test area", kind="area", section_ids=tuple(secs))
    return reg


def test_a_stretch_rule_still_stops_at_the_lake_at_its_top(chain, monkeypatch):
    """A stream is not a tributary of itself: a stretch whose reach takes in the lake at its top ("Down River from Lower
    Lake down", here an area that sweeps the lake in with the river) never takes what feeds the lake on its line (the
    upper lake and the river above it) — the lake is no lake the rule binds (`A_LAKE_AT_A_STRETCHS_TOP_IS_NO_LAKE_THE_RULE
    _BINDS`). Naming the lake (`item_id`) makes it the rule's lake, and then every inflow is its tributary."""
    g, reg = chain
    reg = _area(reg, "area:test:lower", ("D:0", "lake:LL"))
    entry = {"entry_id": "z4:lower_area", "matched": [], "includes_tributaries": True}   # an area row: no water of its own
    rule = {"rule_id": "lower_area.r1", "verbatim": "x",
            "extents": [{"op": "within", "area_id": "area:test:lower"}]}
    b, d = build_reach(entry, rule, reg, g, owned=OWNED)
    assert {"D:0", "C1"} <= set(b.sections), "the stretch, and the lake's own side creek (a tributary of the reach)"
    assert not set(b.sections) & {"C2", "R:up", "RT"}, "never the upper lake's water through the stretch's top lake"
    assert not [x for x in d if x.kind == "through_river"]
    named = dict(rule, extents=[{"op": "whole", "item_id": "gnis:D"}, {"op": "whole", "item_id": "wbk:LL"}])
    b, _ = build_reach({**entry, "matched": ["gnis:D"]}, named, reg, g, owned=OWNED)
    assert {"C2", "R:up", "RT"} <= set(b.sections), "a lake the rule names takes every inflow"
    monkeypatch.setattr(B, "A_LAKE_AT_A_STRETCHS_TOP_IS_NO_LAKE_THE_RULE_BINDS", False)
    b, _ = build_reach(entry, rule, reg, g, owned=OWNED)
    assert {"R:up"} <= set(b.sections), "False = every lake in the reach counts (the mutation pin)"


def test_a_river_passing_through_its_lakes_mid_reach_takes_their_inflows_never_the_lakes(chain):
    """A river row whose reach runs THROUGH lakes on its line (the lakes not in its item, as a named river's item leaves
    them out): the lakes are the river passing through (`LAKE_MID_REACH_IS_THE_RIVER`), their inflows tributaries, the
    lakes never covered and no `through_river` taken (the river above is the row's own)."""
    g, reg = chain
    reg = dict(reg)
    reg["gnis:DR"] = RegistryItem(id="gnis:DR", name="Down River", kind="stream", section_ids=("D:0", "R:up"))
    entry = {"entry_id": "r4:down_river@4-1", "matched": ["gnis:DR"], "includes_tributaries": True}
    rule = {"rule_id": "down_river.r1", "verbatim": "x",
            "extents": [{"op": "whole"}, {"op": "between", "splits": ["a", "b"]}]}
    b, d = build_reach(entry, rule, reg, g, owned=OWNED)
    assert set(b.sections) == {"D:0", "R:up", "C1", "C2", "RT"}
    assert not [x for x in d if x.kind == "through_river"]



def test_a_head_lake_climb_is_stopped_too_and_a_walk_that_never_meets_the_exception_says_nothing(chain):
    """One stop for every walk through the lake: the river's whole-water rule climbs its head lake (T18) up the chain
    and stops at the upper row's exception, reported; with the upper row's exception on water no walk reaches, no
    `lake_stop` is written."""
    g, reg = chain
    entry = {"entry_id": "r4:down_river@4-1", "matched": ["gnis:D"], "includes_tributaries": True}
    rule = {"rule_id": "down_river.r1", "verbatim": "x", "extents": [{"op": "whole"}]}
    got, d = _bind(chain, entry, rule, stops=lake_row_stops([UPPER], reg, g))
    assert got == {"D:0", "C1", "C2"} and [x.payload for x in d if x.kind == "lake_stop"]
    got, d = _bind(chain, stops=lake_row_stops([UPPER], reg, g))
    low = {"entry_id": "r4:low_creek@4-1", "matched": ["gnis:D"]}
    got, d = _bind(chain, low, {"rule_id": "low.r1", "verbatim": "x", "extents": [{"op": "whole"}]},
                   stops=lake_row_stops([UPPER], reg, g))
    assert got == {"D:0"} and not [x for x in d if x.kind == "lake_stop"], "no walk, no stop"


@pytest.fixture
def chain_with_a_lake_in_the_excepted_river(chain):
    """The chain, with a lake WL on the river above UL (the Columbia's Windermere Lake above Kinbasket): R:up flows out
    of WL, R:top flows into it on the same line, and the creek WC feeds it. UL's row excepts the whole river (gnis:R)."""
    g, reg = chain
    extra = [_n("lake:WL", L, name="Windermere-like Lake", wsc="900-1", order=5),
             _n("R:top", blk="D", lo=70000, hi=80000, name="Down River", wsc="900-1", order=4),
             _n("WC", blk="WC", name="Window Creek", wsc="900-1-000900", order=2)]
    for n in extra:
        g.nodes[n.node_id] = n
    for a, b, k, m in [("lake:WL", "R:up", "lake_out", 60000), ("R:top", "lake:WL", "lake_in", 70000),
                       ("WC", "lake:WL", "lake_in", 65000)]:
        g.edges.append(FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m))
        i = len(g.edges) - 1
        g.up_adj.setdefault(b, []).append(i)
        g.down_adj.setdefault(a, []).append(i)
    reg = dict(reg)
    reg["gnis:R"] = RegistryItem(id="gnis:R", name="Up River", kind="stream", section_ids=("R:up", "R:top"))
    reg["wbk:WL"] = RegistryItem(id="wbk:WL", name="Windermere-like Lake", kind="lake", section_ids=("lake:WL",))
    return g, reg


def test_a_stop_holds_only_for_a_walk_that_enters_its_lake(chain_with_a_lake_in_the_excepted_river):
    """Review of the book branch (T1177): a lake row INSIDE the excepted river (Windermere Lake's own tributaries) must
    keep its whole walk — UL's exception holds only for walks that enter UL. Before the fix every walk not touching UL
    or the excepted water was blocked by it, and this row bound nothing."""
    g, reg = chain_with_a_lake_in_the_excepted_river
    stops = lake_row_stops([UPPER], reg, g)
    assert {"WC"} <= set(stops[0].excluded) and "lake:WL" not in stops[0].excluded
    entry = {"entry_id": "r4:windermere_like_lake_s_tributaries@4-1", "matched": ["wbk:WL"]}
    rule = {"rule_id": "wl.r1", "verbatim": "x", "tributaries_only": True, "extents": [{"op": "whole"}]}
    got, d = _bind((g, reg), entry, rule, stops=stops)
    assert "WC" in got and not [x for x in d if x.kind == "lake_stop"]
    got, d = _bind((g, reg), stops=stops)                          # the lower lake's walk enters UL: stopped there
    assert not got & {"R:up", "R:top", "RT", "WC"}
    assert [x.payload for x in d if x.kind == "lake_stop"]


# --------------------------------------------------------------------------------------------------------------- #
# Review round 2 (AGENTS rule 19): A STOP HOLDS THROUGH ITS LAKE (`tributaries.A_STOP_HOLDS_THROUGH_ITS_LAKE`): the
# hold is where the walk is, never what it has visited, so the order `up_adj` lists the edges in cannot change it.
# Mutation: the flag False -> the two-order test fails (one order drops X).
# --------------------------------------------------------------------------------------------------------------- #

def _bypass(ul_first: bool):
    """The chain with the stretch S1 of the river between the lakes, and a creek X that feeds RT (inside UL's excepted
    river) AND spills into C1, the lower lake's own creek: a path into the excepted water that never passes UL. The two
    worlds differ only in the order the lower lake's inflows are listed."""
    nodes = [_n("D:0", blk="D", lo=0, hi=1000, wsc="900-1", order=6), _n("lake:LL", L, wsc="900-1", order=6),
             _n("lake:UL", L, wsc="900-1", order=6), _n("R:up", blk="D", lo=50000, hi=60000, wsc="900-1", order=5),
             _n("RT", blk="RT", wsc="900-1-000800", order=2), _n("C1", blk="C1", wsc="900-1-000300", order=2),
             _n("C2", blk="C2", wsc="900-1-000500", order=2), _n("S1", blk="D", lo=20000, hi=30000, wsc="900-1", order=6),
             _n("X", blk="X", wsc="900-1-000800-1", order=1)]
    into_ll = [("S1", "lake:LL", "lake_in", 30000), ("C1", "lake:LL", "lake_in", 20000)]
    edges = [("lake:LL", "D:0", "lake_out", 1000)] + (into_ll if ul_first else into_ll[::-1]) + [
        ("lake:UL", "S1", "lake_out", 0), ("R:up", "lake:UL", "lake_in", 50000), ("RT", "R:up", "confluence", 55000),
        ("C2", "lake:UL", "lake_in", 40000), ("X", "RT", "confluence", 10), ("X", "C1", "confluence", 10)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {"wbk:LL": RegistryItem(id="wbk:LL", name="Lower Lake", kind="lake", section_ids=("lake:LL",)),
           "wbk:UL": RegistryItem(id="wbk:UL", name="Upper Lake", kind="lake", section_ids=("lake:UL",)),
           "gnis:D": RegistryItem(id="gnis:D", name="Down River", kind="stream", section_ids=("D:0",)),
           "gnis:R": RegistryItem(id="gnis:R", name="Up River", kind="stream", section_ids=("R:up",))}
    return g, reg


def _bypass_bound(ul_first: bool) -> set:
    g, reg = _bypass(ul_first)
    stops = lake_row_stops([UPPER], reg, g)
    assert set(stops[0].excluded) == {"R:up", "RT", "X"}
    return _bind((g, reg), stops=stops)[0]


def test_a_stops_result_never_depends_on_the_edge_order():
    """The reviewer's probe (round 2): the same topology in two edge orders binds the same water. X is reached through
    C1 without passing UL, so UL's exception does not hold it there; RT and R:up are reached only through UL: held."""
    assert _bypass_bound(True) == _bypass_bound(False) == {"C1", "C2", "S1", "X"}


def test_the_stop_policy_mutation_pin(monkeypatch):
    """False = the old walk: the hold lasts the rest of the walk once UL is entered, and the edge order decides X."""
    monkeypatch.setattr(T, "A_STOP_HOLDS_THROUGH_ITS_LAKE", False)
    assert _bypass_bound(True) != _bypass_bound(False)


def test_two_stops_on_one_chain_each_hold_above_their_own_lake(chain_with_a_lake_in_the_excepted_river):
    """UL's row excepts its creek RT, WL's row (higher up the chain) the river R:top above WL. The lower lake's walk
    passes both lakes: RT held at UL, R:top at WL, the rest of the chain taken (R:up between them, WL's creek WC); both
    stops reported. UL's own walk is held at WL only (its own exception is its own row's)."""
    g, reg = chain_with_a_lake_in_the_excepted_river
    reg = dict(reg)
    reg["gnis:RT"] = RegistryItem(id="gnis:RT", name="Rt Creek", kind="stream", section_ids=("RT",))
    reg["gnis:RTOP"] = RegistryItem(id="gnis:RTOP", name="Top River", kind="stream", section_ids=("R:top",))
    upper = {**UPPER, "rules": [{**UPPER["rules"][0], "tributary_excludes": [{"op": "whole", "item_id": "gnis:RT"}]}]}
    top = {"entry_id": "r4:windermere_like_lake_s_tributaries@4-1", "matched": ["wbk:WL"],
           "rules": [{"rule_id": "wl.r1", "verbatim": "x", "tributaries_only": True, "extents": [{"op": "whole"}],
                      "tributary_excludes": [{"op": "whole", "item_id": "gnis:RTOP"}]}]}
    stops = lake_row_stops([upper, top], reg, g)
    assert [(s.lakes, s.excluded) for s in stops] == [(frozenset({"lake:UL"}), frozenset({"RT"})),
                                                      (frozenset({"lake:WL"}), frozenset({"R:top"}))]
    got, d = _bind((g, reg), stops=stops)
    assert got == {"C1", "C2", "R:up", "WC"}
    [row] = [x.payload for x in d if x.kind == "lake_stop"]
    assert sorted(map(tuple, row["rows"])) == [("r4:upper_lake_s_tributaries@4-1", "upper_lake_s_tributaries.r1"),
                                               ("r4:windermere_like_lake_s_tributaries@4-1", "wl.r1")]
    got, d = _bind((g, reg), upper, upper["rules"][0], stops=stops)
    assert got == {"C2", "R:up", "WC"}


def test_a_walk_that_starts_inside_the_excepted_water_is_never_held():
    """The walk from inside UL's excepted river (RT, the river R:up) climbs from there: it never passes UL, so the stop
    holds nothing for it (the walk itself, past `_expander`'s own-water test)."""
    g, _reg = _bypass(True)
    stop = [(frozenset({"lake:UL"}), frozenset({"R:up", "RT", "X"}), frozenset())]
    assert T.tributaries_of_reach(g, {"RT"}, stops=stop) == {"X"}
    assert T.tributaries_of_reach(g, {"R:up"}, stops=stop) == {"RT", "X"}


def test_a_dead_end_seed_in_a_stops_lake_brings_the_stop():
    """A seed is walked in the state of where it is: the Iskut's closed loop (lake LI and its channel IC, a dead end)
    seeded from its collected water, with a stop on LI holding out its feeder IU. Before round 2 a dead-end seed never
    entered its lake's stop, and IU was taken."""
    from pipeline.tests.test_walk_dead_ends import _stikine
    g = _stikine()
    got = T.tributaries_of_reach(g, {"S:0"}, stops=[(frozenset({"lake:LI"}), frozenset({"IU:0"}), frozenset())])
    assert {"lake:LI", "IC:0"} <= got and "IU:0" not in got
    assert "IU:0" in T.tributaries_of_reach(g, {"S:0"})
