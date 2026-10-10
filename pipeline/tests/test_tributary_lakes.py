"""The tributary walk and LAKES — three rulings of 2026-09-24.

1. TRIBUTARIES MEANS STREAMS. The book's glossary (p80): "tributaries: all streams that contribute
   to a larger stream or to a lake". A rule carrying `includes_tributaries` / `tributaries_only`
   collects STREAMS from its walk, never a lake. The walk still climbs THROUGH a lake — a creek
   feeding a tributary lake is a stream contributing to a lake — it just does not collect the
   lake. A row that names a lake binds it through its own extents; a watershed rule (basin ∩
   region) is an area, not a walk, and keeps its lakes. Measured before the ruling: 433 rules
   bound ~232,000 lake sections through the walk.

2. A LAKE IN THE MIDDLE OF THE REACH IS THE RIVER PASSING THROUGH, and the streams entering it are
   tributaries. A named river's item leaves out the lakes on its line (the Iskut's Tatogga,
   Eddontenajon and Kinaskan; the Williams Lake River's Williams Lake), so the walk arrived at such
   a lake from the piece below it by a `lake_out` edge — "the river continuing above the reach" —
   and never visited the lake's other inflows. 10,848 sections in 16 rules, 2,155 of them the
   Iskut's. The fix may NOT take the walk up the mainstem past the reach's top: a lake is "in the
   middle" only when the reach itself carries on above it, and at such a lake the inflow on the
   river's own line is still the river, not a tributary (the reservoir-chain rule, `_through_blks`
   / `_lake_on_line`, unchanged).

5. A BIFURCATION FOLLOWS THE WATERSHED CODE, NOT THE FLOW. Dewar Lake drains both ways: its main
   outlet south (Five Mile Creek → Borland → San Jose → Williams Lake River) and a secondary channel
   north into Seven Mile Lake (South Hawks → Hawks → the Fraser above the WLR). FWA codes the channel
   with Dewar Lake's own code. Where a lake's outlet carries a code into water whose code it does not
   descend from, that run belongs to the lake's watershed: a walk from below it (the Hawks side) does
   not climb into it, and a walk that reaches the lake (the WLR side) takes it with the lake.
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path

import pytest

from pipeline.atlas.graph.tributaries import code_runs, expand, tributaries_of_reach
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.tests.conftest import need, ATLAS_HINT

L = NodeKind.lake


def _n(nid, *, order=1, blk=None, kind=NodeKind.stream, wsc="", at=0.0):
    """`at` is the piece's lower route measure on its blue line (pieces are 100 m)."""
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


# ============================================================================ 1. streams only

@pytest.fixture
def trib_lake():
    """A creek feeding a lake that drains into the reach; the lake has a creek of its own above.

        feeder (F) --lake_in--> lake:t --lake_out--> outlet (O) --confluence--> river (reach)
    """
    return _g([_n("river", order=5, blk="R"), _n("outlet", order=2, blk="O"),
               _n("lake:t", order=2, kind=L), _n("feeder", order=1, blk="F")],
              [("outlet", "river", "confluence"), ("lake:t", "outlet", "lake_out"),
               ("feeder", "lake:t", "lake_in")])


def test_a_tributary_walk_collects_streams_never_lakes(trib_lake):
    got = expand(trib_lake, {"river"}, only=True)
    assert got == {"outlet", "feeder"}, "the lake is climbed through but not collected"


def test_includes_tributaries_keeps_the_rules_own_water_even_a_lake(trib_lake):
    """A row that names a lake binds it through its own extents: the REACH is kept whole."""
    got = expand(trib_lake, {"lake:t"})
    assert got == {"lake:t", "feeder"}


def test_the_topological_walk_still_knows_the_lake(trib_lake):
    """`tributaries_of_reach` answers what drains in (a carve-out blocks everything above it,
    lakes included); only the RULE's collection is streams-only."""
    assert "lake:t" in tributaries_of_reach(trib_lake, {"river"})


# ============================================================================ 2. lake mid-reach

@pytest.fixture
def through():
    """A river R running through a lake, with the reach above AND below it, and a lake at the top.

        feeder_top (T) --> lake:top --lake_out--> r_top (R)     <- the reach's top piece
                                                    | lake_in
        side_up (S2) --> side (S) --lake_in-->  lake:mid          (NOT in the reach: the item
                        tlake_in (Q) --> lake:tl --lake_out-->/    leaves lakes on its line out)
                                                    | lake_out
                                                  r_low (R)     <- the reach
                                                    ^ confluence
                                                  low_trib (B)
    """
    nodes = [_n("r_low", order=5, blk="R"), _n("r_top", order=5, blk="R", at=300.0),
             _n("lake:mid", order=5, kind=L), _n("lake:top", order=5, kind=L),
             _n("feeder_top", order=4, blk="T"),
             _n("side", order=2, blk="S"), _n("side_up", order=1, blk="S2"),
             _n("lake:tl", order=2, kind=L), _n("tlake_in", order=1, blk="Q"),
             _n("low_trib", order=2, blk="B")]
    edges = [("lake:mid", "r_low", "lake_out"), ("r_top", "lake:mid", "lake_in"),
             ("side", "lake:mid", "lake_in"), ("side_up", "side", "confluence"),
             ("lake:tl", "lake:mid", "lake_out"), ("tlake_in", "lake:tl", "lake_in"),
             ("lake:top", "r_top", "lake_out"), ("feeder_top", "lake:top", "lake_in"),
             ("low_trib", "r_low", "confluence")]
    return _g(nodes, edges)


def test_streams_entering_a_lake_on_the_rivers_line_are_tributaries(through):
    got = tributaries_of_reach(through, {"r_low", "r_top"})
    assert {"side", "side_up", "tlake_in"} <= got
    assert "low_trib" in got


def test_the_lake_on_the_line_is_the_river_not_a_tributary(through):
    assert "lake:mid" not in tributaries_of_reach(through, {"r_low", "r_top"})


def test_the_walk_never_climbs_past_the_reachs_top(through):
    """lake:top sits ABOVE the reach's top piece: nothing of the reach flows into it, so it is the
    river continuing above the regulated water, and its feeder is not a tributary of the reach."""
    got = tributaries_of_reach(through, {"r_low", "r_top"})
    assert not {"lake:top", "feeder_top"} & got


def test_a_reach_that_ENDS_at_the_lake_does_not_take_its_inflows(through):
    """`X River downstream of the lake`: the reach is below it only, so the lake is its top."""
    assert tributaries_of_reach(through, {"r_low"}) == {"low_trib"}


def test_the_rule_collects_the_streams_only(through):
    got = expand(through, {"r_low", "r_top"}, only=True)
    assert got == {"low_trib", "side", "side_up", "tlake_in"}


def test_a_lake_at_the_reachs_mouth_that_a_braid_enters_and_leaves_is_not_mid_reach():
    """The Stellako's last braid runs out of Fraser Lake and back into the river's mouth piece: the
    lake both receives from and drains to the reach, but the river does not pass THROUGH it — it
    ends in it. A looser test gave the Stellako every stream feeding Fraser Lake (2,393)."""
    g = _g([_n("mouth", order=5, blk="S", at=600.0), _n("braid", order=3, blk="B"),
            _n("lake:fraser", order=7, kind=L), _n("endako", order=4, blk="E")],
           [("mouth", "lake:fraser", "lake_in"), ("mouth", "braid", "confluence"),
            ("lake:fraser", "braid", "lake_out"), ("endako", "lake:fraser", "lake_in")])
    assert "endako" not in tributaries_of_reach(g, {"mouth", "braid"})


@pytest.fixture
def chain_mid():
    """Two lakes on the river's line back to back (a dam between them), mid-reach.

        r_top (R) -> lake:a -> lake:b -> r_low (R)
                      ^ sa     ^ sb
    """
    nodes = [_n("r_low", order=5, blk="R"), _n("r_top", order=5, blk="R", at=500.0),
             _n("lake:a", order=5, kind=L), _n("lake:b", order=5, kind=L),
             _n("sa", order=1, blk="SA"), _n("sb", order=1, blk="SB"),
             _n("above", order=5, blk="R", at=600.0)]
    edges = [("lake:b", "r_low", "lake_out"), ("lake:a", "lake:b", "lake_out"),
             ("r_top", "lake:a", "lake_in"), ("sa", "lake:a", "lake_in"),
             ("sb", "lake:b", "lake_in"), ("above", "r_top", "continuation")]
    return _g(nodes, edges)


def test_both_lakes_of_a_mid_reach_chain_give_up_their_tributaries(chain_mid):
    got = tributaries_of_reach(chain_mid, {"r_low", "r_top"})
    assert got == {"sa", "sb"}


def test_the_reservoir_chain_rule_still_holds_for_a_lake_reach(chain_mid):
    """T2: a reach that IS the lower lake takes the lake above as the river arriving — it is not
    mid-reach, because no piece of the reach flows into it."""
    assert tributaries_of_reach(chain_mid, {"lake:b"}) == {"sb"}


# ============================================================================ 5. bifurcation

@pytest.fixture
def dewar():
    """Dewar Lake (code W5) drains south by Five Mile Creek (its main outlet, W5) to the Williams
    Lake River (W), and north by a secondary channel (also coded W5) through a pond into Seven Mile
    Lake (H7), which drains by South Hawks Creek (H7) to Hawks Creek (H).

        wlr (W) <- five_mile (W5) <- lake:dewar (W5) -> chan_up (W5) -> lake:pond -> chan_dn (W5)
                                        ^ dewar_in (W5-1)          ^ chan_trib (W5-2)   |
                                                                                        v
        hawks (H) <- south_hawks (H7) <------------------------------------- lake:seven (H7)
                                                                                 ^ seven_in (H7-1)
    """
    W, W5, H, H7 = "100-382626", "100-382626-061403", "100-394295", "100-394295-295494"
    nodes = [_n("wlr", order=4, blk="WLR", wsc=W), _n("five_mile", order=2, blk="FM", wsc=W5),
             _n("lake:dewar", order=2, kind=L, wsc=W5), _n("dewar_in", order=1, blk="DI",
                                                            wsc=W5 + "-100000"),
             _n("chan_up", order=1, blk="CH", wsc=W5), _n("lake:pond", order=1, kind=L, wsc=W5),
             _n("chan_dn", order=1, blk="CH", wsc=W5),
             _n("chan_trib", order=1, blk="CT", wsc=W5 + "-200000"),
             _n("lake:seven", order=2, kind=L, wsc=H7), _n("seven_in", order=1, blk="SI",
                                                           wsc=H7 + "-100000"),
             _n("south_hawks", order=2, blk="SH", wsc=H7), _n("hawks", order=3, blk="HK", wsc=H)]
    edges = [("five_mile", "wlr", "confluence"), ("lake:dewar", "five_mile", "lake_out"),
             ("dewar_in", "lake:dewar", "lake_in"),
             ("lake:dewar", "chan_up", "lake_out"), ("chan_up", "lake:pond", "lake_in"),
             ("chan_trib", "chan_up", "confluence"),
             ("lake:pond", "chan_dn", "lake_out"), ("chan_dn", "lake:seven", "lake_in"),
             ("seven_in", "lake:seven", "lake_in"),
             ("lake:seven", "south_hawks", "lake_out"), ("south_hawks", "hawks", "confluence")]
    return _g(nodes, edges)


def test_the_run_is_found_by_code(dewar):
    runs, cuts = code_runs(dewar)
    assert set(runs["lake:dewar"]) == {"chan_up", "lake:pond", "chan_dn"}
    assert cuts == {("chan_dn", "lake:seven")}


def test_the_flow_side_walk_stops_at_the_code_divide(dewar):
    got = tributaries_of_reach(dewar, {"hawks"})
    assert {"south_hawks", "lake:seven", "seven_in"} <= got
    assert not {"chan_dn", "lake:pond", "chan_up", "chan_trib", "lake:dewar"} & got


def test_the_code_side_walk_takes_the_run_with_its_lake(dewar):
    got = tributaries_of_reach(dewar, {"wlr"})
    assert {"five_mile", "lake:dewar", "dewar_in", "chan_up", "lake:pond", "chan_dn",
            "chan_trib"} <= got
    assert not {"lake:seven", "seven_in", "south_hawks"} & got


def test_a_lake_reach_does_not_take_its_own_outflow(dewar):
    """"Dewar Lake and its tributaries": the channel is the lake's OUTflow (same code as the lake,
    the lake's own line), not water joining it."""
    assert tributaries_of_reach(dewar, {"lake:dewar"}) == {"dewar_in"}


def test_a_secondary_outlet_that_rejoins_its_own_code_is_no_run(dewar):
    """A distributary that comes back to its own watershed is an ordinary braid: nothing to move."""
    g = dewar
    for nid, wsc in (("lake:seven", "100-382626-061403"), ("south_hawks", "100-382626-061403"),
                     ("hawks", "100-382626")):
        g.nodes[nid] = dataclasses.replace(g.nodes[nid], wsc=wsc)
    runs, cuts = code_runs(g)
    assert not runs and not cuts


def test_a_lakes_main_outlet_is_never_a_run(dewar):
    """Nanika Lake's main river met a lake coded off its line, and the walk from below lost the lake
    and everything above it. Only a SECONDARY outlet moves by code: make the channel the bigger
    outlet and it is the lake's river, no run."""
    g = dewar
    g.nodes["chan_up"] = dataclasses.replace(g.nodes["chan_up"], stream_order=5)
    runs, cuts = code_runs(g)
    assert not runs and not cuts
    assert "chan_dn" in tributaries_of_reach(g, {"hawks"})


# ============================================================================ real data (slow)
#
# `ATLAS_BUILD` points these at a side build; the default is the promoted one.

@pytest.fixture(scope="module")
def real(request):
    from pipeline.atlas.registry import load_registry
    from pipeline.common.curated import GENERATED
    from pipeline.common.io.serialize import read_artifact
    b = Path(os.environ.get("ATLAS_BUILD") or GENERATED.build())
    need(request, "atlas", b / "graph.pkl", ATLAS_HINT)
    return read_artifact(str(b / "graph.pkl")), load_registry(str(b / "registry.json"))


def _online_lakes(g, reach):
    """Lakes not in `reach` that a piece of `reach` flows into AND out of which a piece of
    `reach` flows — the lakes in the middle of it."""
    out = set()
    for s in reach:
        for ei in g.down_adj.get(s, []):
            t = g.edges[ei].to_node
            if t in reach or g.nodes[t].kind != L:
                continue
            if any(g.edges[j].to_node in reach for j in g.down_adj.get(t, [])):
                out.add(t)
    return out


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_iskut_lake_chain_inflows_are_tributaries(real):
    """Tatogga, Eddontenajon and Kinaskan lie on the Iskut's line; the item leaves them out. Every
    stream entering them (Todagin Creek, Coyote Creek, ...) is a tributary of the Iskut."""
    g, reg = real
    reach = set(reg["gnis:10765"].section_ids)
    lakes = _online_lakes(g, reach)
    names = {g.nodes[x].display_name for x in lakes}
    assert {"Tatogga Lake", "Eddontenajon Lake", "Kinaskan Lake"} <= names
    got = expand(g, reach, only=True)
    for lake in lakes:
        for ei in g.up_adj.get(lake, []):
            src = g.edges[ei].from_node
            n = g.nodes[src]
            if src in reach or n.kind != NodeKind.stream or n.blk == g.nodes[next(iter(reach))].blk:
                continue
            if n.blk in {g.nodes[r].blk for r in reach}:
                continue
            assert src in got, f"{src} ({n.display_name}) enters {g.nodes[lake].display_name}"
    assert not {x for x in got if g.nodes[x].kind != NodeKind.stream}
    todagin = {x for x in got if g.nodes[x].display_name == "Todagin Creek"}
    assert todagin, "Todagin Creek joins Tatogga Lake"


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_williams_lake_inflows_are_tributaries_of_the_williams_lake_river(real):
    """The WLR's item runs on above Williams Lake, so the lake is mid-reach: the San Jose River
    and Borland Creek enter it and are the WLR's tributaries."""
    g, reg = real
    reach = set(reg["gnis:27764"].section_ids)
    got = expand(g, reach, only=True)
    names = {g.nodes[x].display_name for x in got}
    assert "San Jose River" in names
    assert "lake:329494714" not in got


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_the_walk_does_not_climb_the_fraser_from_a_reservoir_mid_reach(real):
    """The mid-reach rule does not undo the reservoir chain: Kinbasket's tributaries still do not
    include the Columbia above it."""
    g, reg = real
    kin = tributaries_of_reach(g, {"lake:328961767"})
    assert not kin & set(reg["gnis:37414"].section_ids)


@pytest.mark.needs_atlas
@pytest.mark.slow
def test_dewar_lake_channel_follows_its_code(real):
    """The secondary channel (blk 355993608) and pond 329586535 go with Dewar Lake's code — the
    Williams Lake River side — not with the flow into Seven Mile Lake (South Hawks, Hawks)."""
    g, _reg = real
    assert g.nodes["lake:329586257"].wsc == "100-394295-295494", "Seven Mile Lake's own code"
    runs, cuts = code_runs(g)
    run = set(runs.get("lake:329494751", ()))
    assert "lake:329586535" in run
    assert {x for x in run if g.nodes[x].blk == "355993608"}
    assert any(b == "lake:329586257" for _a, b in cuts)


# ============================================================================ mutation checks
#
# Each ruling is a named switch; turned off, the fixtures above must go red — so the tests are
# pinning the ruling, not a coincidence of the fixture.

def test_mutation_streams_only(trib_lake, monkeypatch):
    from pipeline.atlas.graph import tributaries as T
    monkeypatch.setattr(T, "STREAMS_ONLY", False)
    assert "lake:t" in expand(trib_lake, {"river"}, only=True)


def test_mutation_lake_mid_reach(through, chain_mid, monkeypatch):
    from pipeline.atlas.graph import tributaries as T
    monkeypatch.setattr(T, "LAKE_MID_REACH_IS_THE_RIVER", False)
    assert not {"side", "side_up", "tlake_in"} & tributaries_of_reach(through, {"r_low", "r_top"})
    assert tributaries_of_reach(chain_mid, {"r_low", "r_top"}) == frozenset()


def test_mutation_bifurcations(dewar, monkeypatch):
    from pipeline.atlas.graph import tributaries as T
    monkeypatch.setattr(T, "BIFURCATIONS_FOLLOW_THE_CODE", False)
    assert "chan_dn" in tributaries_of_reach(dewar, {"hawks"})
    assert "chan_dn" not in tributaries_of_reach(dewar, {"wlr"})
