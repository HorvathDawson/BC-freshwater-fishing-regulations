"""T804 (owner 2026-11-16, question 9): ONE STATED RULE — a joining water with a row of its own is not inside its
neighbour's stretch rule, and the joining water at a confluence end is the stream that BRINGS the named water to the
line (RULINGS T9).

The Morice's "No Fishing from Gosnell Creek to Lamprey Creek" ends at a cut named for Gosnell Creek, which never meets
the Morice: it joins the Thautil River 222 m above the Thautil's mouth, and the Thautil meets the Morice 20 m below the
cut. The lookup asked only for Gosnell's own mouth within 1 m, found none, and T9 never ran: the closure took the
Thautil, Gosnell Creek and every stream feeding them. Here in miniature:

    M:1000 (above the cut c, "G Creek -> Main River", m=1000)
       |
    M:0    <- R (R River, mouth at 980: 20 m below the cut) <- G (G Creek, joins R at 222) <- G_trib
           <- Q (Q Creek, mouth at 900: 100 m below the cut, nothing named G above it)
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import build as B
from pipeline.atlas.reach.build import build_reach
from pipeline.common.models import FlowEdge, StreamGraph
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.tests.test_reach_confluence_cuts import _cut, _node


@pytest.fixture
def world():
    nodes = [
        _node("M:0", "M", 0, 1000, name="Main River", wsc="100-1", order=5, upper=_cut("c", 1000)),
        _node("M:1000", "M", 1000, 3000, name="Main River", wsc="100-1", order=5, lower=_cut("c", 1000)),
        _node("R", "R", 0, 600, name="R River", wsc="100-1-000980", order=4),
        _node("G", "G", 0, 400, name="G Creek", wsc="100-1-000980-000222", order=3),
        _node("G_trib", "GT", 0, 100, wsc="100-1-000980-000222-1", order=1),
        _node("Q", "Q", 0, 300, name="Q Creek", wsc="100-1-000900", order=2),
    ]
    edges = [("M:1000", "M:0", "continuation", 1000), ("R", "M:0", "confluence", 980),
             ("G", "R", "confluence", 222), ("G_trib", "G", "confluence", 50), ("Q", "M:0", "confluence", 900)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    bds = (RegistryBoundary(id="c", label="G Creek → Main River", kind="confluence", ref="split:c"),)
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="Main River", kind="stream", section_ids=("M:0", "M:1000"),
                                  boundaries=bds),
           "gnis:2": RegistryItem(id="gnis:2", name="R River", kind="stream", section_ids=("R",)),
           "gnis:3": RegistryItem(id="gnis:3", name="G Creek", kind="stream", section_ids=("G",))}
    return g, reg


ENTRY = {"entry_id": "r9:main@9-1", "matched": ["gnis:1"], "includes_tributaries": True}
DOWN = {"op": "downstream_of", "splits": ["c"]}
UP = {"op": "upstream_of", "splits": ["c"]}
CARRIED = {"R", "G", "G_trib"}


def _bind(world, ext, owned):
    g, reg = world
    b, d = build_reach(ENTRY, {"rule_id": "main.r1", "verbatim": "No Fishing from G Creek downstream",
                               "extents": [ext]}, reg, g, owned=owned)
    return set(b.sections), [x.payload for x in d if x.kind == "confluence_cut"]


def test_the_water_that_brings_the_named_creek_is_the_joining_water(world):
    """R River carries G Creek to the line 20 m from the cut: it is the joining water, and with a row of its own it
    and everything above it (G Creek) stay out of the stretch rule (T9)."""
    got, diags = _bind(world, DOWN, {"gnis:2": ("r9:r_river@9-1",)})
    assert not got & CARRIED
    assert {"M:0", "Q"} <= got
    assert [(d["water"], d["with_cut"], d["kept_out"], d["found"]) for d in diags] == [
        ("R River", False, 3, "it carries the named water, 20 m from the cut")]


def test_a_bringer_with_no_row_goes_with_the_cut_on_both_sides(world):
    """The same water with no row of its own goes with the cut — taken by the stretch above the cut too."""
    up, diags = _bind(world, UP, {})
    assert CARRIED <= up
    assert diags[0]["with_cut"] is True and diags[0]["water"] == "R River"


def test_a_creek_near_the_cut_that_does_not_carry_the_named_water_is_not_it(world):
    """Q Creek joins 100 m from the cut but nothing named G Creek is above it: it is walked as any tributary."""
    got, diags = _bind(world, DOWN, {"gnis:2": ("r9:r_river@9-1",)})
    assert "Q" in got and all(d["water"] != "Q Creek" for d in diags)


def test_mutation_pin_without_the_rule_the_closure_takes_the_carried_water(world, monkeypatch):
    monkeypatch.setattr(B, "THE_WATER_AT_A_CONFLUENCE_CUT_IS_THE_ONE_THAT_BRINGS_ITS_NAMED_WATER", False)
    got, diags = _bind(world, DOWN, {"gnis:2": ("r9:r_river@9-1",)})
    assert CARRIED <= got and not diags
