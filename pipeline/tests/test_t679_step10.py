"""T679 step 10, prepared for the wave-4 atlas build (2026-11-20): the switches are off until that build, and each is
pinned here both ways on a hand-made graph. No data.

  #1  ONE outside-B.C. flag (`border.ONE_OUTSIDE_FLAG`): the atlas stamps `out_of_bc` on every node in no region polygon
      (`border.mark_regionless`); the reach then reads the flag alone and refuses an atlas that left one unstamped.
  #6  the registry owns every named node (`registry.build.NAMED_NODES_ARE_IN_ITEMS`): a node no water holds loses its
      `display_name` (`clear_unheld_names`), its `name_tuples` kept.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import outside as O
from pipeline.atlas.registry import build as RB
from pipeline.atlas.splits import border as BD
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.registry import RegistryItem


def _world():
    """River R (two pieces) in Region 1; S:0, a border sliver no region claims (midpoint inside the outline, so not
    `out_of_bc`); X:0 past the border (`out_of_bc`); a named ditch D:0 no water holds."""
    nodes = [StreamNode(node_id="R:0", kind=NodeKind.stream, blk="R", display_name="R River", up_m=100, length_m=100),
             StreamNode(node_id="R:100", kind=NodeKind.stream, blk="R", display_name="R River", down_m=100, up_m=200,
                        length_m=100),
             StreamNode(node_id="S:0", kind=NodeKind.stream, blk="S", up_m=10, length_m=10),
             StreamNode(node_id="X:0", kind=NodeKind.stream, blk="X", up_m=50, length_m=50, out_of_bc=True),
             StreamNode(node_id="D:0", kind=NodeKind.stream, blk="D", display_name="R River", up_m=30, length_m=30)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node="R:100", to_node="R:0", at_measure=100, kind="continuation")]
    g.up_adj, g.down_adj = {"R:0": [0]}, {"R:100": [0]}
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="R River", kind="stream", section_ids=("R:0", "R:100")),
           "area:region:1": RegistryItem(id="area:region:1", name="Region 1", kind="area",
                                         section_ids=("R:0", "R:100", "D:0"))}
    return g, reg


def test_1_off_the_reach_adds_the_regionless_sliver_itself():
    g, reg = _world()
    assert BD.ONE_OUTSIDE_FLAG is False                     # wave 4 flips it, with the rebuild
    assert O.outside_bc(reg, g) == {"S:0", "X:0"}
    assert BD.regionless(g, reg) == {"S:0", "X:0"}


def test_1_on_the_atlas_stamps_the_sliver_and_the_reach_reads_the_flag_alone(monkeypatch):
    g, reg = _world()
    monkeypatch.setattr(BD, "ONE_OUTSIDE_FLAG", True)
    with pytest.raises(SystemExit, match="not stamped"):  # an atlas built before the stamp is refused, never patched
        O.outside_bc(reg, g)
    g, reg = _world()
    assert BD.mark_regionless(g, reg) == 1 and g.nodes["S:0"].out_of_bc
    assert BD.mark_regionless(g, reg) == 0                 # once
    assert O.outside_bc(reg, g) == {"S:0", "X:0"}          # the same answer, from the one flag


def test_1_a_registry_with_no_region_says_nothing():
    g, reg = _world()
    del reg["area:region:1"]
    assert BD.regionless(g, reg) == frozenset() and BD.mark_regionless(g, reg) == 0


def test_6_the_named_node_no_water_holds_is_listed_and_on_loses_its_name():
    g, reg = _world()
    assert RB.NAMED_NODES_ARE_IN_ITEMS is False
    assert RB.unheld_named(g, reg) == ["D:0"]                # an area item is no water
    assert RB.count_by_kind(g, ["D:0"]) == {"stream": 1}
    assert RB.clear_unheld_names(g, reg) == 1
    assert g.nodes["D:0"].display_name == "" and RB.unheld_named(g, reg) == []
    assert g.nodes["R:0"].display_name == "R River"
