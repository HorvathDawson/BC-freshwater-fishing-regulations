"""T1029 (owner 2026-11-24: "No stripe on creeks and only tributaries on the one bank side get included"; on the
Kitimat's 12 creeks: "i think closed we need a clean way to have this calculated from extents so it fits into structure
nicely"): a one-bank place's tributary walk keeps only the systems that JOIN ITS STRETCH ON THAT BANK — read from FWA's
own LEFT_RIGHT_TRIBUTARY at the root and the stretch's drawn banks — and says why it drops or cannot read one."""

from __future__ import annotations

import pytest

from pipeline.atlas.graph.blk_chains import _left_right
from pipeline.atlas.reach.dfo_salmon import TRIB_OTHER_BANK, TRIB_SIDE_UNDETERMINED, one_bank_tributaries
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode


def _graph(nodes: dict, edges: list) -> StreamGraph:
    g = StreamGraph()
    for nid, (kind, lr, banks) in nodes.items():
        g.nodes[nid] = StreamNode(node_id=nid, kind=kind, left_right=lr, drawn_banks=banks)
    for i, (a, b) in enumerate(edges):
        g.edges.append(FlowEdge(from_node=a, to_node=b, at_measure=0.0))
        g.down_adj.setdefault(a, []).append(i)
        g.up_adj.setdefault(b, []).append(i)
    return g


S, L = NodeKind.stream, NodeKind.lake
#: the stretch S, drawn mouth -> source with "east" RIGHT of its line ("R-"): west is LEFT of the drawn line, i.e.
#: looking DOWNSTREAM the west bank is on the RIGHT hand — as the Kitimat at the hatchery signs (FWA: 360639090 RIGHT)
NODES = {
    "stem:0": (S, "", "R-"),
    "a:0": (S, "RIGHT", ""),      # joins the stretch on the west bank
    "a2:0": (S, "LEFT", ""),      # a creek of a: its own LEFT is relative to a, never to the stretch
    "lk": (L, "", ""),            # a pond on a's system (the Kitimat's sewage lagoons)
    "a3:0": (S, "RIGHT", ""),     # above the pond
    "b:0": (S, "LEFT", ""),       # joins on the EAST bank
    "b2:0": (S, "RIGHT", ""),     # a creek of b
    "c:0": (S, "NONE", ""),       # FWA says nothing
    "stem:900": (S, "", ""),      # a stretch piece with no drawn answer
    "d:0": (S, "RIGHT", ""),      # joins there
}
EDGES = [("a:0", "stem:0"), ("a2:0", "a:0"), ("a3:0", "lk"), ("lk", "a:0"), ("b:0", "stem:0"), ("b2:0", "b:0"),
         ("c:0", "stem:0"), ("d:0", "stem:900")]


def test_only_the_systems_joining_on_the_bank_stay():
    g = _graph(NODES, EDGES)
    bound = set(NODES) - {"lk"}
    keep, why = one_bank_tributaries(bound, {"stem:0", "stem:900"}, g, "west")
    # a and everything draining into it (its own creek, the creek above the pond): the west bank
    assert {"a:0", "a2:0", "a3:0"} <= keep
    # b and its creek join on the east bank: out of the place, said so
    assert why[TRIB_OTHER_BANK] == ["b2:0", "b:0"] and not {"b:0", "b2:0"} & keep
    # no side read (FWA's NONE; a stretch piece with no drawn answer): kept as before the rule, and reported
    assert why[TRIB_SIDE_UNDETERMINED] == ["c:0", "d:0"] and {"c:0", "d:0"} <= keep
    assert {"stem:0", "stem:900"} <= keep


def test_the_other_bank_word_flips_every_answer():
    g = _graph(NODES, EDGES)
    keep, why = one_bank_tributaries(set(NODES) - {"lk"}, {"stem:0", "stem:900"}, g, "east")
    assert why[TRIB_OTHER_BANK] == ["a2:0", "a3:0", "a:0"]
    assert {"b:0", "b2:0"} <= keep


def test_a_system_reaching_the_stretch_on_both_banks_is_not_guessed():
    # a braid: e flows into a (west) AND into b (east)
    g = _graph({**NODES, "e:0": (S, "RIGHT", "")}, EDGES + [("e:0", "a:0"), ("e:0", "b:0")])
    keep, why = one_bank_tributaries({"stem:0", "a:0", "b:0", "e:0"}, {"stem:0"}, g, "west")
    assert "e:0" in why[TRIB_SIDE_UNDETERMINED] and "e:0" in keep


def test_a_place_with_no_tributaries_is_untouched():
    g = _graph(NODES, EDGES)
    assert one_bank_tributaries({"stem:0"}, {"stem:0"}, g, "west") == ({"stem:0"}, {})


def test_fwa_left_right_is_read_strictly():
    assert [_left_right(v) for v in ("LEFT", "right ", "NONE", "UNKNOWN", None, float("nan"), "")] == \
        ["LEFT", "RIGHT", "NONE", "UNKNOWN", "", "", ""]
    for bad in ("UPSTREAM", "L"):
        with pytest.raises(ValueError):
            _left_right(bad)
