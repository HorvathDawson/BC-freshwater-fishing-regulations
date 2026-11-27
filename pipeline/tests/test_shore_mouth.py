"""A line ends in the sea when nothing leaves it AT ITS MOUTH (`shore.leaves_at_mouth`, T1199 review 2026-11-27).

Lost Shoe Creek: a channel leaves it 9 km above its mouth. While a Pacific Rim cut at 4 km split its lowest piece, that
edge sat on the piece above and the coast rule saw the line end in the sea; with the closure cuts after the coast cut the
lowest piece held the edge and the line was dropped from the coast record. Synthetic graph, no data.
"""
from __future__ import annotations

from pipeline.atlas.splits import coast as C
from pipeline.atlas.splits import shore as SH
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode


def _graph(leave_m):
    g = StreamGraph()
    g.nodes["L:0"] = StreamNode(node_id="L:0", kind=NodeKind.stream, blk="L", down_m=0.0, up_m=10014.0, length_m=10014.0)
    g.nodes["D:0"] = StreamNode(node_id="D:0", kind=NodeKind.stream, blk="D", down_m=0.0, up_m=500.0, length_m=500.0)
    g.edges.append(FlowEdge("L:0", "D:0", 0.0))
    g.down_adj["L:0"] = [0]
    g.up_adj["D:0"] = [0]
    return g, (lambda i: leave_m)


def test_a_channel_leaving_high_up_is_not_the_mouth():
    g, leaves = _graph(8998.0)
    assert SH.leaves_at_mouth(g, g.nodes["L:0"], leaves, C.STUB_M) == []


def test_an_edge_at_the_mouth_or_unplaced_is_the_mouth():
    for m in (0.0, C.STUB_M - 1.0, None):
        g, leaves = _graph(m)
        assert SH.leaves_at_mouth(g, g.nodes["L:0"], leaves, C.STUB_M) == [0], m


def test_mutation_any_edge_on_the_lowest_piece_hides_the_sea_end(monkeypatch):
    monkeypatch.setattr(SH, "A_DISTRIBUTARY_HIGHER_UP_IS_NOT_THE_MOUTH", False)
    g, leaves = _graph(8998.0)
    assert SH.leaves_at_mouth(g, g.nodes["L:0"], leaves, C.STUB_M) == [0]
