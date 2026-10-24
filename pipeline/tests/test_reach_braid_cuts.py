"""R14 fixture 12 — THE REACH after the braid sweep (T67): each half of a cut braid lies on its own side of
the cut (`extent.A_CUT_SEVERS_EVERY_LINE_IT_CROSSES`, mutation-pinned), an uncut straddler is reported, and
the run's gate (`build.straddle_gate`) fails on it. E15's majority side is gone (RULINGS E17)."""

from __future__ import annotations

import pytest

from pipeline.atlas.splits import braid_sweep as BS
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.tests.test_braid_sweep import _G, _apply, _cut, _one_braid, _pieces


# ---------------------------------------------------------------- 12. the reach


def _reach(g):
    from pipeline.atlas.reach import extent as X
    from pipeline.common.models.registry import RegistryBoundary, RegistryItem
    secs = sorted(g.nodes)
    reg = {"gnis:9": RegistryItem(id="gnis:9", name="X River", kind="stream", section_ids=tuple(secs),
                                  boundaries=(RegistryBoundary(id="c", label="c", kind="split",
                                                               ref="split:c"),))}
    return {op: X.resolve_extent(reg, g, ["gnis:9"], {"op": op, "splits": ["c"]})
            for op in ("upstream_of", "downstream_of")}


def test_12_the_reach_places_each_half_of_a_cut_braid_on_its_own_side():
    g, geoms = _one_braid()
    _apply(g, geoms, [_cut("c", 1000)])
    lo, hi = _pieces(g, "B")
    got = _reach(g)
    assert hi.node_id in got["upstream_of"]["sections"] and lo.node_id not in got["upstream_of"]["sections"]
    assert lo.node_id in got["downstream_of"]["sections"]
    assert not got["upstream_of"].get("unclassified") and not got["downstream_of"].get("unclassified")


def test_12_mutation_without_severing_the_cut_braid_reads_as_one_straddler(monkeypatch):
    from pipeline.atlas.reach import extent as X
    monkeypatch.setattr(X, "A_CUT_SEVERS_EVERY_LINE_IT_CROSSES", False)
    g, geoms = _one_braid()
    _apply(g, geoms, [_cut("c", 1000)])
    lo, hi = _pieces(g, "B")
    got = _reach(g)
    assert {lo.node_id, hi.node_id} <= set(got["upstream_of"].get("unclassified") or [])


def test_12_the_reach_gate_fails_on_an_uncut_straddler():
    from pipeline.atlas.reach.build import StraddleGateError, straddle_gate
    from pipeline.atlas.reach.models import Diagnostic
    ok = [Diagnostic("e", "r1", "unclassified", {"pieces": ["x:0"], "reasons": {"x:0": "detached"}})]
    straddle_gate(ok)
    bad = ok + [Diagnostic("e", "r2", "unclassified", {"pieces": ["B:0"],
                                                       "reasons": {"B:0": "straddles_the_cut"}})]
    with pytest.raises(StraddleGateError, match="B:0"):
        straddle_gate(bad)


def test_12_an_uncut_braid_is_reported_straddling_by_the_reach():
    g, geoms = _one_braid()
    split_graph_at(g, geoms, [_cut("c", 1000)])           # the host only: no sweep
    got = _reach(g)
    assert "B:0" in (got["upstream_of"].get("unclassified") or [])


# ---------------------------------------------------------------- the flow path never crosses the cut either


def _hand(nodes, edges):
    from pipeline.common.models import FlowEdge, StreamGraph
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, at_measure=m, kind=k) for a, b, m, k in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _s(nid, blk, lo, hi, lb=None, ub=None):
    from pipeline.common.models import NodeKind, StreamNode
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc="100", down_m=lo, up_m=hi,
                      length_m=hi - lo, lower_bound=lb, upper_bound=ub)


def _lake(nid):
    from pipeline.common.models import NodeKind, StreamNode
    return StreamNode(node_id=nid, kind=NodeKind.lake, wbk=nid.split(":")[1], wsc="100")


def _bnd(bid, m, kind="split"):
    from pipeline.common.models.enums import BoundaryKind
    from pipeline.common.models.sections import SectionBoundary
    return SectionBoundary(boundary_id=bid, kind=BoundaryKind(kind), route_measure=m)


def _resolve(g, secs, ref, op="upstream_of"):
    from pipeline.atlas.reach import extent as X
    from pipeline.common.models.registry import RegistryBoundary, RegistryItem
    reg = {"gnis:9": RegistryItem(id="gnis:9", name="X River", kind="stream", section_ids=tuple(secs),
                                  boundaries=(RegistryBoundary(id="c", label="c", kind="split", ref=ref),))}
    return X.resolve_extent(reg, g, ["gnis:9"], {"op": op, "splits": ["c"]})


def _chowade():
    """The Chowade below Horseshoe Road Bridge: a braid cut at the bridge's line, whose 26 m piece below the
    cut drains into an unnamed pond (in no item) and on through the braid's mouth into the river below."""
    c, bc = _bnd("split:c", 1000.0), _bnd("split:c", 300.0)
    nodes = [_s("H:0", "H", 0, 1000, ub=c), _s("H:1000", "H", 1000, 3000, lb=c),
             _s("B:0", "B", 0, 100, ub=_bnd("lake:77", 100.0, "lake")), _lake("lake:77"),
             _s("B:200", "B", 200, 300, lb=_bnd("lake:77", 200.0, "lake"), ub=bc), _s("B:300", "B", 300, 800, lb=bc)]
    edges = [("H:1000", "H:0", 1000.0, "continuation"), ("B:300", "B:200", 300.0, "continuation"),
             ("B:200", "lake:77", 0.0, "lake_in"), ("lake:77", "B:0", 100.0, "lake_out"),
             ("B:0", "H:0", 800.0, "confluence"), ("H:1000", "B:300", 800.0, "confluence")]
    return _hand(nodes, edges), ["H:0", "H:1000", "B:0", "B:200", "B:300"]


def test_a_braid_piece_below_its_cut_does_not_climb_back_over_it():
    g, secs = _chowade()
    got = _resolve(g, secs, "split:c")
    assert "B:300" in got["sections"] and "B:200" not in got["sections"]
    assert not got.get("unclassified")


def _ash():
    """Elsie Lake on the Ash: the window is above the lake (`upstream_of` its edge), and a second outlet S
    drains the lake through an unnamed pond (in no item) into the river BELOW it."""
    lo, hi = _bnd("lake:E", 500.0, "lake"), _bnd("lake:E", 900.0, "lake")
    nodes = [_s("H:0", "H", 0, 500, ub=lo), _lake("lake:E"), _s("H:900", "H", 900, 3000, lb=hi),
             _s("S:0", "S", 0, 400, lb=_bnd("lake:P", 0.0, "lake"), ub=_bnd("lake:E", 400.0, "lake")),
             _lake("lake:P")]
    edges = [("H:900", "lake:E", 0.0, "lake_in"), ("lake:E", "H:0", 500.0, "lake_out"),
             ("lake:E", "S:0", 400.0, "lake_out"), ("S:0", "lake:P", 0.0, "lake_in"),
             ("lake:P", "H:0", 300.0, "lake_out")]
    return _hand(nodes, edges), ["H:0", "H:900", "S:0"]


def test_a_second_outlet_of_the_cut_lake_is_on_its_downstream_side():
    g, secs = _ash()
    got = _resolve(g, secs, "lake:E")
    assert "H:900" in got["sections"] and "S:0" not in got["sections"]
    assert not got.get("unclassified")


@pytest.mark.parametrize("make,ref,piece", [(_chowade, "split:c", "B:200"), (_ash, "lake:E", "S:0")])
def test_mutation_a_flow_path_across_the_cut_straddles(monkeypatch, make, ref, piece):
    from pipeline.atlas.reach import extent as X
    monkeypatch.setattr(X, "A_CUT_SEVERS_EVERY_LINE_IT_CROSSES", False)
    g, secs = make()
    got = _resolve(g, secs, ref)
    # without the rule the piece lands on the wrong side of the cut, or straddles it
    assert piece in got["sections"] or piece in (got.get("unclassified") or [])
