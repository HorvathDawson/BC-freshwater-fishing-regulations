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
