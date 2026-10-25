"""A MERGED PLACE ANSWERS TO EVERY ID (T179 follow-up, owner 2026-10-24).

When points merge into one place — authored + minted, two authored under 1 m, a gauge/area/border/length
join, a cut aliased onto a lake's edge — the place keeps ALL their ids and names, and a rule extent naming
ANY of them binds that place. Proved end to end on a synthetic line: sectionizer -> registry -> reach
(`extent.resolve_extent`), each alias id against the canonical id, for every operator. Mutation-checked:
dropping the members from `_place_boundary`'s aliases, the registry boundary's `aliases`, or the alias
half of `RegistryBoundary.answers_to` fails this file.
"""

from __future__ import annotations

import pytest

from pipeline.atlas.reach import extent as X
from pipeline.atlas.registry.build import build_registry
from pipeline.atlas.splits.sectionizer import split_graph_at
from pipeline.common.models import (AnchorType, BoundaryKind, FlowEdge, NodeKind, SectionBoundary,
                                    SplitPoint, StreamGraph, StreamNode)


@pytest.fixture(autouse=True)
def _no_pfma_layer_needed(monkeypatch):
    from pipeline.atlas.splits import pfma_drainage
    monkeypatch.setattr(pfma_drainage, "basin_areas", lambda *a, **kw: {})


def _node(d, u, lb=None, ub=None):
    return StreamNode(node_id=f"1:{int(d)}", kind=NodeKind.stream, blk="1", wsc="100", gnis_id="7",
                      display_name="Test River", down_m=float(d), up_m=float(u), length_m=float(u - d),
                      lower_bound=lb, upper_bound=ub)


def _p(sid, m, source="curated", label=None, anchor=AnchorType.point):
    return SplitPoint(split_id=sid, blk="1", route_measure=float(m), fid="", label=label or sid,
                      anchor_type=anchor, source=source)


def _built():
    """Test River, mouth at 0, through Test Lake (3000-4000). At 1000 a bridge, the same bridge written
    again 0.4 m off, a gauge 150 m up and a region line; at 6000 falls with a length cut; a dam that
    projects 10 m into the lake run."""
    lo = SectionBoundary("lake:55", BoundaryKind.lake, 3000.0, "Test Lake")
    hi = SectionBoundary("lake:55", BoundaryKind.lake, 4000.0, "Test Lake")
    g = StreamGraph()
    for n in (_node(0, 3000, None, lo), _node(4000, 8000, hi, None)):
        g.nodes[n.node_id] = n
    g.edges = [FlowEdge(from_node="1:4000", to_node="1:0", kind="continuation", at_measure=3000.0)]
    split_graph_at(g, {}, [
        _p("bridge", 1000, label="Old Bridge"),
        _p("bridge_again", 1000.4, label="Old Bridge (dup)"),
        _p("gauge__08XX001", 1150, source="gauge", label="Test R near town"),
        _p("area:9", 1000, source="area", label="Region 9", anchor=AnchorType.area_boundary),
        _p("falls", 6000, label="Test Falls"),
        _p("length:1:6000", 6000, source="length", label="10 km"),
        _p("dam", 3010, label="Test Dam"),
    ], aliased=[])
    g.up_adj, g.down_adj = {}, {}
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g, build_registry(g, pinned={})


#: canonical id -> every other id merged into its place
PLACES = {
    "bridge": ("bridge_again", "gauge__08XX001", "area:9"),
    "falls": ("length:1:6000",),
    "test_river__test_lake": ("dam",),
}


def _bind(reg, g, ex):
    reasons: list = []
    got = X.resolve_extent(reg, g, ["gnis:7"], ex, reasons=reasons)
    return (sorted(got["sections"]) if got else None), reasons


def test_every_id_at_a_merged_place_is_on_its_registry_boundary():
    _g, reg = _built()
    bounds = {b.id: b for b in reg["gnis:7"].boundaries}
    for canon, others in PLACES.items():
        assert set(others) <= set(bounds[canon].answers_to()), canon
    assert bounds["bridge"].names == ("Old Bridge (dup)", "Region 9", "Test R near town")
    assert bounds["test_river__test_lake"].names == ("Test Dam",)


@pytest.mark.parametrize("op", ["upstream_of", "downstream_of"])
def test_a_rule_naming_an_alias_binds_as_the_canonical_does(op):
    g, reg = _built()
    for canon, others in PLACES.items():
        want, why = _bind(reg, g, {"op": op, "splits": [canon]})
        assert want, (canon, why)
        for sid in others:
            assert _bind(reg, g, {"op": op, "splits": [sid]})[0] == want, (op, canon, sid)


def test_between_two_merged_places_binds_by_any_pair_of_their_ids():
    g, reg = _built()
    want, why = _bind(reg, g, {"op": "between", "splits": ["bridge", "falls"]})
    assert want == ["1:1000", "1:4000"], why
    for a in ("bridge", *PLACES["bridge"]):
        for b in ("falls", *PLACES["falls"]):
            assert _bind(reg, g, {"op": "between", "splits": [a, b]})[0] == want, (a, b)
