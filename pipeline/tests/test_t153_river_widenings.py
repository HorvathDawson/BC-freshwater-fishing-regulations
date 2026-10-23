"""T153 (owner Q124, open — its stated default built): an UNNAMED lake polygon the river's main flow runs
through is the river for a rule whose extent covers BOTH sides of it; a NAMED lake never takes the river's
row. One function decides it, `extent.river_widenings`; every measured extent calls it.

MUTATION PINS — each mutation of `river_widenings` and the test it fails:
  * `UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER = False`  -> test_the_whole_river_takes_the_unnamed_pond_on_its_line,
                                                           test_a_reach_below_the_cut_takes_the_pond_inside_it,
                                                           test_the_rule_reports_every_polygon_it_took
  * drop the GNIS-name check (`n.gnis_id or n.display_name`) -> test_a_named_lake_never_takes_the_rivers_row
  * drop the registry-water check (`held`)                -> test_a_lake_a_registry_water_holds_is_its_own_water
  * drop `is_main_flow` on either piece (each alone)    -> test_a_pond_on_a_side_channel_is_not_the_river
  * drop `s.blk == p.blk`                                -> test_a_pond_fed_from_another_line_is_not_the_river
  * drop `src in selected` (one side is enough)          -> test_a_pond_holding_the_cut_is_on_neither_side
  * drop `s.down_m >= p.up_m - 1.0` (enter from above)   -> test_a_pond_the_line_enters_from_below_is_not_taken
  * drop `f.kind == "lake_in"` (any edge in counts)      -> test_only_the_line_entering_the_pond_counts
  * stop chaining (`stack.append`)                       -> test_ponds_back_to_back_are_taken_together
  * chain through a NAMED lake                           -> test_a_named_lake_in_the_chain_breaks_it
  * widen a `within` past its area (`inside`)            -> test_a_within_of_the_river_takes_only_ponds_inside_the_area

No data: each graph is a few nodes, so this runs in the no-data CI tier.
"""

from __future__ import annotations

import pathlib

import pytest

from pipeline.atlas.reach import extent as X
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

S, L = NodeKind.stream, NodeKind.lake
MAIN = ("1000",)
CUT = SectionBoundary(boundary_id="split:cut", kind=BoundaryKind("split"), route_measure=2000)
AT_POND = SectionBoundary(boundary_id="split:pond", kind=BoundaryKind("split"), route_measure=1200)


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


def _s(nid, blk, lo, hi, et=MAIN, **kw):
    return StreamNode(node_id=nid, kind=S, blk=blk, wsc="100", down_m=lo, up_m=hi,
                      length_m=hi - lo, edge_types=et, display_name="River", **kw)


def _lake(wbk, **kw):
    return StreamNode(node_id=f"lake:{wbk}", kind=L, wbk=wbk, wsc="100", **kw)


def _river(*, pond=None, et=MAIN, below_pond_bound=None):
    """Line M of the River: M:0 (0-1000), a pond (1000-1200), M:1200 (1200-2000), the cut at 2000,
    M:2000 (2000-3000)."""
    pond = pond or _lake("u")
    return _graph(
        [_s("M:0", "M", 0, 1000, et), pond,
         _s("M:1200", "M", 1200, 2000, et, upper_bound=CUT, lower_bound=below_pond_bound),
         _s("M:2000", "M", 2000, 3000, et, lower_bound=CUT)],
        [("M:2000", "M:1200", "continuation", 2000), ("M:1200", pond.node_id, "lake_in", 1200),
         (pond.node_id, "M:0", "lake_out", 1000)])


RIVER = ("M:0", "M:1200", "M:2000")


def _reg(secs=RIVER, **more):
    reg = {"gnis:1": RegistryItem(
        id="gnis:1", name="River", kind="stream", section_ids=tuple(secs),
        boundaries=(RegistryBoundary(id="cut", label="cut", kind="split", ref="split:cut"),
                    RegistryBoundary(id="pond", label="pond", kind="split", ref="split:pond")))}
    reg.update(more)
    return reg


def _sections(g, ex, reg=None):
    got = X.resolve_extent(reg or _reg(), g, ["gnis:1"], ex)
    return got["sections"]


WHOLE = {"op": "whole"}


def test_the_whole_river_takes_the_unnamed_pond_on_its_line():
    got = X.resolve_extent(_reg(), _river(), ["gnis:1"], WHOLE)
    assert got["sections"] == ["M:0", "M:1200", "M:2000", "lake:u"]
    assert got["widened"] == ["lake:u"]
    assert got["waters"] == ["River"]          # the reach still lands on the River alone


def test_a_reach_below_the_cut_takes_the_pond_inside_it():
    assert _sections(_river(), {"op": "downstream_of", "splits": ["cut"]}) == \
        ["M:0", "M:1200", "lake:u"]


def test_a_reach_above_the_cut_does_not_reach_the_pond():
    assert _sections(_river(), {"op": "upstream_of", "splits": ["cut"]}) == ["M:2000"]


def test_a_pond_holding_the_cut_is_on_neither_side():
    g = _river(below_pond_bound=AT_POND)
    assert _sections(g, {"op": "upstream_of", "splits": ["pond"]}) == ["M:1200", "M:2000"]
    assert _sections(g, {"op": "downstream_of", "splits": ["pond"]}) == ["M:0"]


def test_a_named_lake_never_takes_the_rivers_row():
    for named in (_lake("u", display_name="Pond Lake", gnis_id="9"), _lake("u", gnis_id="9"),
                  _lake("u", display_name="Pond Lake")):
        assert _sections(_river(pond=named), WHOLE) == list(RIVER)


def test_a_lake_a_registry_water_holds_is_its_own_water():
    reg = _reg(**{"wbk:u": RegistryItem(id="wbk:u", name="Pond Lake", kind="lake",
                                        section_ids=("lake:u",))})
    assert _sections(_river(), WHOLE, reg) == list(RIVER)


def _mixed(below, above):
    g = _river()
    g.nodes["M:0"] = _s("M:0", "M", 0, 1000, below)
    g.nodes["M:1200"] = _s("M:1200", "M", 1200, 2000, above, upper_bound=CUT)
    return g


def test_a_pond_on_a_side_channel_is_not_the_river():
    assert _sections(_river(et=("1350",)), WHOLE) == list(RIVER)
    assert _sections(_mixed(("1350",), MAIN), WHOLE) == list(RIVER)      # leaves into a side channel
    assert _sections(_mixed(MAIN, ("1100",)), WHOLE) == list(RIVER)      # fed by a side channel


def test_a_pond_fed_from_another_line_is_not_the_river():
    """A pond at a creek's mouth on the river: the river's piece below it is selected, and so is the
    creek's piece (another blue line of the same water) — but the river does not run through it."""
    g = _graph([_s("M:0", "M", 0, 1000), _lake("t"), _s("T:1500", "T", 1500, 2000)],
               [("T:1500", "lake:t", "lake_in", 1500), ("lake:t", "M:0", "lake_out", 600)])
    assert _sections(g, WHOLE, _reg(("M:0", "T:1500"))) == ["M:0", "T:1500"]


def test_a_pond_the_line_enters_from_below_is_not_taken():
    """Measures run from the mouth: the piece feeding the pond must lie further UP the line than the
    piece it drains into, or the line is not running through it."""
    g = _graph([_s("M:0", "M", 0, 1000), _lake("u"), _s("M:1200", "M", 1200, 2000)],
               [("M:0", "lake:u", "lake_in", 1000), ("lake:u", "M:1200", "lake_out", 1200)])
    assert _sections(g, WHOLE, _reg(("M:0", "M:1200"))) == ["M:0", "M:1200"]


def test_only_the_line_entering_the_pond_counts():
    """The upper side must ENTER the pond (`lake_in`); a piece joined to it by any other edge is not
    the river running through it."""
    g = _river()
    i = next(i for i, e in enumerate(g.edges) if e.kind == "lake_in")
    g.edges[i] = FlowEdge(from_node="M:1200", to_node="lake:u", kind="confluence", at_measure=1200)
    assert _sections(g, WHOLE) == list(RIVER)


def _chain(middle):
    return _graph([_s("M:0", "M", 0, 1000), _lake("a"), middle, _s("M:1500", "M", 1500, 2000)],
                  [("M:1500", middle.node_id, "lake_in", 1500),
                   (middle.node_id, "lake:a", "lake_out", 0),
                   ("lake:a", "M:0", "lake_out", 1000)])


def test_ponds_back_to_back_are_taken_together():
    assert _sections(_chain(_lake("b")), WHOLE, _reg(("M:0", "M:1500"))) == \
        ["M:0", "M:1500", "lake:a", "lake:b"]


def test_a_named_lake_in_the_chain_breaks_it():
    assert _sections(_chain(_lake("b", display_name="Named Lake", gnis_id="7")), WHOLE,
                     _reg(("M:0", "M:1500"))) == ["M:0", "M:1500"]


def test_a_within_of_the_river_takes_only_ponds_inside_the_area():
    park = RegistryItem(id="area:park", name="Park", kind="area", section_ids=RIVER + ("lake:u",))
    assert _sections(_river(), {"op": "within", "area_id": "area:park"},
                     _reg(**{"area:park": park})) == ["M:0", "M:1200", "M:2000", "lake:u"]
    outside = RegistryItem(id="area:park", name="Park", kind="area", section_ids=RIVER)
    assert _sections(_river(), {"op": "within", "area_id": "area:park"},
                     _reg(**{"area:park": outside})) == list(RIVER)


def test_the_policy_flag_is_the_switch(monkeypatch):
    monkeypatch.setattr(X, "UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER", False)
    got = X.resolve_extent(_reg(), _river(), ["gnis:1"], WHOLE)
    assert got["sections"] == list(RIVER) and "widened" not in got


def test_the_rule_reports_every_polygon_it_took():
    """`build_reach` says which polygons a rule's extents took (`river_widening`), never silently."""
    from pipeline.atlas.reach.build import build_reach
    entry = {"entry_id": "river", "matched": ["gnis:1"], "rules": []}
    rule = {"rule_id": "r1", "extents": [WHOLE]}
    binding, diags = build_reach(entry, rule, _reg(), _river(), covered=["gnis:1"],
                                 outside=frozenset(), shared={}, regional=False)
    assert sorted(binding.sections) == ["M:0", "M:1200", "M:2000", "lake:u"]
    assert [(d.kind, d.payload) for d in diags if d.kind == "river_widening"] == \
        [("river_widening", {"polygons": ["lake:u"]})]


def test_one_function_decides_it():
    """No second copy: the policy is read by `river_widenings` alone, and nothing else in the
    pipeline re-derives an unnamed polygon's river from the graph."""
    root = pathlib.Path(__file__).resolve().parents[1]
    readers = sorted(str(p.relative_to(root)) for p in root.rglob("*.py")
                     if "tests" not in p.parts
                     and "UNNAMED_LAKES_ON_THE_RIVER_ARE_THE_RIVER" in p.read_text(encoding="utf-8"))
    assert readers == ["atlas/reach/cache.py", "atlas/reach/extent.py"]
    callers = sorted(str(p.relative_to(root)) for p in root.rglob("*.py")
                     if "tests" not in p.parts and "river_widenings(" in p.read_text(encoding="utf-8"))
    assert callers == ["atlas/reach/extent.py"]


@pytest.mark.parametrize("ex", [{"op": "downstream_of", "splits": ["cut"]}, WHOLE])
def test_the_walk_seed_carries_the_pond(ex):
    """A walking rule seeds from what its extent took, the pond included — the pond's inflows were
    already the river's tributaries (`tributaries._lake_mid_reach`); the seed only stops the walk
    re-entering it as "the river passing through"."""
    park = RegistryItem(id="area:park", name="Park", kind="area", section_ids=RIVER + ("lake:u",))
    got = X.resolve_extent(_reg(**{"area:park": park}), _river(), ["gnis:1"],
                           {**ex, "within_area": "area:park"})
    assert "lake:u" in got["seed"]
