"""T141 — the CARVE-OUT WALK seeds from the extent's cuts, and a CUT ACROSS A BRAID reports its
side-channel crossings (owner Q100, 2026-10-23; T111's follow-ups).

T111 gave `extent.resolve_extent` the `cuts` it used and made the rule's walk seed from them
(`build.seeding_cuts`). Two things were left:
  * the carve-out walk (`build.resolve_carve_outs`, "and everything upstream of it") still seeded from
    its pieces' node bounds (`tributaries_of_reach(graph, secs)`, `cuts=None`);
  * a cut held its PRINCIPAL line only, so a creek hung on a side channel at the cut was invisible to
    the seeding and to the confluence test.

Now each used cut carries `side` (its crossings of the braid's other lines,
`extent.CUTS_REPORT_SIDE_CHANNEL_CROSSINGS`); `build.cut_places` is the whole cut; the rule's walk,
the carve-out walk (`build.carve_out_cuts`) and the licensing carve-out check all seed through
`seeding_cuts`; `confluence_excludes` tests every place of a cut (a side channel by name only).

Mutation pins:
  * `extent.CUTS_REPORT_SIDE_CHANNEL_CROSSINGS = False` fails `test_a_braid_cut_reports_its_side_crossing`,
    `test_a_creek_at_the_side_crossing_is_the_rule_s`, `test_a_carve_out_blocks_the_creek_at_its_side_crossing`
    and `test_a_confluence_joiner_on_a_side_channel_is_found`.
  * `build.CARVE_OUT_WALK_SEEDS_FROM_ITS_CUTS = False` (node bounds) fails
    `test_a_carve_out_does_not_seed_at_a_bound_that_is_not_its_cut`.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import build as B
from pipeline.atlas.reach import extent as X
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

W = "100-1"


def _b(sid, m):
    return SectionBoundary(boundary_id=f"split:{sid}", kind=BoundaryKind("split"), route_measure=m)


def _s(nid, blk, lo, hi, *, wsc=W, order=5, name="", lower=None, upper=None):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc=wsc, display_name=name,
                      down_m=lo, up_m=hi, length_m=hi - lo, stream_order=order,
                      lower_bound=lower, upper_bound=upper)


def _world(side_bound="a", *, kind="split", label="a"):
    """Main river M cut at 1000 by split `a`; side channel S leaves M:1000 and runs down past the cut,
    which the sweep cut on S too, at S's 500 (`side_bound="a"`) — or S's piece there is bounded by
    some other split `z` (`side_bound="z"`). Only S:500 is the river's item (S:0 below is unnamed
    water). X Creek joins S at 500, hung by FWA on S:0 — outside the river, so only a seed at S's
    crossing reaches it.

        M:1000 ─────────── a ─── M:0
          └─ S:500 ── a/z ── S:0 ──┘
                            ^ X Creek (mouth @ S 500, on S:0)
    """
    nodes = [
        _s("M:0", "M", 0, 1000, upper=_b("a", 1000)),
        _s("M:1000", "M", 1000, 3000, lower=_b("a", 1000)),
        _s("S:0", "S", 0, 500, order=1, upper=_b(side_bound, 500)),
        _s("S:500", "S", 500, 1500, order=1, lower=_b(side_bound, 500)),
        _s("X:0", "X", 0, 400, wsc=W + "-000300", order=2, name="X Creek"),
    ]
    edges = [("M:1000", "M:0", "continuation", 1000), ("M:1000", "S:500", "continuation", 1500),
             ("S:500", "S:0", "continuation", 500), ("S:0", "M:0", "confluence", 300),
             ("X:0", "S:0", "confluence", 500)]
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    reg = {"gnis:1": RegistryItem(
        id="gnis:1", name="Main River", kind="stream", section_ids=("M:0", "M:1000", "S:500"),
        boundaries=(RegistryBoundary(id="a", label=label, kind=kind, ref="split:a"),))}
    return g, reg


UP = {"op": "upstream_of", "splits": ["a"]}
ENTRY = {"entry_id": "r9:main@9-1", "matched": ["gnis:1"], "includes_tributaries": True}


def test_a_braid_cut_reports_its_side_crossing():
    g, reg = _world()
    got = X.resolve_extent(reg, g, ["gnis:1"], UP)
    assert got["sections"] == ["M:1000", "S:500"]
    assert got["cuts"] == [{"split": "a", "blk": "M", "m": 1000, "end": "lower", "kind": "split",
                            "label": "a", "side": [["S", 500]]}]
    assert B.cut_places(got["cuts"][0]) == [("M", 1000), ("S", 500)]
    assert B.seeding_cuts(g, [got]) == [("M", 1000), ("S", 500)]


def test_side_crossings_off_report_none(monkeypatch):
    monkeypatch.setattr(X, "CUTS_REPORT_SIDE_CHANNEL_CROSSINGS", False)
    g, reg = _world()
    got = X.resolve_extent(reg, g, ["gnis:1"], UP)
    assert got["cuts"][0]["side"] == [] and B.seeding_cuts(g, [got]) == [("M", 1000)]


def test_a_creek_at_the_side_crossing_is_the_rule_s():
    """The rule's own walk: X Creek joins at the cut (on its side channel), so it is the rule's
    tributary — as a creek at the principal line's cut always was."""
    g, reg = _world()
    b, _ = B.build_reach(ENTRY, {"rule_id": "main.r1", "verbatim": "No Fishing", "extents": [UP]},
                         reg, g, owned={})
    assert set(b.sections) == {"M:1000", "S:500", "X:0"}
    assert set(b.via_tributary) == {"X:0"}


def _carve(g, reg, ex):
    rule = {"rule_id": "main.r1", "tributary_excludes": [{**ex, "item_id": "gnis:1"}]}
    return B.resolve_carve_outs(ENTRY, rule, reg, g, ["gnis:1"])


def test_a_carve_out_blocks_the_creek_at_its_side_crossing():
    g, reg = _world()
    detail, blocked = _carve(g, reg, UP)
    assert detail == [{"extent": {**UP, "item_id": "gnis:1"}, "resolved": True,
                       "sections": ["M:1000", "S:500"], "cuts": [["M", 1000], ["S", 500]],
                       "above": 1}]
    assert blocked == {"M:1000", "S:500", "X:0"}


def test_a_carve_out_does_not_seed_at_a_bound_that_is_not_its_cut(monkeypatch):
    """S's piece is bounded at 500 by ANOTHER split (`z`), not the carve-out's cut: the carve-out's
    walk is seeded at its cut only, exactly as the rule's walk is. MUTATION (node bounds, the old
    `tributaries_of_reach(graph, secs)`): X Creek is blocked again through the `z` bound."""
    g, reg = _world(side_bound="z")
    detail, blocked = _carve(g, reg, UP)
    assert detail[0]["cuts"] == [["M", 1000]] and blocked == {"M:1000", "S:500"}
    b, _ = B.build_reach(ENTRY, {"rule_id": "main.r1", "verbatim": "No Fishing", "extents": [UP]},
                         reg, g, owned={})
    assert set(b.sections) == {"M:1000", "S:500"}, "the rule's walk and its carve-out agree"
    monkeypatch.setattr(B, "CARVE_OUT_WALK_SEEDS_FROM_ITS_CUTS", False)
    detail, blocked = _carve(g, reg, UP)
    assert detail[0]["cuts"] == [["M", 1000], ["S", 500]] and blocked == {"M:1000", "S:500", "X:0"}


def test_a_confluence_joiner_on_a_side_channel_is_found():
    """A curated confluence cut whose joiner sits on the side channel the cut crosses."""
    g, reg = _world(kind="confluence", label="X Creek")
    rows = B.confluence_excludes(ENTRY, {"rule_id": "r", "verbatim": "No Fishing", "extents": [UP]},
                                 reg, g, ["gnis:1"], owned={})
    assert [(r["split"], r["water"], r["blk"], r["m"], r["mouths"]) for r in rows] == [
        ("a", "X Creek", "S", 500, ["X:0"])]


def test_a_side_channel_takes_no_unnamed_joiner():
    """The unnamed fallback of a curated confluence cut is its principal line's alone: a cut labelled
    for a water that joins nowhere finds nothing on a side channel."""
    g, reg = _world(kind="confluence", label="Y Creek")
    rows = B.confluence_excludes(ENTRY, {"rule_id": "r", "verbatim": "No Fishing", "extents": [UP]},
                                 reg, g, ["gnis:1"], owned={})
    assert rows == []


@pytest.mark.parametrize("flag", ["CUTS_REPORT_SIDE_CHANNEL_CROSSINGS"])
def test_the_policy_flags_are_on(flag):
    assert getattr(X, flag) is True and B.CARVE_OUT_WALK_SEEDS_FROM_ITS_CUTS is True


# --------------------------------------------------------------------------- adversarial review

def test_the_carve_out_walk_is_what_the_carve_out_blocks():
    """`carve_out_walk` is the one carve-out walk: what `resolve_carve_outs` blocks, row by row, and
    what the licensing carve-out check hands a designation's excluded water. A `walk_past` row takes
    its own sections only."""
    g, reg = _world()
    detail, blocked = _carve(g, reg, UP)
    assert B.carve_out_walk(g, detail[0]) == blocked == {"M:1000", "S:500", "X:0"}
    assert B.carve_out_walk(g, {**detail[0], "walk_past": True}) == {"M:1000", "S:500"}
    assert B.carve_out_walk(g, {"extent": UP, "resolved": False, "sections": [], "above": 0}) \
        == frozenset()


def _calls_in(tree, name):
    """`(enclosing function, call)` for every call of `name` (bare or attribute) in `tree`."""
    import ast
    out = []
    for fn in ast.walk(tree):
        if isinstance(fn, ast.FunctionDef):
            for node in ast.walk(fn):
                if isinstance(node, ast.Call) and (
                        getattr(node.func, "attr", None) == name or getattr(node.func, "id", None) == name):
                    out.append(fn.name)
    return out


def test_no_second_copy_of_the_carve_out_walk():
    """MUTATION (the T141 review's): `build_reaches`' licensing check calling
    `tributaries_of_reach(graph, secs)` itself again — the copy that seeded from node bounds while
    the carve-out seeded from its cuts — fails here. The reach builder walks a carve-out in one
    place, and the licensing check asks it."""
    import ast
    from pathlib import Path
    tree = ast.parse(Path(B.__file__).read_text(encoding="utf-8"))
    assert _calls_in(tree, "tributaries_of_reach") == ["carve_out_walk"]
    assert "build_reaches" in _calls_in(tree, "carve_out_walk")
    assert "resolve_carve_outs" in _calls_in(tree, "carve_out_walk")
