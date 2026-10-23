"""T111 — EVERY CUT REACHES THE WALK: one answer to "where does this rule's reach cut the water, and
which cuts bound its tributary walk?" (owner 2026-10-22, unification audit finding reach 5).

Before: `build_reach` kept `window = window or got["window"]` — only the FIRST extent's window reached
the tributary seeding, so the creeks at a second reach's lower cut were never the rule's
(`alouette_river.r2`, `bull_river.r1`, `atnarko_bella_coola_rivers.r2`); and `build._cut_places`
re-derived every cut from node bounds for the confluence test, ignoring `cut_pick` and the
ambiguous-cut ruling (it tested EVERY candidate of an ambiguous cut, the one the reach did not use
included).

Now `extent.resolve_extent` returns the `cuts` it actually used (split, line, measure, which END of
the reach, boundary kind and label). The seeding takes every extent's LOWER cuts
(`build.seeding_cuts` -> `tributaries.expand(cuts=)`), and `build.confluence_excludes` reads the same
cuts — it has no geometry of its own.

Mutation pins:
  * `tributaries.EVERY_CUT_REACHES_THE_WALK = False` (first cut only) fails
    `test_both_lower_cuts_of_a_two_reach_rule_seed`.
  * restoring a node-bound re-derivation in `confluence_excludes` (every candidate of the cut) fails
    `test_an_ambiguous_cut_is_tested_only_at_the_candidate_the_reach_used` and
    `test_confluence_excludes_reads_the_cuts_it_is_handed`.
"""

from __future__ import annotations

import inspect

import pytest

from pipeline.atlas.graph import tributaries as T
from pipeline.atlas.reach import build as B
from pipeline.atlas.reach import extent as X
from pipeline.common.models import FlowEdge, NodeKind, StreamGraph, StreamNode
from pipeline.common.models.enums import BoundaryKind
from pipeline.common.models.registry import RegistryBoundary, RegistryItem
from pipeline.common.models.sections import SectionBoundary

INF = float("inf")


def _b(sid, m, kind="split"):
    return SectionBoundary(boundary_id=f"split:{sid}", kind=BoundaryKind(kind), route_measure=m)


def _s(nid, blk, lo, hi, *, wsc, order, name="", lower=None, upper=None):
    return StreamNode(node_id=nid, kind=NodeKind.stream, blk=blk, wsc=wsc, display_name=name,
                      down_m=lo, up_m=hi, length_m=hi - lo, stream_order=order,
                      lower_bound=lower, upper_bound=upper)


def _graph(nodes, edges):
    g = StreamGraph()
    g.nodes = {n.node_id: n for n in nodes}
    g.edges = [FlowEdge(from_node=a, to_node=b, kind=k, at_measure=m) for a, b, k, m in edges]
    for i, e in enumerate(g.edges):
        g.up_adj.setdefault(e.to_node, []).append(i)
        g.down_adj.setdefault(e.from_node, []).append(i)
    return g


# ------------------------------------------------------------------ two reaches, two lower cuts

def _two_reaches():
    """Main river M (mouth at 0), cut at 1000 (`a`), 2000 (`b`), 5000 (`c`) and 6000 (`d`). The rule
    is "between a and b, and between c and d". Creek A's mouth is AT a (1000) and creek C's AT c
    (5000), each hung by FWA on the piece BELOW the cut — outside the reach, so only the seeding
    finds them.

        M:6000 ── d ── M:5000 ── c ── M:2000 ── b ── M:1000 ── a ── M:0
                                 ^ C (mouth @5000, on M:2000)   ^ A (mouth @1000, on M:0)
    """
    w = "100-1"
    nodes = [
        _s("M:0", "M", 0, 1000, wsc=w, order=5, upper=_b("a", 1000)),
        _s("M:1000", "M", 1000, 2000, wsc=w, order=5, lower=_b("a", 1000), upper=_b("b", 2000)),
        _s("M:2000", "M", 2000, 5000, wsc=w, order=5, lower=_b("b", 2000), upper=_b("c", 5000)),
        _s("M:5000", "M", 5000, 6000, wsc=w, order=5, lower=_b("c", 5000), upper=_b("d", 6000)),
        _s("M:6000", "M", 6000, 9000, wsc=w, order=5, lower=_b("d", 6000)),
        _s("A:0", "A", 0, 500, wsc=w + "-001000", order=2, name="A Creek"),
        _s("C:0", "C", 0, 500, wsc=w + "-005000", order=2, name="C Creek"),
    ]
    edges = [("M:6000", "M:5000", "continuation", 6000), ("M:5000", "M:2000", "continuation", 5000),
             ("M:2000", "M:1000", "continuation", 2000), ("M:1000", "M:0", "continuation", 1000),
             ("A:0", "M:0", "confluence", 1000), ("C:0", "M:2000", "confluence", 5000)]
    reg = {"gnis:1": RegistryItem(
        id="gnis:1", name="Main River", kind="stream", section_ids=tuple(n.node_id for n in nodes[:5]),
        boundaries=tuple(RegistryBoundary(id=c, label=c, kind="split", ref=f"split:{c}")
                         for c in "abcd"))}
    return _graph(nodes, edges), reg


ENTRY = {"entry_id": "r9:main@9-1", "matched": ["gnis:1"], "includes_tributaries": True}
TWO = [{"op": "between", "splits": ["a", "b"]}, {"op": "between", "splits": ["c", "d"]}]


def _bind(g, reg, extents, *, entry=ENTRY, **rule):
    b, _ = B.build_reach(entry, {"rule_id": "main.r1", "verbatim": "No Fishing",
                                 "extents": extents, **rule}, reg, g, owned={})
    return set(b.sections)


def test_resolve_extent_returns_the_cuts_it_used():
    g, reg = _two_reaches()
    got = X.resolve_extent(reg, g, ["gnis:1"], TWO[1])
    assert got["cuts"] == [
        {"split": "c", "blk": "M", "m": 5000, "end": "lower", "kind": "split", "label": "c"},
        {"split": "d", "blk": "M", "m": 6000, "end": "upper", "kind": "split", "label": "d"}]
    got = X.resolve_extent(reg, g, ["gnis:1"], {"op": "upstream_of", "splits": ["c"]})
    assert [(c["split"], c["m"], c["end"]) for c in got["cuts"]] == [("c", 5000, "lower")]
    got = X.resolve_extent(reg, g, ["gnis:1"], {"op": "downstream_of", "splits": ["c"]})
    assert [(c["split"], c["m"], c["end"]) for c in got["cuts"]] == [("c", 5000, "upper")]
    assert X.resolve_extent(reg, g, ["gnis:1"], {"op": "whole"})["cuts"] == []


def test_both_lower_cuts_of_a_two_reach_rule_seed(monkeypatch):
    """`alouette_river.r2` / `bull_river.r1`: the second reach's lower cut seeds too. MUTATION
    (`EVERY_CUT_REACHES_THE_WALK` off, the first cut only): C Creek is lost."""
    g, reg = _two_reaches()
    assert _bind(g, reg, TWO) == {"M:1000", "M:5000", "A:0", "C:0"}
    assert B.seeding_cuts(g, [X.resolve_extent(reg, g, ["gnis:1"], ex) for ex in TWO]) == \
        [("M", 1000), ("M", 5000)]
    monkeypatch.setattr(T, "EVERY_CUT_REACHES_THE_WALK", False)
    assert _bind(g, reg, TWO) == {"M:1000", "M:5000", "A:0"}


def test_the_order_of_the_extents_does_not_matter():
    """Before, the FIRST extent's window won: the same rule written the other way round seeded the
    other creek."""
    g, reg = _two_reaches()
    assert _bind(g, reg, TWO[::-1]) == _bind(g, reg, TWO) == {"M:1000", "M:5000", "A:0", "C:0"}


def test_an_extent_with_no_cut_offers_its_own_lower_bounds():
    """A `whole` on an item that is part of the line (an item boundary at its foot) seeds there, as a
    rule of `whole` extents alone always did (`tributaries.lower_bound_cuts`) — now also beside a
    cut extent."""
    g, reg = _two_reaches()
    reg = {**reg, "gnis:up": RegistryItem(id="gnis:up", name="Upper Main", kind="stream",
                                          section_ids=("M:5000", "M:6000"))}
    got = _bind(g, reg, [TWO[0], {"op": "whole", "item_id": "gnis:up"}])
    assert got == {"M:1000", "M:5000", "M:6000", "A:0", "C:0"}


def test_a_reach_running_down_to_the_mouth_has_no_lower_cut():
    g, reg = _two_reaches()
    got = X.resolve_extent(reg, g, ["gnis:1"], {"op": "downstream_of", "splits": ["a"]})
    assert B.seeding_cuts(g, [got]) == []
    # A Creek hangs on M:0 itself, so the walk (not the seeding) finds it.
    assert _bind(g, reg, [{"op": "downstream_of", "splits": ["a"]}]) == {"M:0", "A:0"}


# ------------------------------------------------------------- an ambiguous cut, picked once

def _ambiguous():
    """Main river M with ONE confluence split `x` landing at TWO measures (1000 and 3000) — the
    curated cut's id carried by two node bounds. X Creek joins AT 1000 (on M:0, below), Y Creek AT
    3000 (on M:1000, inside a reach running up from 1000). Y Creek has a row of its own."""
    w = "100-1"
    nodes = [
        _s("M:0", "M", 0, 1000, wsc=w, order=5, upper=_b("x", 1000, "confluence")),
        _s("M:1000", "M", 1000, 3000, wsc=w, order=5, lower=_b("x", 1000, "confluence"),
           upper=_b("x", 3000, "confluence")),
        _s("M:3000", "M", 3000, 6000, wsc=w, order=5, lower=_b("x", 3000, "confluence")),
        _s("X:0", "X", 0, 500, wsc=w + "-001000", order=2, name="X Creek"),
        _s("Y:0", "Y", 0, 500, wsc=w + "-003000", order=2, name="Y Creek"),
        _s("Y:500", "Y", 500, 900, wsc=w + "-003000", order=2, name="Y Creek"),
    ]
    edges = [("M:3000", "M:1000", "continuation", 3000), ("M:1000", "M:0", "continuation", 1000),
             ("X:0", "M:0", "confluence", 1000), ("Y:0", "M:1000", "confluence", 3000),
             ("Y:500", "Y:0", "continuation", 500)]
    reg = {"gnis:1": RegistryItem(id="gnis:1", name="Main River", kind="stream",
                                  section_ids=("M:0", "M:1000", "M:3000"),
                                  boundaries=(RegistryBoundary(id="x", label="X Creek → Main River",
                                                               kind="confluence", ref="split:x"),)),
           "gnis:y": RegistryItem(id="gnis:y", name="Y Creek", kind="stream",
                                  section_ids=("Y:0", "Y:500"))}
    return _graph(nodes, edges), reg


OWNED = {"gnis:y": ("r9:y_creek@9-1",)}
PICK_LOWER = {"op": "upstream_of", "splits": ["x"], "cut_pick": {"x": "lower"}}


def test_an_ambiguous_cut_is_tested_only_at_the_candidate_the_reach_used():
    """`cut_pick: lower` starts the reach at 1000. Y Creek joins at 3000 — INSIDE the reach, an
    ordinary tributary, walked. The old `_cut_places` tested every candidate of the cut, found Y
    Creek "at the confluence" at 3000 and, Y having a row of its own, kept it and its subtree out
    of a reach it flows into mid-way."""
    g, reg = _ambiguous()
    b, d = B.build_reach(ENTRY, {"rule_id": "main.r1", "verbatim": "No Fishing",
                                 "extents": [PICK_LOWER]}, reg, g, owned=OWNED)
    assert set(b.sections) == {"M:1000", "M:3000", "X:0", "Y:0", "Y:500"}
    conf = [x.payload for x in d if x.kind == "confluence_cut"]
    assert [(p["water"], p["with_cut"], p["kept_out"]) for p in conf] == [("X Creek", True, 0)]
    rows = B.confluence_excludes(ENTRY, {"rule_id": "r", "verbatim": "", "extents": [PICK_LOWER]},
                                 reg, g, ["gnis:1"], owned=OWNED)
    assert [(r["split"], r["water"], r["m"]) for r in rows] == [("x", "X Creek", 1000)]


def test_the_general_rule_s_candidate_is_the_one_tested_too():
    """No pick: `upstream_of` takes the candidate nearest the source (3000). Y Creek is AT that cut
    and has its own row: kept out. X Creek at 1000 is not this reach's cut at all."""
    g, reg = _ambiguous()
    rule = {"rule_id": "r", "verbatim": "", "extents": [{"op": "upstream_of", "splits": ["x"]}]}
    rows = B.confluence_excludes(ENTRY, rule, reg, g, ["gnis:1"], owned=OWNED)
    assert [(r["water"], r["m"], r["sections"]) for r in rows] == \
        [("Y Creek", 3000, ["Y:0", "Y:500"])]


def test_confluence_excludes_reads_the_cuts_it_is_handed():
    """The confluence test answers from `resolved[i]["cuts"]` alone: hand it a cut the graph's
    bounds do not carry at all and that is where it looks."""
    g, reg = _ambiguous()
    rule = {"rule_id": "r", "verbatim": "", "extents": [{"op": "upstream_of", "splits": ["x"]}]}
    fake = [{"sections": ["M:3000"], "cuts": [{"split": "x", "blk": "M", "m": 1000, "end": "lower",
                                                "kind": "confluence", "label": "X Creek → Main"}]}]
    rows = B.confluence_excludes(ENTRY, rule, reg, g, ["gnis:1"], owned=OWNED, resolved=fake)
    assert [(r["water"], r["m"]) for r in rows] == [("X Creek", 1000)]


def test_no_second_copy_of_the_cut_geometry():
    """`_cut_places` is gone, and `confluence_excludes` reads no node bound: the cut is
    `resolve_extent`'s."""
    assert not hasattr(B, "_cut_places")
    src = inspect.getsource(B.confluence_excludes)
    for word in ("lower_bound", "upper_bound", "route_measure", "_cut_at", "boundaries"):
        assert word not in src, word
    src = inspect.getsource(B.build_reach)
    assert "window" not in src, "the walk takes the extents' cuts, never one window"
