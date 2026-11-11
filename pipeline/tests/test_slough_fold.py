"""T322: the owner-confirmed slough calls — the apply step (`tools.slough_calls`) and the registry fold
(`registry.flowing.fold_confirmed_sloughs`). Synthetic graphs; the committed file is checked against what the marks decide.

Owner 2026-10-30 (corrected): only an ISOLATED confirmed polygon (no line touches it) is skipped. A named stream
through it, or on only one side of it, takes it; with only unnamed lines, the item holding it becomes a stream on that
line (never a new item). Two different named streams are never picked."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.atlas.registry.flowing import fold_confirmed_sloughs
from pipeline.common.models import RegistryBoundary
from pipeline.tests.test_registry_flowing import L, S, W, _graph, _item
from pipeline.tools import slough_calls as SC


def _creek():
    """X Creek: x2 -> lake:5 (unnamed polygon) -> x1 -> lake:6 -> x0. lake:7 is a polygon an unnamed line (u1)
    runs through; lake:8 is where X Creek's head piece x3 ENDS (nothing leaves); Y Creek y1 feeds lake:6; lake:9 is
    isolated (no edge)."""
    g = _graph({"x2": S, "lake:5": L, "x1": S, "lake:6": L, "x0": S, "u2": S, "lake:7": L, "u1": S,
                "x3": S, "lake:8": W, "y1": S, "lake:9": L},
               [("x2", "lake:5"), ("lake:5", "x1"), ("x1", "lake:6"), ("lake:6", "x0"),
                ("u2", "lake:7"), ("lake:7", "u1"), ("u1", "x2"), ("x3", "lake:8"), ("y1", "lake:6")])
    reg = {
        "gnis:1": _item("gnis:1", "X Creek", "stream", ["x0", "x1", "x2", "x3"], boundaries=[
            RegistryBoundary("x_creek__lake_5", "lake 5", "lake", ref="lake:5", wbk="5"),
            RegistryBoundary("x_creek__lake_6", "lake 6", "lake", ref="lake:6", wbk="6")]),
        "gnis:2": _item("gnis:2", "Y Creek", "stream", ["y1"], boundaries=[
            RegistryBoundary("y_creek__lake_6", "lake 6", "lake", ref="lake:6", wbk="6")]),
    }
    return g, reg


def test_a_confirmed_polygon_a_stream_flows_through_joins_that_stream():
    """Mutation: skip the join, or join by `ins` alone -> red."""
    g, reg = _creek()
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"5"}, rep)
    x = out["gnis:1"]
    assert "lake:5" in x.section_ids and "wbk:5" in x.ref_ids
    assert "x_creek__lake_5" not in {b.id for b in x.boundaries}      # F6: its own polygon is not another water
    assert "x_creek__lake_6" in {b.id for b in x.boundaries}          # an unconfirmed lake keeps its edge
    assert rep["counts"] == {"folded": 1}
    assert rep["rows"][0]["stream"] == "gnis:1"
    assert len(out) == len(reg)                                        # no item made


def test_only_an_isolated_polygon_is_skipped():
    """lake:9 no line touches; lake:7 only an unnamed line and no item holds it (held, never a new item); 999 is not in
    the graph. Mutation: make an item -> red."""
    g, reg = _creek()
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"7", "9", "999"}, rep)
    assert out == reg
    why = {r["wbk"]: r["why"] for r in rep["rows"]}
    assert why["9"].startswith("isolated")
    assert why["7"].startswith("unnamed, no item")
    assert why["999"] == "not in the graph"
    assert rep["counts"] == {"ignored": 3}


def test_a_stream_on_one_side_takes_it():
    """lake:8: X Creek ends in it, nothing leaves. Mutation: require in AND out -> red."""
    g, reg = _creek()
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"8"}, rep)
    assert "lake:8" in out["gnis:1"].section_ids
    assert rep["rows"][0]["via"] == "ends in it"
    # two named streams on its one side are never picked
    reg2 = dict(reg)
    reg2["gnis:1"] = _item("gnis:1", "X Creek", "stream", ["x0", "x1", "x2"])
    reg2["gnis:4"] = _item("gnis:4", "W Creek", "stream", ["x3"])
    g2 = _graph({"x3": S, "w3": S, "lake:8": W}, [("x3", "lake:8"), ("w3", "lake:8")])
    reg2["gnis:5"] = _item("gnis:5", "V Creek", "stream", ["w3"])
    rep2: dict = {}
    assert fold_confirmed_sloughs(reg2, g2, {"8"}, rep2) == reg2
    assert rep2["rows"][0]["why"].startswith("two streams")


def test_only_unnamed_lines_make_its_own_item_a_stream_on_that_line():
    """lake:7 held by an oxbow lake item: the item becomes kind stream, same id, nothing new; the line stays unnamed.
    Mutation: skip it, or mint an item -> red."""
    g, reg = _creek()
    reg = dict(reg, **{"wbk:7": _item("wbk:7", "OXBOWS", "lake", ["lake:7"], boundaries=[
        RegistryBoundary("oxbows__lake_7", "lake 7", "lake", ref="lake:7", wbk="7")])})
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"7"}, rep)
    assert set(out) == set(reg)
    assert out["wbk:7"].kind == "stream" and out["wbk:7"].section_ids == ("lake:7",)
    assert out["wbk:7"].boundaries == ()                               # F6: its own polygon is not another water
    assert "u1" not in out["wbk:7"].section_ids                         # the unnamed line is not taken
    assert rep["rows"][0]["via"] == "unnamed line" and rep["own_streams"] == {"wbk:7": {"name": "OXBOWS",
                                                                                       "polygons": 1}}


def test_unconfirmed_polygons_are_untouched():
    """lake:6 is threaded by X Creek but nobody confirmed it. Mutation: fold every threaded polygon -> red."""
    g, reg = _creek()
    out = fold_confirmed_sloughs(reg, g, set())
    assert out == reg
    out = fold_confirmed_sloughs(reg, g, {"5"})
    assert "lake:6" not in out["gnis:1"].section_ids


def test_different_streams_in_and_out_are_never_picked():
    g, reg = _creek()
    g2, _ = _creek()
    # lake:6: X and Y flow in, X flows out -> X is the one through it, Y is a tributary (folds into X)
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"6"}, rep)
    assert "lake:6" in out["gnis:1"].section_ids
    # Y Creek's lake edge onto it is now X Creek's mouth (F6, the other side)
    yb = {b.id: b for b in out["gnis:2"].boundaries}["y_creek__lake_6"]
    assert (yb.kind, yb.label) == ("confluence", "X Creek")
    # make x0 Z Creek's: X and Y in, Z out -> two streams, ignored
    reg2 = dict(reg)
    reg2["gnis:1"] = _item("gnis:1", "X Creek", "stream", ["x1", "x2", "x3"])
    reg2["gnis:3"] = _item("gnis:3", "Z Creek", "stream", ["x0"])
    rep2: dict = {}
    assert fold_confirmed_sloughs(reg2, g2, {"6"}, rep2) == reg2
    assert rep2["rows"][0]["why"] == "two streams (different in and out)"


def test_a_run_of_confirmed_polygons_folds_as_one():
    """p1 -> p2 joined directly: X in on p1, X out of p2. Mutation: test each polygon alone -> red."""
    g = _graph({"a": S, "lake:1": L, "lake:2": L, "b": S}, [("a", "lake:1"), ("lake:1", "lake:2"), ("lake:2", "b")])
    reg = {"gnis:1": _item("gnis:1", "X Creek", "stream", ["a", "b"])}
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"1", "2"}, rep)
    assert {"lake:1", "lake:2"} <= set(out["gnis:1"].section_ids)
    assert rep["counts"] == {"folded": 2}
    # one of the two unconfirmed: X enters the confirmed one only -> it still joins X (one side); lake:2 stays
    rep = {}
    out = fold_confirmed_sloughs(reg, g, {"1"}, rep)
    assert "lake:1" in out["gnis:1"].section_ids and "lake:2" not in out["gnis:1"].section_ids
    assert rep["rows"][0]["via"] == "ends in it"


def test_a_confirmed_lake_item_is_absorbed_but_never_split():
    g, reg = _creek()
    reg = dict(reg, **{"wbk:5": _item("wbk:5", "X Creek", "lake", ["lake:5"], variants=["Five Pond"])})
    out = fold_confirmed_sloughs(reg, g, {"5"})
    assert "wbk:5" not in out and "wbk:5" in out["gnis:1"].aliases and "Five Pond" in out["gnis:1"].variants
    # the item holds an unconfirmed polygon too -> ignored, nothing split
    reg2 = dict(reg, **{"wbk:5": _item("wbk:5", "X Creek", "lake", ["lake:5", "lake:6"])})
    rep: dict = {}
    assert fold_confirmed_sloughs(reg2, g, {"5"}, rep) == reg2
    assert rep["rows"][0]["why"].startswith("item not confirmed")


def test_already_stream_and_a_cut_on_its_edge_are_left_alone():
    g, reg = _creek()
    reg2 = dict(reg)
    reg2["gnis:1"] = _item("gnis:1", "X Creek", "stream", ["x0", "x1", "x2", "x3"], boundaries=[
        RegistryBoundary("x_creek__lake_5", "lake 5", "lake", ref="lake:5", wbk="5", aliases=("split:falls",))])
    rep: dict = {}
    assert fold_confirmed_sloughs(reg2, g, {"5"}, rep) == reg2
    assert rep["rows"][0]["why"].startswith("cut on its edge")
    once = fold_confirmed_sloughs(reg, g, {"5"})
    rep = {}
    assert fold_confirmed_sloughs(once, g, {"5"}, rep) == once            # idempotent
    assert rep["counts"] == {"already stream": 1}


# --------------------------------------------------------------------------- the apply step

def _mark(target, verdict="verified", call="stream", P="y"):
    return SimpleNamespace(scope="slough", target=target, verdict=verdict, call=call, answers={"P": P},
                           key=("slough", target, None), reviewer="dawson", date="2026-10-29", note="n")


def test_apply_writes_only_confirmed_stream_calls_and_skips_stale(monkeypatch):
    """Mutation: drop the call / P / verdict test, or write a stale mark -> red."""
    from pipeline.regs.review import marks as M
    ms = [_mark("30"), _mark("4"), _mark("5", call="not stream"), _mark("6", P="?", verdict="unsure"),
          _mark("7"), _mark("8")]
    decided = SimpleNamespace(scope=lambda *s: ms, provenance={("slough", "4", None): {"mark": "abc"}})
    monkeypatch.setattr(M.Prints, "model_validate", staticmethod(lambda x: x))
    monkeypatch.setattr(M, "status", lambda m, now: {"status": "stale" if now == "moved" else m.verdict,
                                                     "reasons": ["binding"] if now == "moved" else []})
    rows, skipped = SC.build(decided, prints_of=lambda w: None if w == "8" else ("moved" if w == "7" else "ok"))
    assert [r["wbk"] for r in rows] == ["4", "30"]                     # numeric order
    assert rows[0]["mark"] == "abc"
    assert {s["wbk"]: s["why"].split(" ")[0] for s in skipped} == {"7": "stale:", "8": "candidate"}


def test_load_round_trips_and_refuses_a_missing_file(tmp_path):
    p = tmp_path / "slough_calls.json"
    p.write_text(SC.document([{"wbk": "4", "call": "stream"}, {"wbk": "30", "call": "stream"}]))
    assert SC.load(p) == {"4", "30"}
    with pytest.raises(FileNotFoundError, match="slough_calls"):
        SC.load(tmp_path / "nope.json")


def test_the_committed_file_is_apply_of_the_marks():
    """`--check`: data/curated/waters/slough_calls.json == apply(the slough marks' decisions, in memory)."""
    from pipeline.tests import under_test
    from pipeline.regs.review import export as E
    rows, _ = SC.build(E.load())
    assert under_test.curated().waters.slough_calls.read_text(encoding="utf-8") == SC.document(rows)
    assert len(rows) == 811


def test_an_isolated_item_is_rekinded_a_general_stream_in_place():
    """Owner 2026-10-30: the Okanagan oxbows are general streams even cut off from the river. lake:9 (no edge) held by
    an oxbow lake item -> kind stream, same id, joined to nothing; an isolated polygon with no item stays skipped.
    Mutation: skip it, join it to a river, or mint an item -> red."""
    g, reg = _creek()
    reg = dict(reg, **{"wbk:9": _item("wbk:9", "OKANAGAN RIVER OXBOWS", "lake", ["lake:9"])})
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"9"}, rep)
    assert set(out) == set(reg)
    assert out["wbk:9"].kind == "stream" and out["wbk:9"].section_ids == ("lake:9",) and not out["wbk:9"].aliases
    assert out["gnis:1"] == reg["gnis:1"]
    assert rep["rows"][0]["via"] == "isolated item"


# --------------------------------------------------------------------------- the promote gate (mechanical)

def test_the_gate_passes_changes_on_allowed_waters_and_stops_on_any_other():
    """Owner 2026-10-30: only folded polygons, re-kinded items and the streams they join may change. Mutation: let a
    rescope or a gain elsewhere through -> red."""
    from pipeline.tools.audit.slough_fold_gate import allowed_sections, violations
    g, reg = _creek()
    reg = dict(reg, **{"wbk:9": _item("wbk:9", "OXBOWS", "lake", ["lake:9"])})
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"5", "9"}, rep)
    allowed = allowed_sections(rep, out)
    assert {"lake:5", "lake:9", "x0", "x3"} <= allowed and "y1" not in allowed

    def rows(*r):
        return iter(sorted(((e, ru, s), sc) for e, ru, s, sc in r))
    before = rows(("e1", "z1", "lake:5", "reach"), ("e1", "z1", "y1", "reach"))
    after = rows(("e1", "z1", "x0", "reach"), ("e1", "z1", "y1", "reach"), ("e2", "r", "lake:9", "trib"))
    assert violations(before, after, allowed) == (0, [])
    before = rows(("e1", "z1", "y1", "reach"))
    after = rows(("e1", "z1", "y1", "trib"), ("e3", "r", "u1", "reach"))
    n, kept = violations(before, after, allowed)
    assert n == 2 and {k["change"] for k in kept} == {"rescoped", "gained"}


def test_the_gate_allows_a_creek_that_reaches_its_stream_through_a_folded_polygon():
    """c1 -> lake:5 (folded into X Creek): c1 is X's tributary now — allowed and listed as via_fold. A change on y1
    (Y Creek, drains into lake:6, not folded) still fails. Mutation: drop the via set, or walk past X -> red."""
    from pipeline.tools.audit.slough_fold_gate import allowed_sections, folded_polygons, via_fold_sections, violations
    from pipeline.tests.test_registry_flowing import _graph as G
    g = G({"x2": S, "lake:5": L, "x1": S, "c1": S, "c2": S, "x3": S, "y1": S, "lake:6": L, "x0": S},
          [("x3", "x2"), ("x2", "lake:5"), ("c2", "c1"), ("c1", "lake:5"), ("lake:5", "x1"), ("x1", "lake:6"),
           ("y1", "lake:6"), ("lake:6", "x0")])
    reg = {"gnis:1": _item("gnis:1", "X Creek", "stream", ["x0", "x1", "x2", "x3"]),
           "gnis:2": _item("gnis:2", "Y Creek", "stream", ["y1"])}
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"5"}, rep)
    allowed = allowed_sections(rep, out)
    via = via_fold_sections(g, folded_polygons(rep), allowed)
    assert via == {"c1", "c2"}                                         # never up X Creek's own line

    def rows(*r):
        return iter(sorted(((e, ru, s), sc) for e, ru, s, sc in r))
    listed: list = []
    before = rows(("e1", "x", "x1", "reach"))
    after = rows(("e1", "x", "x1", "reach"), ("e1", "x", "c1", "trib"), ("e1", "x", "c2", "trib"))
    assert violations(before, after, allowed, via, listed) == (0, [])
    assert [r["section"] for r in listed] == ["c1", "c2"]
    before = rows(("e1", "x", "x1", "reach"))
    after = rows(("e1", "x", "x1", "reach"), ("e1", "x", "y1", "trib"))
    n, kept = violations(before, after, allowed, via, [])
    assert n == 1 and kept[0]["section"] == "y1"


def test_a_water_of_another_name_is_never_absorbed():
    """Owner 2026-11-03: an Okanagan River Oxbow touching Park Rill stays its own water (a stream in place), so the
    oxbows' rule never spreads over the rill. Mutation: drop the name check -> red."""
    g, reg = _creek()
    reg = dict(reg, **{"wbk:5": _item("wbk:5", "OXBOWS", "lake", ["lake:5"])})
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"5"}, rep)
    assert set(out) == set(reg)
    assert out["wbk:5"].kind == "stream" and out["wbk:5"].section_ids == ("lake:5",)
    assert "lake:5" not in out["gnis:1"].section_ids and "wbk:5" not in out["gnis:1"].aliases
    assert rep["rows"][0]["via"] == "own name (touches gnis:1)"
    # the same polygon under the stream's own name still joins it
    reg["wbk:5"] = _item("wbk:5", "X Creek", "lake", ["lake:5"])
    out = fold_confirmed_sloughs(reg, g, {"5"}, {})
    assert "lake:5" in out["gnis:1"].section_ids and "wbk:5" not in out
