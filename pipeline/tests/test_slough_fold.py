"""T322: the owner-confirmed slough calls — the apply step (`tools.slough_calls`) and the registry fold
(`registry.flowing.fold_confirmed_sloughs`). Synthetic graphs; the committed file is checked against the decided data.

Owner 2026-10-30: a confirmed polygon with a stream flowing through it joins that stream; with no stream through it,
it is ignored (it is not a stream). Never a new unnamed stream."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.atlas.registry.flowing import fold_confirmed_sloughs
from pipeline.common.models import RegistryBoundary
from pipeline.tests.test_registry_flowing import L, S, W, _graph, _item
from pipeline.tools import slough_calls as SC


def _creek():
    """X Creek: x2 -> lake:5 (unnamed polygon) -> x1 -> lake:6 -> x0. lake:7 is a polygon an unnamed line (u1)
    runs through; lake:8 is where X Creek's head piece x3 ENDS (nothing leaves); Y Creek y1 feeds lake:6."""
    g = _graph({"x2": S, "lake:5": L, "x1": S, "lake:6": L, "x0": S, "u2": S, "lake:7": L, "u1": S,
                "x3": S, "lake:8": W, "y1": S},
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


def test_confirmed_with_no_stream_through_it_is_ignored_never_a_new_stream():
    """lake:7 has only an unnamed line through it; lake:8 is where X Creek ends. Mutation: make an item -> red."""
    g, reg = _creek()
    rep: dict = {}
    out = fold_confirmed_sloughs(reg, g, {"7", "8", "999"}, rep)
    assert out == reg
    why = {r["wbk"]: r["why"] for r in rep["rows"]}
    assert why["7"].startswith("no stream through it (only unnamed lines)")
    assert why["8"].startswith("no stream through it (a stream ends in it")
    assert why["999"] == "not in the graph"
    assert rep["counts"] == {"ignored": 3}


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
    # one of the two unconfirmed: the other has X on one side only -> ignored
    rep = {}
    assert fold_confirmed_sloughs(reg, g, {"1"}, rep) == reg
    assert rep["counts"] == {"ignored": 1}


def test_a_confirmed_lake_item_is_absorbed_but_never_split():
    g, reg = _creek()
    reg = dict(reg, **{"wbk:5": _item("wbk:5", "Five Pond", "lake", ["lake:5"])})
    out = fold_confirmed_sloughs(reg, g, {"5"})
    assert "wbk:5" not in out and "wbk:5" in out["gnis:1"].aliases and "Five Pond" in out["gnis:1"].variants
    # the item holds an unconfirmed polygon too -> ignored, nothing split
    reg2 = dict(reg, **{"wbk:5": _item("wbk:5", "Five Pond", "lake", ["lake:5", "lake:6"])})
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


def test_the_committed_file_is_apply_of_the_decided_data():
    """`--check`: data/curated/waters/slough_calls.json == apply(decided/sloughs.json)."""
    from pipeline.common.curated import CURATED
    from pipeline.regs.review import export as E
    rows, _ = SC.build(E.load())
    assert CURATED.waters.slough_calls.read_text(encoding="utf-8") == SC.document(rows)
    assert len(rows) == 811
