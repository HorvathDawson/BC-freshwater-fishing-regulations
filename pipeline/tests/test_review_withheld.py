"""T779: the side check of a DFO place lists every piece it withholds and refuses what the reach gate would stop on
(`unplaced_piece_calls.json` `accepted`), at save time, not 14 minutes into the build."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.atlas.reach.build import UnplacedGateError, unplaced_gate
from pipeline.regs.review import apply_proposals as AP
from pipeline.regs.review import withheld as W


def diag(kind="unclassified", pieces=("p1", "p2"), reasons=None, included=False):
    return SimpleNamespace(kind=kind, payload={"pieces": list(pieces), "included": included,
                                               "reasons": reasons or {"p1": "straddles_the_cut", "p2": "unknown"}})


def accepted(piece, reason, loc="6:river:a"):
    return {"entry_id": loc, "rule_id": "", "piece": piece, "reason": reason, "kind": "unclassified"}


def test_every_withheld_piece_is_listed_with_its_reason_and_an_included_one_is_not():
    got = W.rows("6:river:a", [diag(), diag(pieces=("p3",), included=True), diag(kind="complement")])
    assert [(x["piece"], x["reason"], x["entry_id"], x["rule_id"]) for x in got] == [
        ("p1", "straddles_the_cut", "6:river:a", ""), ("p2", "unknown", "6:river:a", "")]


def test_a_piece_no_accepted_row_names_is_refused_and_an_accepted_one_is_not():
    rows = W.rows("6:river:a", [diag()])
    assert [x["piece"] for x in W.unaccepted(rows, [])] == ["p1", "p2"]
    assert [x["piece"] for x in W.unaccepted(rows, [accepted("p1", "straddles_the_cut")])] == ["p2"]
    # an acceptance is for that piece, that reason and that place: any other is a new piece
    assert len(W.unaccepted(rows, [accepted("p1", "other_reason"), accepted("p2", "unknown", loc="6:river:z")])) == 2
    assert W.unaccepted(rows, [accepted("p1", "straddles_the_cut"), accepted("p2", "unknown")]) == []


def test_the_check_is_the_gates_own_rule():
    """what `unaccepted` refuses is exactly what `unplaced_gate` raises on (same key, so no drift)."""
    rows = W.rows("6:river:a", [diag()])
    for acc in ([], [accepted("p1", "straddles_the_cut")]):
        refused = bool(W.unaccepted(rows, acc))
        try:
            unplaced_gate(rows, acc)
            stopped = False
        except UnplacedGateError:
            stopped = True
        assert refused == stopped is True


def test_apply_refuses_a_dfo_mark_whose_check_withholds_a_piece():
    bad = {"id": "dfo-1", "check": {"cheap": True, "unaccepted": [{"piece": "p1", "reason": "straddles_the_cut"}]}}
    why = AP.withheld_refusal(bad)
    assert why and "p1 (straddles_the_cut)" in why and "unplaced_piece_calls.json" in why
    assert AP.withheld_refusal({"id": "dfo-2", "check": {"cheap": True, "unaccepted": []}}) is None
    assert AP.withheld_refusal({"id": "dfo-3"}) is None


def test_the_reach_binding_change_reports_withheld_and_unaccepted(monkeypatch):
    """`reuse.dfo_binding_change` carries `withheld` / `unaccepted` from `build_reach`'s diagnostics."""
    import sys
    sys.path.insert(0, "curation-review/backend")
    import dfo
    reuse = dfo.reuse                                      # the one the backend uses now (tests reload them)
    loc = {"binding": {"extents": [{"op": "whole"}]}}
    monkeypatch.setattr(dfo, "_locations", lambda: {"6:river:a": ("s", loc, None)})
    monkeypatch.setattr(dfo, "_reach_input", lambda lc, water: ({"entry_id": "6:river:a"}, {"rule_id": "r", "extents": [1]}))
    monkeypatch.setattr(reuse, "_registry", lambda: {})
    monkeypatch.setattr(reuse, "_graph", lambda: SimpleNamespace(nodes={}))
    binding = SimpleNamespace(sections=["s1"], outcome=SimpleNamespace(value="bound"))
    monkeypatch.setattr(reuse, "_build_reach", lambda *a, **k: (binding, [diag(pieces=("p1",))]))
    monkeypatch.setattr(W, "unaccepted", lambda rows, accepted=None: [x for x in rows if x["piece"] == "p1"])
    got = reuse.dfo_binding_change("6:river:a", [])
    assert got["withheld"] == [{"piece": "p1", "reason": "straddles_the_cut", "accepted": False}]
    assert got["unaccepted"] == got["withheld"]
