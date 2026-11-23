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


def _stops(rows, acc) -> bool:
    try:
        unplaced_gate(rows, acc)
    except UnplacedGateError:
        return True
    return False


def test_the_check_is_the_gates_own_rule():
    """what the save-time check refuses is exactly what `unplaced_gate` raises on (one rule, `unplaced_verdict`), in
    both directions: none accepted, one accepted, ALL accepted (lands, not stopped), and a stale acceptance."""
    rows = W.rows("6:river:a", [diag()])
    both = [accepted("p1", "straddles_the_cut"), accepted("p2", "unknown")]
    gone = accepted("p9", "straddles_the_cut")                       # this place withholds no p9 now: stale
    for acc, want in (([], True), (both[:1], True), (both, False), (both + [gone], True)):
        got = W.check("6:river:a", rows, acc)
        refused = bool(got["unaccepted"] or got["stale_acceptances"])
        assert refused == _stops(rows, acc) is want, acc


def test_a_stale_acceptance_of_this_place_is_reported_and_another_places_is_not_its_own():
    rows = W.rows("6:river:a", [diag()])
    both = [accepted("p1", "straddles_the_cut"), accepted("p2", "unknown")]
    assert W.stale("6:river:a", rows, both) == []
    assert [(x["piece"], x["reason"]) for x in W.stale("6:river:a", rows, both + [accepted("p2", "moved_on")])] == [
        ("p2", "moved_on")]                                         # the same piece, another reason: stale
    assert W.stale("6:river:a", rows, both + [accepted("p9", "x", loc="6:river:z")]) == []   # another place's row
    assert W.check("6:river:a", [], [accepted("p1", "straddles_the_cut")])["stale_acceptances"] == [
        {"piece": "p1", "reason": "straddles_the_cut"}]            # the piece was placed: the acceptance is stale


def test_apply_refuses_a_dfo_mark_whose_check_withholds_a_piece():
    bad = {"id": "dfo-1", "check": {"cheap": True, "unaccepted": [{"piece": "p1", "reason": "straddles_the_cut"}]}}
    why = AP.withheld_refusal(bad)
    assert why and "p1 (straddles_the_cut)" in why and "unplaced_piece_calls.json" in why
    assert AP.withheld_refusal({"id": "dfo-2", "check": {"cheap": True, "unaccepted": []}}) is None
    assert AP.withheld_refusal({"id": "dfo-3"}) is None
    stale = {"id": "dfo-4", "check": {"cheap": True, "unaccepted": [],
                                      "stale_acceptances": [{"piece": "p9", "reason": "unknown"}]}}
    assert "stale acceptance" in AP.withheld_refusal(stale)


def test_a_dfo_place_whose_check_cannot_run_is_refused_never_landed_unchecked():
    """a new or just-landed cut (`depends_on`), an unbound split, an error: `check` says `cheap: False` with no
    `unaccepted` — what the place withholds is unknown, and unknown never passes (W46)"""
    for why in ("names a new split (split-n1): bound only on the side atlas", "KeyError: 'no such split'"):
        got = AP.withheld_refusal({"id": "dfo-5", "scope": "dfo", "kind": "extent", "check": {"cheap": False, "why": why}})
        assert got and why in got and "cannot run" in got
    assert AP.withheld_refusal({"id": "dfo-6", "scope": "dfo", "kind": "extent"}) is not None   # never checked
    # an entry's cut change is bound by the side REACH run, whose own gate stops on the piece: not this check's
    assert AP.withheld_refusal({"id": "e-1", "scope": "entry", "kind": "extent", "check": {"cheap": False}}) is None


def _run_one(monkeypatch, prop, check, *, side=True, argv=()):
    """`apply_proposals.run` on ONE proposal, the binding check stubbed to `check`; returns (rc, events, materialized)."""
    from pipeline.regs.review import export as E
    from pipeline.regs.review import proposals as P
    made = []
    monkeypatch.setattr(E, "load", lambda *a, **k: [])
    monkeypatch.setattr(P.Sources, "live", classmethod(lambda cls, build=None: SimpleNamespace(text=lambda p: "")))
    monkeypatch.setattr(P, "build", lambda decided, src: [prop])
    monkeypatch.setattr(P, "duplicate_ids", lambda props: {})
    monkeypatch.setattr(AP, "plan", lambda props, tried, *a, **k: ((props[0], []) if props[0]["id"] not in tried
                                                                    else (None, [])))
    monkeypatch.setattr(AP, "needs_side", lambda *a: side)
    monkeypatch.setattr(P, "check", lambda ps, src: [q.update(check=dict(check)) for q in ps])
    monkeypatch.setattr(P, "materialize", lambda ps, src: made.append(ps[0]["id"]) or {})
    rc, events = AP.run(["--dry-run", *argv])
    return rc, events, made


@pytest.mark.parametrize("mode", ["side-checked", "side atlas ready", "no cut change"])
def test_run_refuses_a_dfo_place_the_gate_would_stop_and_lands_one_it_would_not(monkeypatch, mode):
    """THE GATE INSIDE `run`: a DFO place on a side (cut-changing) chain is checked too, whether `--side-checked` or its
    side atlas is ready; a check that cannot run, an unaccepted piece or a stale acceptance refuses it (nothing
    materialized); a clean check lands it (here: `would`, a dry run)."""
    prop = {"id": "dfo-1", "scope": "dfo", "kind": "extent", "marks": [], "title": "t", "edits": [], "digest": "0" * 64}
    side = mode != "no cut change"
    argv = ["--side-checked", "dfo-1"] if mode == "side-checked" else []
    cases = [({"cheap": False, "why": "names a new split (split-n1): bound only on the side atlas"}, "refused"),
             ({"cheap": True, "unaccepted": [{"piece": "p1", "reason": "straddles_the_cut"}]}, "refused"),
             ({"cheap": True, "unaccepted": [], "stale_acceptances": [{"piece": "p9", "reason": "x"}]}, "refused"),
             ({"cheap": True, "unaccepted": [], "stale_acceptances": [], "rules": {}}, "would")]
    for check, want in cases:
        rc, events, made = _run_one(monkeypatch, dict(prop), check, side=side, argv=argv)
        assert [e["status"] for e in events] == [want], check
        assert (rc, made) == ((1, []) if want == "refused" else (0, ["dfo-1"])), check


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
    calls = []
    monkeypatch.setattr(reuse, "_build_reach", lambda *a, **k: calls.append(k) or (binding, [diag(pieces=("p1",))]))
    monkeypatch.setattr(dfo, "as_the_run_binds", lambda lid: {"regional": False, "tidal": {"t1": "x"}})
    import pipeline.atlas.reach.build as B
    monkeypatch.setattr(B, "load_unplaced_calls", lambda path=None: [])
    got = reuse.dfo_binding_change("6:river:a", [])
    assert got["withheld"] == [{"piece": "p1", "reason": "straddles_the_cut", "accepted": False}]
    assert got["unaccepted"] == got["withheld"] and got["stale_acceptances"] == []
    assert calls == [{"regional": False, "tidal": {"t1": "x"}}] * 2          # before and after: the run's own limits


def test_a_save_time_binding_takes_the_runs_limits(monkeypatch):
    """(T779 item 4) `dfo_salmon.resolve_all` binds a place with `regional=False` and the run's tidal water out unless
    `keep_tidal` names it; the save-time bindings take the same (`dfo.as_the_run_binds`)."""
    import sys
    sys.path.insert(0, "curation-review/backend")
    import dfo
    import pipeline.atlas.reach.dfo_salmon as DS
    reuse = dfo.reuse
    monkeypatch.setattr(reuse, "_tidal", lambda: {"t1": "water:x"})
    monkeypatch.setattr(DS, "load_tidal_calls", lambda path=None: {"6:river:keep": "the owner's reason"})
    assert dfo.as_the_run_binds("6:river:a") == {"regional": False, "tidal": {"t1": "water:x"}}
    assert dfo.as_the_run_binds("6:river:keep") == {"regional": False, "tidal": None}
    # the preview binds with them
    import pipeline.atlas.reach.build as B
    seen = []
    binding = SimpleNamespace(sections=[], outcome=SimpleNamespace(value="unresolved"))
    monkeypatch.setattr(B, "build_reach", lambda *a, **k: seen.append(k) or (binding, []))
    monkeypatch.setattr(B, "load_unplaced_calls", lambda path=None: [])
    monkeypatch.setattr(reuse, "_registry", lambda: {})
    monkeypatch.setattr(reuse, "_graph", lambda: SimpleNamespace(nodes={}))
    monkeypatch.setattr(dfo, "section_km", lambda ids: 0.0)
    monkeypatch.setattr(dfo, "channels_in", lambda *a: [])
    monkeypatch.setattr(reuse, "_sections_geojson", lambda ids, kind: [])
    dfo._bind({"entry_id": "6:river:keep"}, {"rule_id": "r", "extents": [1]}, 10)
    assert seen == [{"regional": False, "tidal": None}]
