"""T779: the side check of a DFO place lists every piece it withholds and refuses what the reach gate would stop on
(`unplaced_piece_calls.json` `accepted`), at save time, not 14 minutes into the build."""
from __future__ import annotations

import json
import os
from pathlib import Path
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
        assert got and why in got and "could not run" in got
    assert AP.withheld_refusal({"id": "dfo-6", "scope": "dfo", "kind": "extent"}) is not None   # never checked
    # an entry's cut change is bound by the side REACH run, whose own gate stops on the piece: not this check's
    assert AP.withheld_refusal({"id": "e-1", "scope": "entry", "kind": "extent", "check": {"cheap": False}}) is None


def _run_one(monkeypatch, tmp_path, prop, check, *, mode="no cut change", stdout=None):
    """`apply_proposals.run` (a dry run) on ONE DFO proposal; returns (rc, events, materialized, side runs). `mode`:
    "no cut change" (the in-memory `proposals.check`, stubbed to `check`), "side atlas ready" (its side atlas and
    side copies at `proposals.side_dir`), "side-checked, --build" (a side atlas made elsewhere, named by --build),
    "side-checked, no atlas" (no atlas has the cut). On a side chain the check runs for real as `side_check`'s
    subprocess, whose PROCESS is stubbed: it prints `check`; each run's argv and env are recorded."""
    import subprocess
    from pipeline.regs.review import export as E
    from pipeline.regs.review import proposals as P
    made, runs = [], []
    monkeypatch.delenv("CURATION_BUILD", raising=False)
    monkeypatch.setattr(P, "REPO", tmp_path)
    monkeypatch.setattr(E, "load", lambda *a, **k: [])
    monkeypatch.setattr(P.Sources, "live", classmethod(lambda cls, build=None: SimpleNamespace(text=lambda p: "")))
    monkeypatch.setattr(P, "build", lambda decided, src: [prop])
    monkeypatch.setattr(P, "duplicate_ids", lambda props: {})
    monkeypatch.setattr(AP, "plan", lambda props, tried, *a, **k: ((props[0], []) if props[0]["id"] not in tried
                                                                    else (None, [])))
    monkeypatch.setattr(AP, "needs_side", lambda *a: mode != "no cut change")
    monkeypatch.setattr(P, "check", lambda ps, src: [q.update(check=dict(check)) for q in ps])
    monkeypatch.setattr(P, "materialize", lambda ps, src: made.append(ps[0]["id"]) or {})
    monkeypatch.setattr(P, "side_ready", lambda p, *a, **k: (tmp_path / P.side_dir(p) / "atlas").is_dir())
    monkeypatch.setattr(P, "atlas_holds_chain", lambda p, props, atlas, src=None: Path(atlas).is_dir())  # T1040: own test

    def fake_run(cmd, **k):
        runs.append({"cmd": cmd, "env": k.get("env") or {}})
        return subprocess.CompletedProcess(cmd, 0, json.dumps(check) + "\n" if stdout is None else stdout, "")
    monkeypatch.setattr(AP.subprocess, "run", fake_run)
    argv = ["--dry-run"]
    if mode == "side atlas ready":
        (tmp_path / P.side_dir(prop) / "atlas").mkdir(parents=True)
        (tmp_path / P.side_dir(prop) / P.SIDE_ROOT).mkdir(parents=True)
    if mode.startswith("side-checked"):
        argv += ["--side-checked", prop["id"]]
    if mode == "side-checked, --build":
        (tmp_path / "elsewhere").mkdir(parents=True)
        argv += ["--build", str(tmp_path / "elsewhere")]
    rc, events = AP.run(argv)
    return rc, events, made, runs


CASES = [({"cheap": False, "why": "KeyError: 'shuswap_river__new_cut'"}, "refused"),
         ({"cheap": True, "unaccepted": [{"piece": "p1", "reason": "straddles_the_cut"}]}, "refused"),
         ({"cheap": True, "unaccepted": [], "stale_acceptances": [{"piece": "p9", "reason": "x"}]}, "refused"),
         ({"cheap": True, "unaccepted": [], "stale_acceptances": [], "rules": {}}, "would")]
PROP = {"id": "dfo-1", "scope": "dfo", "kind": "extent", "target": "8:river:a", "marks": [], "title": "t",
        "edits": [{"scope": "entry", "field": "extents", "after": [{"op": "between", "splits": ["a", "b"]}]}],
        "digest": "0" * 64}


@pytest.mark.parametrize("mode", ["side atlas ready", "side-checked, --build", "no cut change"])
def test_run_refuses_a_dfo_place_the_gate_would_stop_and_lands_one_it_would_not(monkeypatch, tmp_path, mode):
    """THE GATE INSIDE `run`: a check that cannot run, an unaccepted piece or a stale acceptance refuses the place
    (nothing materialized); a clean check lands it (here: `would`, a dry run) — on the live atlas, and on a side chain
    on the atlas that has the cut."""
    for n, (check, want) in enumerate(CASES):
        rc, events, made, runs = _run_one(monkeypatch, tmp_path / str(n), dict(PROP), check, mode=mode)
        assert [e["status"] for e in events] == [want], check
        assert (rc, made) == ((1, []) if want == "refused" else (0, ["dfo-1"])), check
        assert len(runs) == (0 if mode == "no cut change" else 1)


def test_a_side_checked_place_is_bound_on_the_atlas_that_has_its_cut_and_lands(monkeypatch, tmp_path):
    """the side flow (`proposals.side_commands` for a DFO place with a new cut): the side check binds the place on its
    SIDE ATLAS with the patched side copies as the curated root, and a clean place lands (one live rebuild, not two)"""
    from pipeline.regs.review import proposals as P
    rc, events, made, runs = _run_one(monkeypatch, tmp_path, dict(PROP), CASES[-1][0], mode="side atlas ready")
    assert (rc, [e["status"] for e in events], made) == (0, ["would"], ["dfo-1"])
    (run,) = runs
    assert run["env"]["CURATION_BUILD"] == str(tmp_path / P.side_dir(PROP) / "atlas")
    assert run["env"]["CURATED_ROOT"] == str(tmp_path / P.side_dir(PROP) / P.SIDE_ROOT)
    assert run["cmd"][2:4] == ["pipeline.regs.review.side_check", "8:river:a"]
    assert json.loads(run["cmd"][4]) == [{"scope": "entry", "field": "extents", "after": PROP["edits"][0]["after"]}]
    # --build names a side atlas made elsewhere: bound there (no side copies beside it: the live curated root)
    rc, events, made, runs = _run_one(monkeypatch, tmp_path / "b", dict(PROP), CASES[-1][0], mode="side-checked, --build")
    assert made == ["dfo-1"] and runs[0]["env"]["CURATION_BUILD"] == str(tmp_path / "b" / "elsewhere")
    assert runs[0]["env"].get("CURATED_ROOT") == os.environ.get("CURATED_ROOT")       # not a side root: none was made


@pytest.mark.parametrize("stdout", ["", "Traceback (most recent call last):\n  KeyError: 'x'\n", "{not json"])
def test_a_side_check_that_prints_no_result_refuses(monkeypatch, tmp_path, stdout):
    """the side process exits 0 but prints no JSON result (a crash swallowed, a stray print): unknown, so refused"""
    rc, events, made, runs = _run_one(monkeypatch, tmp_path, dict(PROP), CASES[-1][0], mode="side atlas ready",
                                      stdout=stdout)
    assert (rc, [e["status"] for e in events], made, len(runs)) == (1, ["refused"], [], 1)
    assert "printed no result" in events[0]["why"]


def _boundary(*ids):
    return SimpleNamespace(answers_to=lambda: list(ids))


def test_the_cuts_a_place_names_are_read_at_any_depth_and_checked_against_the_atlas():
    binding = {"extents": [{"op": "between", "splits": ["a", "b"]},
                           {"op": "rest", "bounds": {"op": "upstream_of", "splits": ["c"]}}],
               "tributary_excludes": [{"splits": ["d"]}]}
    assert W.cuts_named(binding) == {"a", "b", "c", "d"}
    reg = {"i1": SimpleNamespace(boundaries=[_boundary("a", "lake_alias_b"), _boundary("b")]),
           "i2": SimpleNamespace(boundaries=[_boundary("c")])}
    assert W.missing_cuts(binding, reg) == ["d"]
    assert W.missing_cuts({"extents": [{"op": "upstream_of", "splits": ["lake_alias_b"]}]}, reg) == []   # an alias
    assert W.missing_cuts({"extents": [{"op": "whole"}]}, {}) == []


def test_a_place_on_an_atlas_without_its_cut_is_not_bound_there_and_refused(monkeypatch):
    """the binding check on an atlas that has not got the place's cut (the live one for a new cut, a stale side atlas,
    --build naming the wrong one) would bind NOTHING and withhold nothing: it says the cut is missing instead"""
    import sys
    sys.path.insert(0, "curation-review/backend")
    import dfo
    reuse = dfo.reuse
    loc = {"binding": {"extents": [{"op": "between", "splits": ["cut_a", "cut_b"]}]}}
    monkeypatch.setattr(dfo, "_locations", lambda: {"8:river:a": ("s", loc, None)})
    monkeypatch.setattr(dfo, "_reach_input", lambda lc, water: ({"entry_id": "8:river:a"}, {"rule_id": "r", "extents": [1]}))
    monkeypatch.setattr(reuse, "_registry", lambda: {"i": SimpleNamespace(boundaries=[_boundary("cut_a"), _boundary("cut_b")])})
    monkeypatch.setattr(reuse, "_graph", lambda: SimpleNamespace(nodes={}))
    binding = SimpleNamespace(sections=[], outcome=SimpleNamespace(value="unresolved"))
    monkeypatch.setattr(reuse, "_build_reach", lambda *a, **k: (binding, []))
    monkeypatch.setattr(dfo, "as_the_run_binds", lambda lid, loc=None: {"regional": False, "tidal": None})
    import pipeline.atlas.reach.build as B
    monkeypatch.setattr(B, "load_unplaced_calls", lambda path=None: [])
    probe = [{"field": "extents", "after": [{"op": "between", "splits": ["cut_a", "no_such_cut__t779_probe"]}]}]
    got = reuse.dfo_binding_change("8:river:a", probe)
    assert got["cheap"] is False and got["missing_cuts"] == ["no_such_cut__t779_probe"]
    assert AP.withheld_refusal({"id": "x", "scope": "dfo", "kind": "extent", "check": got})
    assert reuse.dfo_binding_change("8:river:a", [])["cheap"] is True               # its cuts are on this atlas


def test_with_no_atlas_that_has_the_cut_the_place_is_refused(monkeypatch, tmp_path):
    rc, events, made, runs = _run_one(monkeypatch, tmp_path, dict(PROP), CASES[-1][0], mode="side-checked, no atlas")
    assert (rc, [e["status"] for e in events], made, runs) == (1, ["refused"], [], [])
    assert "no atlas with this place's cut" in events[0]["why"]


def test_the_side_commands_of_a_dfo_place_with_a_new_cut_end_in_the_apply_that_checks_it():
    from pipeline.regs.review import proposals as P
    prop = {**PROP, "edits": [{"kind": "dfo", "scope": "entry", "field": "extents", "after": []}]}
    import unittest.mock as um
    with um.patch.object(P, "changes_cuts", lambda p, props: True):
        cmds = P.side_commands(prop, [prop])
    assert "--side-checked dfo-1" in cmds[-1] and "apply_proposals" in cmds[-1]


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
    monkeypatch.setattr(dfo, "as_the_run_binds", lambda lid, loc=None: {"regional": False, "tidal": {"t1": "x"}})
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
    monkeypatch.setattr(dfo, "_locations", lambda: {})
    assert dfo.as_the_run_binds("6:river:a") == {"regional": False, "tidal": {"t1": "water:x"}}
    assert dfo.as_the_run_binds("6:river:keep") == {"regional": False, "tidal": None}
    # a VARIANT with no extent of its own binds through its primary, and `keep_tidal` naming the primary keeps it too
    # (`resolve_all`: `loc` or `target`); a variant with its own extent, or an unconfirmed one, is its own target
    var = {"duplicate_of": "6:river:keep", "binding": {"extents": []}}
    assert dfo.as_the_run_binds("6:river:v", var)["tidal"] is None
    assert dfo.as_the_run_binds("6:river:v", {**var, "binding": {"extents": [{"op": "whole"}]}})["tidal"] == {"t1": "water:x"}
    assert dfo.as_the_run_binds("6:river:v", {**var, "duplicate_confirmed": False})["tidal"] == {"t1": "water:x"}
    monkeypatch.setattr(dfo, "_locations", lambda: {"6:river:v": ("6", var, None)})
    assert dfo.as_the_run_binds("6:river:v")["tidal"] is None                         # the stored place by default
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
