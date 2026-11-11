"""T546: THE REACH RUN DROPS NOTHING SILENTLY — a curated id, a scope or a closure piece that cannot be
placed stops the run, or is counted where the run and the release check read it.

Synthetic river (`test_registry_flowing._river`), no FWA. Each test names the mutation that turns it red."""
from __future__ import annotations

import pytest

from pipeline.atlas.reach.build import build_reaches, gone_refs
from pipeline.tests.test_registry_flowing import _river


def _entry(**kw):
    return {"entry_id": "r1:x_river@1-1", "name": "X RIVER", "region": "1", "matched": ["gnis:1"],
            "rules": [{"rule_id": "r1", "type": "closure", "extents": [{"op": "whole"}]}], **kw}


# ------------------------------------------------------------------ items 1 + 6: a gone id stops the run

def test_a_matched_id_the_registry_lacks_stops_the_run():
    """Mutation: drop the `gone_refs` refusal in `build_reaches` — `covered_ids` filters the id out and the
    row binds the rest of its waters with no word said."""
    g, reg = _river()
    assert build_reaches([("1", _entry())], reg, g).report.n_rules == 1         # clean: runs
    with pytest.raises(SystemExit, match=r"r1:x_river@1-1 matched: gnis:404"):
        build_reaches([("1", _entry(matched=["gnis:1", "gnis:404"]))], reg, g)


def test_an_extent_or_carve_out_id_the_registry_lacks_is_named_too():
    g, reg = _river()
    e = _entry(rules=[{"rule_id": "r1", "type": "closure",
                       "extents": [{"op": "whole", "item_ids": ["gnis:1", "wbk:404"],
                                    "outside_items": ["wbk:405"]}]}])
    assert gone_refs([e], reg) == ["r1:x_river@1-1 item_ids: wbk:404",
                                   "r1:x_river@1-1 outside_items: wbk:405"]
    with pytest.raises(SystemExit, match="2 record id"):
        build_reaches([("1", e)], reg, g)


def test_an_absorbed_id_is_not_counted_as_gone():
    """An absorbed id has its own refusal (`absorbed_refs`, naming the absorber): not reported twice."""
    from pipeline.atlas.registry.flowing import merge_flowing_polygons
    g, reg = _river()
    out = merge_flowing_polygons(reg, g)
    assert "wbk:2" not in out
    assert gone_refs([_entry(matched=["wbk:2"])], out) == []


# ------------------------------------------------------------------ item 2: a scope that fails unbinds the row

def _scoped(*scope, rules=True):
    return _entry(extents=list(scope), rules=[{"rule_id": "r1", "type": "closure", "extents": [{"op": "whole"}]}]
                  if rules else [])


def test_a_good_scope_still_clips():
    g, reg = _river()
    res = build_reaches([("1", _scoped({"op": "upstream_of", "splits": ["x_river__lake_1"]}))], reg, g)
    b = res.bindings[0]
    assert b.outcome.value == "bound" and "r0" not in b.sections and res.report.scope_unresolved == []


@pytest.mark.parametrize("scope", [
    [{"op": "upstream_of", "splits": ["no_such_cut"]}],                                          # fails
    [{"op": "upstream_of", "splits": ["x_river__lake_1"]}, {"op": "upstream_of", "splits": ["no_such_cut"]}],
], ids=["fails", "partly_fails"])
def test_a_scope_that_fails_holds_every_rule_unresolved_and_stops_the_run(scope):
    """Mutation: `A_FAILED_SCOPE_UNBINDS_THE_ROW = False` — the row binds the whole river (the closure of
    one stretch spread along all of it) or narrows to the part that resolved, and the run passes."""
    from pipeline.atlas.reach.build import ScopeGateError, scope_gate
    g, reg = _river()
    res = build_reaches([("1", _scoped(*scope))], reg, g)
    b = res.bindings[0]
    assert (b.outcome.value, b.reason.value, b.sections) == ("unresolved", "scope_unresolved", ())
    assert res.report.scope_unresolved == ["r1:x_river@1-1"]
    with pytest.raises(ScopeGateError, match=r"r1:x_river@1-1: 1 rule"):
        scope_gate(res.bindings)


def test_a_scope_placing_no_section_is_a_failed_scope():
    from pipeline.atlas.reach.build import entry_scope
    g, reg = _river()
    from pipeline.tests.test_registry_flowing import _item
    reg = {**reg, "gnis:7": _item("gnis:7", "Z Creek", "stream", [])}
    clip, failed = entry_scope({"extents": [{"op": "whole", "item_id": "gnis:7"}]}, ["gnis:1"], reg, g)
    assert clip == frozenset() and failed


def test_a_pointer_row_with_a_failed_scope_is_listed_but_does_not_stop_the_run():
    from pipeline.atlas.reach.build import scope_gate
    g, reg = _river()
    res = build_reaches([("1", _scoped({"op": "upstream_of", "splits": ["no_such_cut"]}, rules=False))], reg, g)
    assert res.report.scope_unresolved == ["r1:x_river@1-1"]
    scope_gate(res.bindings)                                     # no rule held: passes


def test_the_review_app_path_gets_the_same_verdict_by_construction():
    """`entry_reaches` passes `entry_scope`'s clip to `build_reach` (AGENTS 16): the empty clip is the verdict."""
    from pipeline.atlas.reach.build import build_reach, entry_scope
    g, reg = _river()
    e = _scoped({"op": "upstream_of", "splits": ["no_such_cut"]})
    clip, _ = entry_scope(e, ["gnis:1"], reg, g)
    b, _ = build_reach(e, e["rules"][0], reg, g, covered=["gnis:1"], clip=clip)
    assert b.reason.value == "scope_unresolved"


# ------------------------------------------------------------------ item 3: an unplaced piece is counted

def test_every_withheld_piece_is_listed_with_its_reason():
    """Mutation: `report.unplaced = []` (or drop the `scope_unclassified` branch of `unplaced_pieces`) — the
    Noons Creek / Seymour River pieces are again only lines in rule_diagnostic.jsonl."""
    from pipeline.atlas.reach.build import unplaced_pieces
    from pipeline.atlas.reach.models import Diagnostic
    diags = [
        Diagnostic("r2:noons_creek@2-8", "noons_creek.r1", "unclassified",
                   {"pieces": ["-2200000392:0"], "included": False,
                    "reasons": {"-2200000392:0": "flows_to_the_sea_unjoined"}}),
        Diagnostic("r2:x@2-8", "", "scope_unclassified",
                   {"pieces": ["b:0", "b:10"], "reasons": {"b:0": "meets_the_line_at_the_cut"}}),
        Diagnostic("r2:y@2-8", "y.r1", "ambiguous_cut", {"pieces": ["c:0"]}),
    ]
    got = unplaced_pieces(diags)
    assert [(x["entry_id"], x["piece"], x["reason"]) for x in got] == [
        ("r2:noons_creek@2-8", "-2200000392:0", "flows_to_the_sea_unjoined"),
        ("r2:x@2-8", "b:0", "meets_the_line_at_the_cut"),
        ("r2:x@2-8", "b:10", "unknown")]


def test_a_scope_unclassified_piece_carries_reasons_so_the_straddle_gate_sees_it(monkeypatch):
    """Mutation: drop `reasons` from the `scope_unclassified` payload — `straddle_gate` reads only `reasons`,
    so a straddler lost at the entry's scope passed the gate (T546 item 3)."""
    from pipeline.atlas.reach.build import StraddleGateError, straddle_gate
    import pipeline.atlas.reach.build as B
    g, reg = _river()
    e = _scoped({"op": "upstream_of", "splits": ["x_river__lake_1"]})

    def fake(*a, why=None, **k):
        if why is not None:
            why["c1"] = "straddles_the_cut"
        return ["c1"]
    monkeypatch.setattr(B, "scope_unclassified", fake)
    res = build_reaches([("1", e)], reg, g)
    d = [x for x in res.diagnostics if x.kind == "scope_unclassified"][0]
    assert d.payload["reasons"] == {"c1": "straddles_the_cut"}
    assert res.report.unplaced == [{"entry_id": "r1:x_river@1-1", "rule_id": "", "piece": "c1",
                                    "reason": "straddles_the_cut", "kind": "scope_unclassified"}]
    with pytest.raises(StraddleGateError, match="c1"):
        straddle_gate(res.diagnostics)


# ------------------------------------------------------------------ item 4: a DFO place's diagnostics are kept

def test_a_dfo_places_diagnostics_are_written_not_discarded(monkeypatch, tmp_path):
    """Mutation: `binding, _ = build_reach(...)` in `dfo_salmon.resolve_all` — the ambiguous-cut pick and the
    unplaced piece of a DFO place vanish again."""
    from types import SimpleNamespace
    from pipeline.atlas.reach import build as B
    from pipeline.atlas.reach import dfo_salmon as D
    from pipeline.atlas.reach.models import Diagnostic, Outcome, RuleBinding
    from pipeline.regs.dfo_salmon import entries as E
    loc = SimpleNamespace(location_id="2:x:below", binding=SimpleNamespace(resolved=True), is_variant=False,
                          duplicate_of=None, water_id=None, tidal_water=False)
    ef = SimpleNamespace(locations=[loc], waters=[], water=lambda w: None)
    monkeypatch.setattr(D, "_slugs", lambda: ["2"])
    monkeypatch.setattr(E, "load", lambda *a, **k: ef)
    monkeypatch.setattr(E, "to_reach_input", lambda *a, **k: ({"entry_id": "?"}, [{"rule_id": "p", "extents": [{}]}]))
    monkeypatch.setattr(B, "build_reach", lambda e, *a, **k: (
        RuleBinding(e["entry_id"], "p", Outcome.bound, ("s1",)),
        [Diagnostic(e["entry_id"], "p", "unclassified", {"pieces": ["s9"], "reasons": {"s9": "leaves_bc_unjoined"}}),
         Diagnostic(e["entry_id"], "p", "ambiguous_cut", {"split": "c", "used_m": 10.0})]))
    got: list = []
    rows = D.resolve_all({}, None, outside=frozenset(), diagnostics=got)
    assert rows[0]["sections"] == ["s1"]
    assert sorted(d["kind"] for d in got) == ["ambiguous_cut", "unclassified"]
    assert all(d["location_id"] == "2:x:below" and d["region"] == "2" for d in got)
    assert D.write_diagnostics(tmp_path, got) == 2
    first = (tmp_path / D.DIAG_FILE).read_bytes()
    D.write_diagnostics(tmp_path, list(reversed(got)))
    assert (tmp_path / D.DIAG_FILE).read_bytes() == first          # sorted: byte-identical whatever the order


# ------------------------------------------------------------------ item 7: an override id that hits nothing

def test_an_override_id_no_registry_item_carries_is_listed():
    """Mutation: drop `override_id_gaps` (or its partial rows) — OKANAGAN RIVER OXBOWS binds its 41 resolved
    ids and its 26 dead wsc channels drop with nothing said."""
    from pipeline.regs.matching.matcher import override_id_gaps
    from pipeline.tests.test_registry_flowing import _item
    reg = {"gnis:1": _item("gnis:1", "X River", "stream", ["r0"], refs=("gnis:1",)),
           "wbk:7": _item("wbk:7", "Oxbow", "lake", ["lake:7"], refs=("wbk:7",))}
    ov = [{"criteria": {"name_verbatim": "X OXBOWS", "mus": ["8-1"]}, "waterbody_keys": ["7"],
           "fwa_watershed_codes": ["300-432687-461418"]},
          {"criteria": {"name_verbatim": "X RIVER", "mus": []}, "gnis_ids": ["1"]},
          {"criteria": {"name_verbatim": "GONE", "mus": []}, "skip": True, "gnis_ids": ["404"]}]
    assert override_id_gaps(ov, reg) == [{"name": "X OXBOWS", "mu": ["8-1"],
                                          "unresolved": ["wsc:300-432687-461418"], "resolved": 1}]


# ------------------------------------------------------------------ T632 1: tidal water leaves every DFO place

def _dfo_places(monkeypatch, *ids):
    """DFO places on the synthetic X River (`whole` of gnis:1: r0..r3), resolved by the REAL `build_reach`."""
    from types import SimpleNamespace
    from pipeline.atlas.reach import dfo_salmon as D
    from pipeline.regs.dfo_salmon import entries as E
    locs = [SimpleNamespace(location_id=i, binding=SimpleNamespace(resolved=True), is_variant=False,
                            duplicate_of=None, water_id=None, tidal_water=False) for i in ids]
    ef = SimpleNamespace(locations=locs, waters=[], water=lambda w: None)
    monkeypatch.setattr(D, "_slugs", lambda: ["1"])
    monkeypatch.setattr(E, "load", lambda *a, **k: ef)
    monkeypatch.setattr(E, "to_reach_input", lambda *a, **k: (
        {"entry_id": "?", "name": "X RIVER", "region": "1", "matched": ["gnis:1"]},
        [{"rule_id": "p", "type": "closure", "extents": [{"op": "whole"}]}]))
    return D


def test_a_dfo_place_loses_the_runs_tidal_water(monkeypatch):
    """Mutation: drop `tidal=lim` from `resolve_all`'s `build_reach` call (the T546 state) — r0, below the tidal
    boundary, stays in the place and the map paints a DFO salmon chain on tidal water."""
    g, reg = _river()
    D = _dfo_places(monkeypatch, "1:x:whole")
    assert D.resolve_all(reg, g, outside=frozenset())[0]["sections"] == ["r0", "r1", "r2", "r3"]   # no map: all
    diags: list = []
    rows = D.resolve_all(reg, g, outside=frozenset(), diagnostics=diags, tidal={"r0": "split:x_river__tidal"})
    assert rows[0]["sections"] == ["r1", "r2", "r3"] and rows[0]["mapped"]
    assert [(d["kind"], d["payload"]["removed"]) for d in diags if d["kind"] == "tidal"] == [("tidal", 1)]


def test_a_place_with_a_stated_reason_keeps_its_tidal_water(monkeypatch):
    """Mutation: ignore `keep_tidal` — the place the owner exempted loses its tidal water like the rest."""
    g, reg = _river()
    D = _dfo_places(monkeypatch, "1:x:whole", "1:x:other")
    rows = D.resolve_all(reg, g, outside=frozenset(), tidal={"r0": "split:x"},
                         keep_tidal={"1:x:whole": "DFO's own text runs to the river mouth"})
    by = {r["location_id"]: r["sections"] for r in rows}
    assert by == {"1:x:whole": ["r0", "r1", "r2", "r3"], "1:x:other": ["r1", "r2", "r3"]}


def test_a_place_the_subtraction_empties_or_a_keep_row_naming_no_place_stops_the_run(monkeypatch):
    """Mutation: drop the `drowned` (or `stale`) refusal — a place wholly in tidal water reads 'not yet mapped' on the
    card, and a keep row for a renamed place silently does nothing."""
    g, reg = _river()
    D = _dfo_places(monkeypatch, "1:x:whole")
    with pytest.raises(ValueError, match=r"wholly in tidal water.*\n.*1:x:whole"):
        D.resolve_all(reg, g, outside=frozenset(), tidal={s: "split:x" for s in ("r0", "r1", "r2", "r3")})
    with pytest.raises(ValueError, match=r"name no DFO place.*\n  1:x:gone"):
        D.resolve_all(reg, g, outside=frozenset(), tidal={"r0": "split:x"}, keep_tidal={"1:x:gone": "why"})


def test_tidal_calls_refuse_a_row_without_a_reason(tmp_path):
    import json
    from pipeline.atlas.reach.dfo_salmon import load_tidal_calls
    p = tmp_path / "tidal_calls.json"
    p.write_text(json.dumps({"keep_tidal": {"1:x:whole": {"reason": "the place is the estuary run"}}}))
    assert load_tidal_calls(p) == {"1:x:whole": "the place is the estuary run"}
    p.write_text(json.dumps({"keep_tidal": {"1:x:whole": {"reason": " "}}}))
    with pytest.raises(ValueError, match="no stated `reason`"):
        load_tidal_calls(p)
    assert load_tidal_calls() == {}            # the curated file: no place keeps its tidal water (T632, 2026-11-10)


# ------------------------------------------------------------------ T632 2: an unplaced piece stops the run

_NOONS = {"entry_id": "r2:noons_creek@2-8", "rule_id": "noons_creek.r1", "piece": "-2200000392:0",
          "reason": "flows_to_the_sea_unjoined", "kind": "unclassified"}
_SKEENA = {"entry_id": "6:skeena-river:near-kitsumkalum", "rule_id": "", "piece": "360220099:0",
           "reason": "straddles_the_cut", "kind": "dfo"}


def _calls(tmp_path, accepted=(), proposed=()):
    import json
    p = tmp_path / "unplaced_piece_calls.json"
    p.write_text(json.dumps({"_about": "test", "accepted": list(accepted), "proposed": list(proposed)}))
    return p


def test_every_unplaced_piece_stops_the_run_unless_accepted(tmp_path):
    """Mutation: gate only STRADDLE_GATE_REASONS (the T546 state) — Noons Creek's closure piece, which flows to the
    sea unjoined, passes the run withheld; or gate only the provincial pieces — the DFO Skeena pieces pass."""
    from pipeline.atlas.reach.build import UnplacedGateError, load_unplaced_calls, unplaced_gate
    with pytest.raises(UnplacedGateError, match=r"2 unplaced piece\(s\) not accepted") as e:
        unplaced_gate([_NOONS, _SKEENA], load_unplaced_calls(_calls(tmp_path)))
    assert "-2200000392:0 (flows_to_the_sea_unjoined)" in str(e.value) and "360220099:0" in str(e.value)
    ok = [{**x, "accepted_by": "owner", "accepted_on": "2026-11-11"} for x in (_NOONS, _SKEENA)]
    unplaced_gate([_NOONS, _SKEENA], load_unplaced_calls(_calls(tmp_path, accepted=ok)))       # passes


def test_a_proposal_is_not_an_acceptance(tmp_path):
    """Mutation: `load_unplaced_calls` reads `proposed` too — Claude's own proposals would pass the gate."""
    from pipeline.atlas.reach.build import UnplacedGateError, load_unplaced_calls, unplaced_gate
    calls = load_unplaced_calls(_calls(tmp_path, proposed=[_NOONS]))
    assert calls == []
    with pytest.raises(UnplacedGateError, match="noons_creek"):
        unplaced_gate([_NOONS], calls)


def test_a_stale_acceptance_stops_the_run_and_a_moved_piece_is_a_new_piece(tmp_path):
    """Mutation: key the acceptance on the piece alone — a piece now withheld for ANOTHER reason passes unseen."""
    from pipeline.atlas.reach.build import UnplacedGateError, load_unplaced_calls, unplaced_gate
    acc = load_unplaced_calls(_calls(tmp_path, accepted=[{**_NOONS, "accepted_by": "o", "accepted_on": "d"}]))
    with pytest.raises(UnplacedGateError, match="STALE acceptance r2:noons_creek@2-8"):
        unplaced_gate([], acc)
    with pytest.raises(UnplacedGateError, match="meets_the_line_at_the_cut"):
        unplaced_gate([{**_NOONS, "reason": "meets_the_line_at_the_cut"}], acc)


def test_an_acceptance_needs_who_and_when_and_never_takes_a_provincial_straddler(tmp_path):
    from pipeline.atlas.reach.build import load_unplaced_calls
    with pytest.raises(ValueError, match="lacks"):
        load_unplaced_calls(_calls(tmp_path, accepted=[_NOONS]))
    straddler = {**_NOONS, "reason": "straddles_the_cut", "accepted_by": "o", "accepted_on": "d"}
    with pytest.raises(ValueError, match="never accepted"):
        load_unplaced_calls(_calls(tmp_path, accepted=[straddler]))
    load_unplaced_calls(_calls(tmp_path, accepted=[{**_SKEENA, "accepted_by": "o", "accepted_on": "d"}]))


def test_a_dfo_places_withheld_pieces_feed_the_gate():
    """Mutation: `dfo_salmon.unplaced` skips `straddles_the_cut` or keeps an included piece."""
    from pipeline.atlas.reach.dfo_salmon import unplaced
    diags = [{"location_id": "6:s:x", "region": "6", "kind": "unclassified",
              "payload": {"included": False, "pieces": ["b:0", "a:0"], "reasons": {"a:0": "straddles_the_cut"}}},
             {"location_id": "6:s:y", "region": "6", "kind": "unclassified",
              "payload": {"included": True, "pieces": ["c:0"], "reasons": {}}},
             {"location_id": "6:s:x", "region": "6", "kind": "ambiguous_cut", "payload": {"pieces": ["d:0"]}}]
    assert [(x["entry_id"], x["piece"], x["reason"], x["rule_id"]) for x in unplaced(diags)] == [
        ("6:s:x", "a:0", "straddles_the_cut", ""), ("6:s:x", "b:0", "unknown", "")]


def test_the_curated_accept_list_holds_no_acceptance_claude_made():
    """Everything Claude adds is a proposal (owner 2026-10-29): an `accepted` row names its owner, never Claude."""
    from pipeline.atlas.reach.build import load_unplaced_calls
    assert all("claude" not in r["accepted_by"].lower() for r in load_unplaced_calls())


# ------------------------------------------------------------------ T632 3: a dead override id stops the build

def test_a_dead_override_id_is_found_in_every_override_the_dfo_ones_too(tmp_path):
    """Mutation: `load_overrides(p)` without `source="dfo"` in `dead_override_ids` — a DFO-tagged override with a
    dead id passes the check."""
    import json
    from pipeline.regs.matching.matcher import dead_override_ids, dead_override_lines
    from pipeline.tests.test_registry_flowing import _item
    reg = {"gnis:1": _item("gnis:1", "X River", "stream", ["r0"], refs=("gnis:1",))}
    p = tmp_path / "links.json"
    p.write_text(json.dumps([{"criteria": {"name_verbatim": "X RIVER", "mus": []}, "gnis_ids": ["1"]},
                             {"source": "dfo", "criteria": {"name_verbatim": "Y", "mus": ["6-1"]},
                              "gnis_ids": ["404"]}]))
    gaps = dead_override_ids(reg, p)
    assert gaps == [{"name": "Y", "mu": ["6-1"], "unresolved": ["gnis:404"], "resolved": 0}]
    assert dead_override_lines(gaps) == ["Y 6-1: 1 id(s) name no water (0 resolve): gnis:404"]


def test_the_override_check_exits_non_zero_and_fast_fail_runs_it(monkeypatch, tmp_path, capsys):
    """Mutation: `main` returns 0 on a gap, or the fast-fail step is removed."""
    import json
    from pathlib import Path
    import pipeline.atlas.registry as R
    from pipeline.regs.matching import matcher as M
    from pipeline.tests.test_registry_flowing import _item
    reg = {"gnis:1": _item("gnis:1", "X River", "stream", ["r0"], refs=("gnis:1",))}
    monkeypatch.setattr(R, "load_registry", lambda path: reg)
    p = tmp_path / "links.json"
    p.write_text(json.dumps([{"criteria": {"name_verbatim": "X RIVER", "mus": []}, "gnis_ids": ["1", "404"]}]))
    monkeypatch.setattr(M, "dead_override_ids", lambda r, path=None, _f=M.dead_override_ids: _f(r, p))
    assert M.main(["--registry", "unused"]) == 1 and "X RIVER -: 1 id(s)" in capsys.readouterr().err
    p.write_text(json.dumps([{"criteria": {"name_verbatim": "X RIVER", "mus": []}, "gnis_ids": ["1"]}]))
    assert M.main(["--registry", "unused"]) == 0
    ff = (Path(__file__).resolve().parents[2] / "scripts" / "fast-fail.sh").read_text()
    assert '"$PY" -m pipeline.regs.matching.matcher' in ff
