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
