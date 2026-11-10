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
