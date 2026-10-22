"""`reach.binding_diff`: the streamed gate run before a reach rerun is promoted.

No data: each run is a few hand-written rows in a tmp dir.
"""

from __future__ import annotations

import json

import pytest

from pipeline.atlas.reach import binding_diff as BD


def _run(d, sections, unresolved=(), licensing=None):
    d.mkdir()
    (d / "rule_section.jsonl").write_text("".join(
        json.dumps({"entry_id": e, "rule_id": r, "scope": sc, "section_id": s}, sort_keys=True) + "\n"
        for e, r, s, sc in sorted(sections)))
    (d / "rule_unresolved.jsonl").write_text("".join(
        json.dumps({"entry_id": e, "rule_id": r, "reason": why, "detail": ""}) + "\n"
        for e, r, why in unresolved))
    if licensing is not None:
        (d / "licensing_section.jsonl").write_text("".join(
            json.dumps({"entry_id": e, "record_id": r, "kind": "designation", "scope": "reach",
                        "section_id": s}) + "\n" for e, r, s in sorted(licensing)))
    return d


SAME = [("e1", "r1", "a", "reach"), ("e1", "r1", "b", "trib"), ("e2", "r1", "c", "reach")]


def test_identical_runs_report_nothing_and_exit_zero(tmp_path, capsys):
    a, b = _run(tmp_path / "a", SAME), _run(tmp_path / "b", SAME)
    rep = BD.diff_runs(a, b)
    assert (rep["rules_changed"], rep["unresolved_changed"]) == (0, 0)
    assert BD.main([str(a), str(b)]) == 0
    assert "no binding changed" in capsys.readouterr().out


def test_gained_lost_and_rescoped_sections_are_counted_per_rule(tmp_path):
    a = _run(tmp_path / "a", SAME)
    b = _run(tmp_path / "b", [("e1", "r1", "a", "reach"), ("e1", "r1", "b", "reach"),
                              ("e1", "r1", "z", "trib"), ("e2", "r1", "d", "reach")])
    rep = BD.diff_runs(a, b)
    assert rep["rules"] == {
        "e2::r1": {"gained": 1, "lost": 1, "rescoped": 0, "sample_gained": ["d"], "sample_lost": ["c"]},
        "e1::r1": {"gained": 1, "lost": 0, "rescoped": 1, "sample_gained": ["z"], "sample_lost": []}}
    assert BD.main([str(a), str(b)]) == 1


def test_a_rule_that_stops_binding_is_reported_as_lost_and_unresolved(tmp_path):
    a = _run(tmp_path / "a", SAME)
    b = _run(tmp_path / "b", SAME[:2], unresolved=[("e2", "r1", "cut_not_on_this_water")])
    rep = BD.diff_runs(a, b)
    assert rep["rules"]["e2::r1"]["lost"] == 1
    assert rep["unresolved"] == {"e2::r1": {"before": None, "after": "cut_not_on_this_water"}}


def test_licensing_records_are_compared_too(tmp_path):
    a = _run(tmp_path / "a", SAME, licensing=[("e1", "L1", "a")])
    b = _run(tmp_path / "b", SAME, licensing=[("e1", "L1", "a"), ("e1", "L1", "b")])
    rep = BD.diff_runs(a, b)
    assert rep["licensing"]["e1::L1"]["gained"] == 1 and rep["rules_changed"] == 0


def test_a_table_out_of_order_is_refused_not_misread(tmp_path):
    """The merge-walk is only right on sorted input; an unsorted file would report phantom
    changes, so it is refused."""
    a = _run(tmp_path / "a", SAME)
    b = tmp_path / "b"
    b.mkdir()
    (b / "rule_section.jsonl").write_text(
        json.dumps({"entry_id": "e2", "rule_id": "r1", "scope": "reach", "section_id": "c"}) + "\n"
        + json.dumps({"entry_id": "e1", "rule_id": "r1", "scope": "reach", "section_id": "a"}) + "\n")
    with pytest.raises(ValueError, match="out of order"):
        BD.diff_runs(a, b)
    assert BD.main([str(a), str(b)]) == 2


def test_a_run_written_by_write_run_is_in_the_order_the_diff_needs(tmp_path):
    """The order is `io.write_run`'s, not an assumption: write one and diff it against itself."""
    from pipeline.atlas.reach.build import ReachResult
    from pipeline.atlas.reach.io import write_run
    from pipeline.atlas.reach.models import BuildReport, Outcome, RuleBinding
    bindings = [RuleBinding("e2", "r1", Outcome.bound, ("s9", "s1")),
                RuleBinding("e1", "r2", Outcome.bound, ("s3", "s2")),
                RuleBinding("e1", "r1", Outcome.bound, ("s5",))]
    res = ReachResult(bindings, [], BuildReport(build="t"), [], [], [])
    write_run(tmp_path / "run", res, [])
    assert BD.diff_runs(tmp_path / "run", tmp_path / "run")["rules_changed"] == 0
