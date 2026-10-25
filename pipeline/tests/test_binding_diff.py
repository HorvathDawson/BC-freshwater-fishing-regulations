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


# ---------------------------------------------------------------------------------------------
# T73: the bindings as one gzip line per rule; the old form still reads, and diffs against it
# ---------------------------------------------------------------------------------------------
ROWS = [("e1", "r1", "1:0", "reach"), ("e1", "r1", "2:0", "trib"), ("e1", "r2", "1:0", "trib"),
        ('e"q', "r1", "é:1", "reach")]


def test_the_gzip_form_reads_back_the_rows_written_in_their_order(tmp_path):
    import gzip
    from pipeline.atlas.reach import io as IO
    p = tmp_path / IO.RULE_SECTION_FILE
    assert IO.write_rule_sections(p, sorted(ROWS)) == 4
    assert list(IO.iter_rule_sections(p)) == sorted(ROWS)
    lines = [json.loads(x) for x in gzip.decompress(p.read_bytes()).decode().splitlines()]
    assert len(lines) == 3                                       # one line per bound rule
    assert lines[1] == {"entry_id": "e1", "rule_id": "r1", "sections": ["1:0", "2:0"], "trib": ["2:0"]}
    again = tmp_path / "again.jsonl.gz"
    IO.write_rule_sections(again, sorted(ROWS))
    assert again.read_bytes() == p.read_bytes()                  # byte-identical (gzip mtime 0)


def test_an_old_run_and_its_conversion_diff_as_identical(tmp_path):
    """The T73 acceptance in small: binding_diff old form vs new form = no binding changed; a lost
    or rescoped section after conversion is still seen (mutation)."""
    from pipeline.atlas.reach import io as IO
    old, new = _run(tmp_path / "old", ROWS), _run(tmp_path / "new", ROWS)
    got = IO.convert(new)
    assert got["rows"] == 4 and not (new / IO.LEGACY_RULE_SECTION_FILE).exists()
    assert IO.rule_section_file(new).name == IO.RULE_SECTION_FILE
    rep = BD.diff_runs(old, new)
    assert (rep["rules_changed"], rep["unresolved_changed"]) == (0, 0)
    assert IO.read_run(old) == IO.read_run(new)
    IO.write_rule_sections(new / IO.RULE_SECTION_FILE, sorted(ROWS[:1] + ROWS[2:]))
    assert BD.diff_runs(old, new)["rules"]["e1::r1"]["lost"] == 1
    IO.write_rule_sections(new / IO.RULE_SECTION_FILE,
                           sorted([ROWS[0], ("e1", "r1", "2:0", "reach")] + ROWS[2:]))
    assert BD.diff_runs(old, new)["rules"]["e1::r1"]["rescoped"] == 1
    side = tmp_path / "side"
    IO.convert(old, to=side)                         # --to: the source run is left as it was
    assert (old / IO.LEGACY_RULE_SECTION_FILE).exists() and (side / "rule_unresolved.jsonl").exists()
    assert BD.diff_runs(old, side)["rules_changed"] == 0


def test_write_run_writes_only_the_gzip_form_and_every_reader_takes_it(tmp_path):
    from pipeline.atlas.reach import io as IO
    from pipeline.atlas.reach.build import ReachResult
    from pipeline.atlas.reach.models import BuildReport, Outcome, RuleBinding
    from pipeline.regs.review.parity import run_sections
    d = tmp_path / "run"
    d.mkdir()
    (d / IO.LEGACY_RULE_SECTION_FILE).write_text("stale\n")       # a rerun into an old run's dir
    b = RuleBinding("e1", "r1", Outcome.bound, ("s2", "s1"), via_tributary=("s2",))
    counts = IO.write_run(d, ReachResult([b], [], BuildReport(build="t"), [], [], []), [])
    assert counts["rule_section"] == 2
    assert not (d / IO.LEGACY_RULE_SECTION_FILE).exists()
    assert list(IO.iter_rule_sections(IO.rule_section_file(d))) == [
        ("e1", "r1", "s1", "reach"), ("e1", "r1", "s2", "trib")]
    assert [(e, list(rr)) for e, rr in run_sections(d)] == [("e1", [("r1", frozenset({"s1", "s2"}))])]


def test_a_gzip_file_out_of_order_or_beside_the_old_one_is_refused(tmp_path):
    from pipeline.atlas.reach import io as IO
    p = tmp_path / IO.RULE_SECTION_FILE
    IO.write_rule_sections(p, [("e2", "r1", "a", "reach"), ("e1", "r1", "a", "reach")])
    with pytest.raises(ValueError, match="out of order"):
        list(IO.iter_rule_sections(p))
    IO.write_rule_sections(p, [("e1", "r1", "b", "reach"), ("e1", "r1", "a", "reach")])
    with pytest.raises(ValueError, match="out of order"):
        list(IO.iter_rule_sections(p))
    (tmp_path / IO.LEGACY_RULE_SECTION_FILE).write_text("")
    with pytest.raises(SystemExit, match="both"):
        IO.rule_section_file(tmp_path)
    with pytest.raises(SystemExit, match="rule_section.jsonl"):
        IO.rule_section_file(tmp_path / "nowhere")
    with pytest.raises(ValueError, match="neither reach nor trib"):
        IO.write_rule_sections(p, [("e1", "r1", "a", "up")])
