"""T184: the review page's marks come back into the repo as the marks `POST /api/marks` writes.

    page doc (artifact db, collection "marks") -> import_artifact_marks -> marks.jsonl -> gate.pilot_report

The export here is built by hand in the export's shape (targets with their markable units, each unit's
prints keyed by `fp_of`); the round trip and every refusal are pinned whole, and by mutation.
"""
from __future__ import annotations

import json

import pytest
from pipeline.regs.review import gate as G
from pipeline.regs.review import import_artifact_marks as IM
from pipeline.regs.review import marks as M
from pipeline.regs.review.export_artifact import fp_of

SHA = "a" * 64


def _prints_entry(eid, rid=None):
    recs = [{"key": "entry", "meaning": f"m-{eid}", "binding": "b0", "reach": "r0", "answer": "a0"}]
    if rid:
        recs.append({"key": f"rule:{rid}", "meaning": f"m-{rid}", "binding": "b1", "reach": "r1", "answer": "a1"})
    return {"text": f"t-{eid}", "records": recs}


def _export():
    units, prints = {}, {}

    def unit(scope, target, rid, pr):
        fp = fp_of(pr)
        prints[fp] = pr
        return {"scope": scope, "rule_id": rid, "fp": fp}

    split_pr = {"text": None, "records": [{"key": "split:s1", "meaning": "ms", "binding": "bs"}]}
    targets = [
        {"tier": "S0", "kind": "split", "id": "s1", "pages": [],
         "units": [unit("split", "s1", None, split_pr)]},
        {"tier": "P1", "kind": "entry", "id": "r1:a@1-1", "pages": [14],
         "units": [unit("entry", "r1:a@1-1", None, _prints_entry("r1:a@1-1"))]},
        {"tier": "Z", "kind": "entry", "id": "z1:q", "pages": [12],
         "units": [unit("rule", "z1:q", "q.r1", _prints_entry("z1:q", "q.r1")),
                   unit("rule", "z1:q", "q.r2", _prints_entry("z1:q", "q.r2"))]},
    ]
    return {"pdf_sha256": SHA, "targets": targets, "prints": prints}


def _doc(export, ti, ui=0, **kw):
    t = export["targets"][ti]
    u = t["units"][ui]
    scope = u["scope"]
    checks = M.CHECKS[scope]
    d = {"scope": scope, "target": t["id"], "rule_id": u["rule_id"], "verdict": "verified",
         "answers": {c: "y" for c in checks}, "problem": None, "note": "", "seconds": 42.5,
         "reviewer": "u_abc", "fp": u["fp"], "pages": t["pages"], "at": "2026-10-24T21:15:00.000Z", "v": 1}
    d.update(kw)
    return d


@pytest.fixture
def store(tmp_path):
    p = tmp_path / "marks.jsonl"
    p.write_text("", encoding="utf-8")
    return p


def test_a_page_doc_becomes_exactly_the_mark_post_api_marks_writes(store):
    ex = _export()
    got = IM.import_docs([_doc(ex, 1)], ex, store, reviewers={"u_abc": "Pat"})
    assert got == {"added": 1, "already": 0, "refused": [],
                   "marks": [{"scope": "entry", "target": "r1:a@1-1", "rule_id": None,
                              "verdict": "verified", "reviewer": "Pat"}]}
    want = M.Mark.model_validate({
        "scope": "entry", "target": "r1:a@1-1", "rule_id": None, "verdict": "verified",
        "answers": {"T": "y", "R": "y", "P": "y", "A": "y"}, "problem": None, "note": "",
        "reviewer": "Pat", "date": "2026-10-24", "pdf_sha256": SHA, "pages": [14],
        "prints": ex["prints"][ex["targets"][1]["units"][0]["fp"]], "seconds": 42.5})
    # the one serialisation of a mark: the store holds the line `POST /api/marks` would append
    assert store.read_text(encoding="utf-8") == M.line(want)


def test_the_round_trip_to_the_pilot_report(store):
    ex = _export()
    docs = [
        _doc(ex, 0, seconds=30),
        _doc(ex, 1, verdict="wrong", answers={"T": "y", "R": "n", "P": "y", "A": "y"},
             problem="quota", note="the book says 2", seconds=90),
        _doc(ex, 2, 0, seconds=20),
        _doc(ex, 2, 1, verdict="deferred", answers={}, note="TOOL: the map never drew", seconds=10,
             reviewer="u_def"),
    ]
    got = IM.import_docs(docs, ex, store, reviewers={"u_abc": "Pat"})
    assert (got["added"], got["refused"]) == (4, [])
    targets = [{"tier": t["tier"], "kind": t["kind"], "id": t["id"]} for t in ex["targets"]]
    rep = G.pilot_report(targets, M.load(store), {"r1:a@1-1": [], "z1:q": ["q.r1", "q.r2"]})
    assert rep["overall"] == {"targets": 3, "marked": 3, "timed": 3, "median_s": 30.0, "mean_s": 50.0,
                              "defects": 1, "tool_blockers": 1}
    assert rep["passes"] is False
    z = next(r for r in rep["targets"] if r["id"] == "z1:q")
    assert z["seconds"] == 30.0 and z["reviewers"] == ["Pat", "artifact:u_def"]
    assert z["tool_blockers"] == ["TOOL: the map never drew"]


def test_the_import_is_idempotent_and_a_remark_appends(store):
    ex = _export()
    d = _doc(ex, 1)
    assert IM.import_docs([d], ex, store)["added"] == 1
    assert IM.import_docs([d], ex, store)["added"] == 0
    assert IM.import_docs([d], ex, store)["already"] == 1
    again = dict(d, verdict="unsure", answers={"T": "y", "R": "?", "P": "y", "A": "y"},
                 at="2026-10-25T08:00:00Z")
    assert IM.import_docs([d, again], ex, store) == {**IM.import_docs([], ex, store, dry_run=True),
                                                     "added": 1, "already": 1,
                                                     "marks": [{"scope": "entry", "target": "r1:a@1-1",
                                                                "rule_id": None, "verdict": "unsure",
                                                                "reviewer": "artifact:u_abc"}]}
    held = M.latest(M.load(store))
    assert held[("entry", "r1:a@1-1", None)].verdict == "unsure"
    assert len(M.load(store)) == 2


def test_the_docs_read_from_a_dir_and_unwrapped(tmp_path, store):
    ex = _export()
    d = tmp_path / "docs"
    d.mkdir()
    (d / "m~3aentry.json").write_text(json.dumps({"id": "m:entry", "data": _doc(ex, 1)}), encoding="utf-8")
    (d / "m~3asplit.json").write_text(json.dumps(_doc(ex, 0)), encoding="utf-8")
    docs = IM.read_docs(d)
    assert sorted(x["target"] for x in docs) == ["r1:a@1-1", "s1"]
    assert IM.import_docs(docs, ex, store)["added"] == 2


@pytest.mark.parametrize("mutate, why", [
    (lambda d, ex: d.update(verdict="verified", answers={"T": "y", "R": "n", "P": "y", "A": "y"}),
     "is not what the answers give"),
    (lambda d, ex: d.update(answers={"T": "y"}), "Answer every check"),
    (lambda d, ex: d.update(verdict="wrong", answers={"T": "n", "R": "y", "P": "y", "A": "y"}, problem="",
                            note="x"), "A wrong mark needs a problem"),
    (lambda d, ex: d.update(verdict="deferred", answers={}, note=""), "To defer, write a note"),
    (lambda d, ex: d.update(fp="0" * 16), "is not a unit of this export"),
    (lambda d, ex: d.update(fp=ex["targets"][0]["units"][0]["fp"]), "is not the export's"),
    (lambda d, ex: d.update(target="r1:other@1-1"), "is not the export's"),
    (lambda d, ex: d.update(rule_id="a.r9"), "is not the export's"),
    (lambda d, ex: d.update(reviewer=""), "Type your name first"),
    (lambda d, ex: d.update(at="yesterday"), "is not an ISO time"),
    (lambda d, ex: d.update(seconds=-1), "greater than or equal to 0"),
])
def test_what_the_store_would_refuse_is_refused_and_nothing_is_written(store, mutate, why):
    ex = _export()
    d = _doc(ex, 1)
    mutate(d, ex)
    got = IM.import_docs([d], ex, store)
    assert got["added"] == 0 and len(got["refused"]) == 1 and why in got["refused"][0]["why"], got
    assert store.read_text(encoding="utf-8") == ""


def test_the_prints_are_the_export_s_never_the_doc_s(store):
    """MUTATION PIN: a doc carrying prints of its own (or another vintage's fp) does not set the mark's
    prints — the mark is held against what the export says the reviewer saw."""
    ex = _export()
    d = _doc(ex, 1, prints={"text": "forged", "records": []}, pdf_sha256="b" * 64)
    IM.import_docs([d], ex, store)
    m = M.load(store)[0]
    assert m.prints.text == "t-r1:a@1-1" and m.pdf_sha256 == SHA


def test_a_missing_store_raises():
    ex = _export()
    with pytest.raises(FileNotFoundError):
        IM.import_docs([_doc(ex, 1)], ex, __import__("pathlib").Path("/nonexistent/marks.jsonl"))


def test_fp_is_order_independent_and_content_bound():
    a = {"text": "t", "records": [{"key": "entry", "meaning": "m", "binding": "b"}]}
    b = {"records": [{"binding": "b", "meaning": "m", "key": "entry"}], "text": "t"}
    assert fp_of(a) == fp_of(b)
    assert fp_of(a) != fp_of({**a, "text": "u"})
