"""T266 / T408: what the marks decide, computed in memory (no decided-data files) — what the apply paths read.

  no files    there is no data/curated/decided/ and no `under_test.curated().decided` (owner 2026-11-05: dropped)
  load        load(marks) is decide(marks) with a CLOSED domain's marks left out (in `latest` order) + the shared cuts
  cleared     a CLEARED key decides nothing, and its proposed cuts are taken back with it
  conflict    two tidal decisions cutting one water at different points are refused
  provenance  every record names its mark (line sha), line, reviewer and date
"""
from __future__ import annotations

import pytest

from pipeline.regs.review import export as E
from pipeline.regs.review import marks as M
from pipeline.tests import under_test

PDF = "0" * 64


def _tidal(target, on_item, coord, *, verdict="verified", date="2026-10-28", note=""):
    return M.Mark.model_validate({
        "scope": "tidal", "target": target, "verdict": verdict, "answers": {"P": "y"},
        "reviewer": "dawson", "date": date, "pdf_sha256": PDF, "note": note,
        "prints": {"text": None, "records": [{"key": f"split:{target}", "meaning": "m", "binding": "b"}]},
        "new_splits": [{"ref": target.replace(":", "-"), "anchor": "coord", "coord": coord, "on_item": on_item,
                        "name": "tidal boundary"}]})


def _cleared(m, date="2026-10-29"):
    return M.cleared_mark(m, reviewer="dawson", note="taken back", date=date)


def test_there_is_no_decided_data_directory():
    from pipeline.common.curated import REPO_ROOT
    assert not (REPO_ROOT / "data" / "curated" / "decided").exists()
    assert not hasattr(under_test.curated(), "decided")


def test_load_is_what_the_marks_decide_less_the_closed_domains():
    from pipeline.regs.review import closed as C
    want = E.decide(M.load(under_test.curated().review.marks))
    got = E.load()
    shut = C.scopes()                     # T325: a CLOSED domain (ponds, T329) is left out
    open_ = [m for m in want.inforce if m.scope not in shut]
    assert [m.key for m in got.inforce] == [m.key for m in open_]
    assert list(got.inforce) == open_
    assert [(g["id"], g["split"], g["members"], g["marks"]) for g in got.groups] == \
        [(g["id"], g["split"], g["members"], g["marks"]) for g in want.groups]
    assert got.shared() == M.proposals(  # the shared cuts, as the marks module makes them from the history
        list(_early(want)) + M.load(under_test.curated().review.marks))


def _early(d):
    from pipeline.regs.review import proposals as P
    from pipeline.common.curated import REPO_ROOT
    return P.tidal_decision_marks(REPO_ROOT / d.tidal_decisions) if d.tidal_decisions else []


def test_a_cleared_key_decides_nothing_and_takes_its_cut_back(tmp_path):
    a = _tidal("tidal:1", "gnis:1", (-123.0, 49.0))
    b = _tidal("tidal:2", "gnis:2", (-124.0, 50.0))
    d = E.decide([a, b, _cleared(a)], tidal_decisions=None)
    assert [m.target for m in d.inforce] == ["tidal:2"]
    assert [g["id"] for g in d.groups] == ["tidal-2"]
    store = tmp_path / "marks.jsonl"
    store.write_text("".join(M.line(m) + "\n" for m in (a, b, _cleared(a))))
    back = E.load(store)
    assert back.inforce == d.inforce and [g["id"] for g in back.groups] == ["tidal-2"]


def test_conflicting_tidal_decisions_are_refused(tmp_path):
    a = _tidal("tidal:1", "gnis:7", (-123.0, 49.0))
    b = _tidal("tidal:2", "gnis:7", (-123.5, 49.5))         # the same water, 50 km away
    with pytest.raises(ValueError, match="conflicting decisions"):
        E.decide([a, b], tidal_decisions=None)
    store = tmp_path / "marks.jsonl"
    store.write_text("".join(M.line(m) + "\n" for m in (a, b)))
    with pytest.raises(ValueError, match="conflicting decisions"):
        E.load(store)


def test_every_record_carries_its_provenance(tmp_path):
    a = _tidal("tidal:1", "gnis:1", (-123.0, 49.0), date="2026-10-27")
    a2 = _tidal("tidal:1", "gnis:1", (-123.0, 49.0), date="2026-10-28", note="again")
    d = E.decide([a, a2], tidal_decisions=None)
    [m] = d.inforce
    assert d.provenance[m.key] == {"mark": E._sha(M.line(a2)), "line": 2, "reviewer": "dawson",
                                   "date": "2026-10-28", "source": "marks.jsonl"}
    assert m.note == "again"


def test_a_closed_domain_is_left_out(tmp_path):
    a = _tidal("tidal:1", "gnis:1", (-123.0, 49.0))
    store = tmp_path / "marks.jsonl"
    store.write_text(M.line(a) + "\n")
    assert [m.key for m in E.load(store, closed=()).inforce] == [a.key]
    shut = E.load(store, closed={"tidal"})
    assert shut.inforce == () and dict(shut.provenance) == {}
    assert E.load(store).inforce == E.load(store, closed=()).inforce   # no closed.json beside a temp store


def test_load_with_no_store_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        E.load(tmp_path / "absent.jsonl")
