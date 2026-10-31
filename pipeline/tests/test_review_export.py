"""T266: ONE EXPORTER — the marks in force -> per-domain decided-data files, which the apply paths read.

  committed   export(marks.jsonl) == the committed data/curated/decided/ files (the export is never stale)
  round trip  load(files) is decide(marks): the marks in force (in `latest` order) and the shared cuts
  cleared     a CLEARED key decides nothing, and its proposed cuts are taken back with it
  conflict    two tidal decisions cutting one water at different points are refused, nothing written
  provenance  every record names its mark (line sha), line, reviewer and date
"""
from __future__ import annotations

import json

import pytest

from pipeline.common.curated import CURATED
from pipeline.regs.review import export as E
from pipeline.regs.review import marks as M

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


def test_the_committed_files_are_the_export_of_the_marks():
    assert E.stale() == [], "python -m pipeline.regs.review.export"


def test_loading_the_files_gives_back_what_the_marks_decide():
    from pipeline.regs.review import closed as C
    want = E.decide(M.load(CURATED.regulations.review.marks))
    got = E.load()
    shut = C.scopes()                     # T325: a CLOSED domain (ponds, T329) is not exported, so not loaded
    open_ = [m for m in want.inforce if m.scope not in shut]
    assert [m.key for m in got.inforce] == [m.key for m in open_]
    assert list(got.inforce) == open_
    assert [(g["id"], g["split"], g["members"], g["marks"]) for g in got.groups] == \
        [(g["id"], g["split"], g["members"], g["marks"]) for g in want.groups]
    assert got.shared() == M.proposals(  # the shared cuts, as the marks module makes them from the history
        list(_early(want)) + M.load(CURATED.regulations.review.marks))


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
    E.write(d, tmp_path)
    back = E.load(tmp_path)
    assert back.inforce == d.inforce and [g["id"] for g in back.groups] == ["tidal-2"]


def test_conflicting_tidal_decisions_are_refused(tmp_path):
    a = _tidal("tidal:1", "gnis:7", (-123.0, 49.0))
    b = _tidal("tidal:2", "gnis:7", (-123.5, 49.5))         # the same water, 50 km away
    with pytest.raises(ValueError, match="conflicting decisions"):
        E.decide([a, b], tidal_decisions=None)
    assert not list(tmp_path.iterdir())


def test_every_record_carries_its_provenance(tmp_path):
    a = _tidal("tidal:1", "gnis:1", (-123.0, 49.0), date="2026-10-27")
    a2 = _tidal("tidal:1", "gnis:1", (-123.0, 49.0), date="2026-10-28", note="again")
    E.write(E.decide([a, a2], tidal_decisions=None), tmp_path)
    doc = json.loads((tmp_path / "tidal.json").read_text())
    [r] = doc["decisions"]
    assert r["provenance"] == {"mark": E._sha(M.line(a2)), "line": 2, "reviewer": "dawson",
                               "date": "2026-10-28", "source": "marks.jsonl"}
    assert r["mark"]["note"] == "again"


def test_the_export_is_idempotent(tmp_path):
    d = E.decide([_tidal("tidal:1", "gnis:1", (-123.0, 49.0))], tidal_decisions=None)
    assert len(E.write(d, tmp_path)) == len(E.FILES)
    assert E.write(d, tmp_path) == []


def test_a_missing_file_raises_naming_the_command(tmp_path):
    with pytest.raises(FileNotFoundError, match="pipeline.regs.review.export"):
        E.load(tmp_path)
