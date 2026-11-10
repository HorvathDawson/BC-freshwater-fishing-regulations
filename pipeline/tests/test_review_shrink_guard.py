"""T429 (audit 2026-11-05): a missing review input must not empty a curated file.

  loaders   names / sloughs / tidal candidates: a missing input RAISES (it once read as an empty set, and the next
            saved mark wrote `[]` over found_names.json / slough_calls.json)
  shrink    `apply.write_files` refuses a write that drops more than `SHRINK_LIMIT` rows (an emptied live file), unless
            `accept_shrink`; an un-mark (a row or two, even the last) lands; `apply.run` says the refusal as a
            refused event, rc 2
  dropped   (T545) every row a write drops is NAMED; a drop of even ONE row the owner never spoke about (no mark on it,
            ever: a regenerated candidates file, a flipped auto-accept flag) is refused, by name
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from pipeline.regs.review import apply as AP
from pipeline.regs.review import export as E
from pipeline.regs.review import names as N
from pipeline.regs.review import slough as S
from pipeline.regs.review import tidal as T


def _doc(n: int) -> str:
    return json.dumps({"_about": "t", "entries": [{"i": i} for i in range(n)]}) + "\n"


# --------------------------------------------------------------------------- loaders

def test_names_missing_input_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("CURATION_NAME_CANDIDATES", str(tmp_path / "gone.json"))
    N._load.cache_clear()
    with pytest.raises(OSError):
        N.load()


@pytest.mark.parametrize("missing", ["automark", "finder"])
def test_slough_missing_input_raises(tmp_path, monkeypatch, missing):
    (tmp_path / "automark.json").write_text(json.dumps({"rows": []}))
    with gzip.open(tmp_path / "finder.json.gz", "wt") as f:
        json.dump({"candidates": []}, f)
    (tmp_path / ("automark.json" if missing == "automark" else "finder.json.gz")).unlink()
    monkeypatch.setenv("CURATION_SLOUGH_AUTOMARK", str(tmp_path / "automark.json"))
    monkeypatch.setenv("CURATION_SLOUGH_FINDER", str(tmp_path / "finder.json.gz"))
    S._load.cache_clear()
    with pytest.raises(OSError):
        S.load()


def test_tidal_missing_input_raises(tmp_path, monkeypatch):
    monkeypatch.setenv("CURATION_TIDAL_CANDIDATES", str(tmp_path / "gone.json"))
    with pytest.raises(OSError):
        T.load()


# --------------------------------------------------------------------------- shrink

@pytest.fixture()
def out(tmp_path, monkeypatch):
    """A 40-row final file; the names writer returns whatever `want[0]` rows."""
    p = tmp_path / "found_names.json"
    p.write_text(_doc(40))
    want = [40]
    monkeypatch.setitem(AP.WRITERS, "names", lambda d: {p: _doc(want[0])})
    monkeypatch.setitem(AP.SPOKEN, "names", lambda d: set(AP.row_keys(_doc(40))))   # every row was marked once
    return p, want


@pytest.mark.parametrize("n", [0, 40 - AP.SHRINK_LIMIT - 1])
def test_shrink_refused(out, n):
    p, want = out
    want[0] = n
    with pytest.raises(ValueError, match="refused"):
        AP.write_files("names", None, dry_run=False)
    assert AP.rows_in(p.read_text()) == 40                      # the file is untouched


def test_shrink_accepted(out):
    p, want = out
    want[0] = 0
    AP.write_files("names", None, dry_run=False, accept_shrink=True)
    assert AP.rows_in(p.read_text()) == 0


@pytest.mark.parametrize("n", [39, 40 - AP.SHRINK_LIMIT, 45])
def test_small_change_lands(out, n):
    p, want = out
    want[0] = n
    assert AP.write_files("names", None, dry_run=False) == [p]
    assert AP.rows_in(p.read_text()) == n


def test_last_row_unmarked_lands(tmp_path, monkeypatch):
    p = tmp_path / "found_names.json"
    p.write_text(_doc(1))
    monkeypatch.setitem(AP.WRITERS, "names", lambda d: {p: _doc(0)})
    monkeypatch.setitem(AP.SPOKEN, "names", lambda d: set(AP.row_keys(_doc(1))))
    assert AP.write_files("names", None, dry_run=False) == [p]


def test_run_says_refused(out, tmp_path, monkeypatch):
    _, want = out
    want[0] = 0
    monkeypatch.setattr(E, "load", lambda *a, **k: E.decide([]))
    got = AP.run(["names"], marks=tmp_path / "marks.jsonl")
    assert got["rc"] == 2
    assert [e["status"] for e in got["events"]] == ["refused"]
    assert "would drop 40 of 40" in got["events"][0]["why"]


# --------------------------------------------------------------------------- dropped rows (T545)

def _named(*names: str) -> str:
    return json.dumps({"entries": [{"target": {"blks": [str(i)]}, "names": [{"name": n, "candidate": f"blk:{i}~{n}"}]}
                                   for i, n in enumerate(names)]}) + "\n"


@pytest.fixture()
def names_file(tmp_path, monkeypatch):
    """found_names.json with three names; the writer now makes it without `Gone Creek`."""
    p = tmp_path / "found_names.json"
    p.write_text(_named("Kept Creek", "Gone Creek", "Other Creek"))
    monkeypatch.setitem(AP.WRITERS, "names", lambda d: {p: _named("Kept Creek", "Other Creek").replace(
        '"blks": ["1"]', '"blks": ["2"]').replace("blk:1~Other", "blk:2~Other")})
    spoken: set = set()
    monkeypatch.setitem(AP.SPOKEN, "names", lambda d: spoken)
    return p, spoken


def test_a_row_nobody_marked_is_never_dropped_and_the_refusal_names_it(names_file):
    p, _ = names_file
    before = p.read_text()
    with pytest.raises(ValueError, match=r"would drop 1 row\(s\) that no mark in force explains — Gone Creek \(blk:1~Gone Creek\)"):
        AP.write_files("names", None, dry_run=False)
    assert p.read_text() == before


def test_a_dropped_row_the_owner_spoke_about_lands_and_is_named(names_file, capsys):
    p, spoken = names_file
    spoken.add("blk:1~Gone Creek")
    report: dict = {}
    assert AP.write_files("names", None, dry_run=False, report=report) == [p]
    assert report["dropped"] == ["Gone Creek (blk:1~Gone Creek)"]
    assert "names: DROPPED Gone Creek (blk:1~Gone Creek)" in capsys.readouterr().out
    assert sorted(AP.row_keys(p.read_text())) == ["blk:0~Kept Creek", "blk:2~Other Creek"]


def test_run_says_the_dropped_row_on_the_written_event(names_file, tmp_path, monkeypatch):
    _, spoken = names_file
    spoken.add("blk:1~Gone Creek")
    monkeypatch.setattr(E, "load", lambda *a, **k: E.decide([]))
    got = AP.run(["names"], marks=tmp_path / "marks.jsonl")
    [e] = got["events"]
    assert got["rc"] == 0 and e["status"] == "written" and e["dropped"] == ["Gone Creek (blk:1~Gone Creek)"]
    assert "dropped: Gone Creek" in e["why"]


def test_every_mark_ever_made_explains_its_row_a_cleared_one_too():
    """An un-mark (verdict cleared) leaves no mark in force — the row it takes out is still the owner's doing."""
    from pipeline.regs.review import marks as M
    def mark(verdict, **kw):
        return M.Mark.model_validate({"scope": "slough", "target": "7", "verdict": verdict, "answers": {},
                                      "reviewer": "t", "date": "2026-11-09", "pdf_sha256": "0" * 64, "note": "n",
                                      "prints": {"text": None, "records": [{"key": "slough:7", "meaning": "m", "binding": "b"}]},
                                      **kw})
    d = E.decide([mark("verified", call="stream", answers={"P": "y"}), mark("cleared")])
    assert not d.scope("slough") and AP.SPOKEN["sloughs"](d) == {"7"}
