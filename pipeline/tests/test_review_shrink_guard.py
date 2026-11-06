"""T429 (audit 2026-11-05): a missing review input must not empty a curated file.

  loaders   names / sloughs / tidal candidates: a missing input RAISES (it once read as an empty set, and the next
            saved mark wrote `[]` over found_names.json / slough_calls.json)
  shrink    `apply.write_files` refuses a write that drops more than `SHRINK_LIMIT` rows (an emptied live file), unless
            `accept_shrink`; an un-mark (a row or two, even the last) lands; `apply.run` says the refusal as a
            refused event, rc 2
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
    assert AP.write_files("names", None, dry_run=False) == [p]


def test_run_says_refused(out, tmp_path, monkeypatch):
    _, want = out
    want[0] = 0
    monkeypatch.setattr(E, "load", lambda *a, **k: None)
    got = AP.run(["names"], marks=tmp_path / "marks.jsonl")
    assert got["rc"] == 2
    assert [e["status"] for e in got["events"]] == ["refused"]
    assert "would drop 40 of 40" in got["events"][0]["why"]
