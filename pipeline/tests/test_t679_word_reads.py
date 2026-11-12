"""T679 D4 (owner 2026-11-12): the printed-word reads stay regexes; a read that matched before and stops matching stops the
reach run, naming the row (`reach.word_reads`).

MUTATION PINS:
  * `compare` counts a lost read as gone (or drops it)  -> test_a_read_that_stops_matching_is_lost_and_named
  * a new read stops the run                           -> test_a_new_read_is_reported_not_a_stop
  * the list loader returns [] for a missing file      -> test_a_missing_list_is_refused
No data: synthetic rows.
"""
from __future__ import annotations

import pytest

from pipeline.atlas.reach import word_reads as W
from pipeline.atlas.reach.models import Diagnostic


def _corpus(words):
    return [{"entry_id": "r9:main@9-1", "name": "MAIN RIVER (also in M.U. 5-15)",
             "rules": [{"rule_id": "main.r1", "verbatim": words}]}]


DIAG = [Diagnostic("r9:main@9-1", "main.r1", "confluence_cut", {"water": "X Creek", "split": "c"})]


def test_every_read_is_keyed_by_row_and_read():
    got = W.reads(_corpus("No Fishing upstream of and including X Creek"), DIAG)
    assert got == ["r9:main@9-1|main.r1|includes|X Creek", "r9:main@9-1||also_in|5-15"]


def test_a_read_that_stops_matching_is_lost_and_named():
    before = W.reads(_corpus("No Fishing upstream of and including X Creek"), DIAG)
    corpus = _corpus("No Fishing upstream of X Creek")
    diff = W.compare(W.reads(corpus, DIAG), before, W.present(corpus))
    assert diff["lost"] == ["r9:main@9-1|main.r1|includes|X Creek"] and diff["new"] == []
    assert "r9:main@9-1|main.r1|includes|X Creek" in W.lost_message(diff)


def test_a_new_read_is_reported_not_a_stop():
    corpus = _corpus("No Fishing upstream of and including X Creek")
    diff = W.compare(W.reads(corpus, DIAG), [], W.present(corpus))
    assert diff["lost"] == [] and len(diff["new"]) == 2


def test_a_read_whose_row_is_gone_is_not_a_stop():
    diff = W.compare([], ["r9:gone@9-1|gone.r1|includes|X Creek"], W.present(_corpus("")))
    assert diff["lost"] == [] and diff["gone"] == ["r9:gone@9-1|gone.r1|includes|X Creek"]


def test_a_missing_list_is_refused(tmp_path):
    with pytest.raises(SystemExit, match="missing"):
        W.load(tmp_path / "none.json")
    p = tmp_path / "w.json"
    p.write_text(W.document(["a|b|includes|X"]))
    assert W.load(p) == ["a|b|includes|X"]

