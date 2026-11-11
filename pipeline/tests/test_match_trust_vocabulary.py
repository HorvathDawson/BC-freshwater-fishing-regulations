"""T550 item 6: the gauge match's trust words are a closed set, and promote writes a status that agrees with its trust."""

from __future__ import annotations

import json

import pytest

from pipeline.gauges.matches import TRUST_STATUS, read_match, trust_disagreements


def _file(tmp_path, rows):
    p = tmp_path / "matches.json"
    p.write_text(json.dumps({"stations": rows}))
    return p


ROW = {"station": "08XX001", "lon": -123.0, "lat": 49.0, "wbk": [], "wsc": [], "status": "matched", "resolved_by": "test",
       "distance_m": 0.0}


@pytest.mark.parametrize("word", ["rejected", "Wrong", "matched", ""])
def test_an_unknown_trust_word_is_refused_not_read_as_matched(tmp_path, word):
    p = _file(tmp_path, [ROW | {"trust": word}])
    if not word:  # no trust word: an unpromoted row, read by its own status
        assert read_match(p)[0].status == "matched"
        return
    with pytest.raises(ValueError, match="08XX001"):
        read_match(p)


@pytest.mark.parametrize("word,status", sorted(TRUST_STATUS.items()))
def test_every_trust_word_reads_as_its_status(tmp_path, word, status):
    assert read_match(_file(tmp_path, [ROW | {"trust": word}]))[0].status == status


def test_a_stored_status_that_contradicts_its_trust_is_listed(tmp_path):
    p = _file(tmp_path, [ROW | {"trust": "wrong"}, ROW | {"station": "08XX002", "trust": "auto"}])
    assert trust_disagreements(p) == ["08XX001"]


def test_promote_writes_the_status_its_trust_reads_as():
    import inspect

    from pipeline.gauges.generate import promote
    src = inspect.getsource(promote)
    for word in ("bound", "reviewed", "wrong", "none", "auto"):
        assert f'"trust": "{word}", "status": TRUST_STATUS["{word}"]' in src
