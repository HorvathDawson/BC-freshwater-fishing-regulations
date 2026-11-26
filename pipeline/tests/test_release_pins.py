"""(T1135) The release pins, the pipeline helper (`pipeline/tests/pins.py`) on scratch files, and the release's judge of
a moved pin (`release_follows.moved_answer_pins`) on a scratch repo. No release is read."""
from __future__ import annotations

import json
import subprocess

import pytest

from pipeline import release_follows as RF
from pipeline.tests.pins import PINS_FILE, make_pin, read

PINS = {"a.count": {"answers": False, "value": {"n": 67, "v": 5}},
        "a.words": {"answers": True, "value": "From the table."}}


def _file(d, pins=PINS):
    f = d / "release-pins.json"
    f.write_text(json.dumps({"pins": pins}), encoding="utf-8")
    return f


def test_compare_an_equal_value_passes_another_fails_naming_the_pin_the_file_and_the_recut(tmp_path):
    pin = make_pin(_file(tmp_path), None)
    pin("a.count", {"v": 5, "n": 67})
    pin("a.words", "From the table.")
    with pytest.raises(AssertionError, match=r"pin a\.words .*release-pins\.json.*the release says 'From the table\. "
                                             r"Covers it\.'.*node app/tools/regs-recut/sync\.mjs"):
        pin("a.words", "From the table. Covers it.")


def test_compare_an_unknown_name_fails_never_a_pass(tmp_path):
    pin = make_pin(_file(tmp_path), None)
    with pytest.raises(AssertionError, match=r"pin a\.nope: not in .*add its row by hand"):
        pin("a.nope", 1)
    with pytest.raises(AssertionError, match="a pin name is"):
        pin("a b", 1)
    with pytest.raises(AssertionError, match="not a JSON value"):
        pin("a.count", object())


def test_record_writes_the_value_and_asserts_nothing(tmp_path):
    rec = tmp_path / "rec"
    pin = make_pin(_file(tmp_path), str(rec))
    pin("a.words", "From the table. Covers it.")          # moved: recorded, no failure
    pin("a.new", (1, 2))                                  # a name the file lacks: recorded (the writer refuses it)
    got = {p.stem: json.loads(p.read_text()) for p in (rec / "pipeline").glob("*.json")}
    assert got == {"a.words": {"name": "a.words", "value": "From the table. Covers it."},
                   "a.new": {"name": "a.new", "value": [1, 2]}}
    with pytest.raises(AssertionError, match="recorded twice with different values"):
        pin("a.words", "another")
    assert read(tmp_path / "release-pins.json") == PINS   # a test never writes the pins file


def test_the_committed_file_holds_every_pin_with_its_answers_flag():
    pins = read(PINS_FILE)
    assert pins and all(isinstance(p["answers"], bool) and "value" in p for p in pins.values())


# ------------------------------------------------------------------------------------- the release's judge, real git
def _git(d, *a):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *a], cwd=d, capture_output=True,
                          text=True, check=True).stdout


@pytest.fixture
def repo(tmp_path, monkeypatch):
    d = tmp_path / "repo"
    (d / "pipeline/tests/fixtures").mkdir(parents=True)
    (d / "app/fixtures/golden").mkdir(parents=True)
    for p in RF.paths_of(RF.RECUT):
        if p.endswith("release-pins.json"):
            (d / p).write_text(json.dumps({"pins": PINS}), encoding="utf-8")
    _git(d, "init", "-q")
    _git(d, "add", "-A")
    _git(d, "commit", "-q", "-m", "first")
    monkeypatch.setattr(RF, "REPO_ROOT", d)
    return d


APP_PINS = "app/fixtures/golden/release-pins.json"


def _put(d, pins):
    (d / APP_PINS).write_text(json.dumps({"pins": pins}), encoding="utf-8")


def test_a_moved_count_is_committed_a_moved_sentence_stops(repo):
    """`answers: false` (counts, ids) moves like a renumber; `answers: true` (words, pages) stops with the golden pages."""
    assert RF.row_of(APP_PINS).name == "pins" and RF.row_of(APP_PINS).pins
    _put(repo, {**PINS, "a.count": {"answers": False, "value": {"n": 68, "v": 5}}})
    assert RF.moved_answer_pins(APP_PINS) == [] and RF.answer_paths([APP_PINS]) == []
    _put(repo, {**PINS, "a.words": {"answers": True, "value": "From the table. Covers it."}})
    assert RF.moved_answer_pins(APP_PINS) == ["a.words"]
    page = "app/packages/ui-native/src/regs/fixtures/glenn_501.json"
    assert RF.answer_paths([APP_PINS, page]) == [APP_PINS, page]
    assert RF.said([APP_PINS, page]) == [f"{APP_PINS} — answer pin(s) moved: a.words", page]
    assert APP_PINS in RF.answer_roots() and page.rsplit("/", 1)[0] in RF.answer_roots()


def test_an_answer_pin_dropped_or_added_stops_and_an_unreadable_file_never_passes(repo):
    _put(repo, {"a.count": PINS["a.count"]})                                   # the sentence pin dropped (stale)
    assert RF.moved_answer_pins(APP_PINS) == ["a.words"]
    _put(repo, {**PINS, "b.new": {"answers": True, "value": "x"}, "c.id": {"answers": False, "value": 3}})
    assert RF.moved_answer_pins(APP_PINS) == ["b.new"]
    (repo / APP_PINS).write_text("{not json", encoding="utf-8")
    moved = RF.moved_answer_pins(APP_PINS)
    assert len(moved) == 1 and moved[0].startswith("(unreadable: JSONDecodeError")
    assert RF.answer_paths([APP_PINS]) == [APP_PINS]
