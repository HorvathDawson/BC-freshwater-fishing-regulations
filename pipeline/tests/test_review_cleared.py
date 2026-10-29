"""Owner 2026-10-29: a CLEARED mark takes back the mark before it (the store is append-only, so an
accidental "accept all visible" is undone by a newer line, never an edit). Its target reads UNMARKED to
every reader of the marks in force; a later mark stands as usual; no build acts on a cleared mark."""
from __future__ import annotations

import pytest

from pipeline.regs.review import gate
from pipeline.regs.review import marks as M

SHA = "0" * 64


def _prints(key: str) -> dict:
    return {"text": None, "records": [{"key": key, "meaning": "m", "binding": "b"}]}


def slough(t: str, *, verdict="verified", call="stream", note="", date="2026-10-29") -> M.Mark:
    return M.Mark.model_validate({"scope": "slough", "target": t, "verdict": verdict,
                                  "answers": {"P": "y"} if verdict == "verified" else {},
                                  "call": call if verdict == "verified" else None, "note": note,
                                  "reviewer": "owner", "date": date, "pdf_sha256": SHA,
                                  "prints": _prints(f"slough:{t}")})


def clear(m: M.Mark) -> M.Mark:
    return M.cleared_mark(m, reviewer="owner", note="accidental accept all", date="2026-10-29")


def tidal_unsure(t: str) -> M.Mark:
    return M.Mark.model_validate({
        "scope": "tidal", "target": t, "verdict": "unsure", "answers": {"P": "?"}, "note": "n",
        "reviewer": "owner", "date": "2026-10-29", "pdf_sha256": SHA, "prints": _prints(f"split:{t}"),
        "new_splits": [{"ref": "c1", "anchor": "coord", "coord": [-122.0, 49.2]}]})


def test_cleared_after_verified_is_todo():
    a = slough("1")
    ms = [a, clear(a)]
    assert M.latest(ms) == {}                                        # nothing in force
    assert M.status(M.latest(ms).get(a.key), M.Prints.model_validate(_prints("slough:1")))["status"] \
        == "unmarked"
    assert M.uncleared(ms) == []


def test_new_verified_after_cleared_is_verified():
    a = slough("1")
    b = slough("1", call="not stream")
    ms = [a, clear(a), b]
    assert M.latest(ms) == {b.key: b}
    assert M.status(b, M.Prints.model_validate(_prints("slough:1")))["status"] == "verified"
    assert M.uncleared(ms) == [b]


def test_cleared_needs_a_note_and_carries_nothing():
    a = slough("1")
    with pytest.raises(ValueError, match="cleared needs a note"):
        M.cleared_mark(a, reviewer="owner", note=" ", date="2026-10-29")
    with pytest.raises(ValueError, match="nothing to clear"):
        M.cleared_mark(clear(a), reviewer="owner", note="x", date="2026-10-29")
    with pytest.raises(ValueError, match="carries no answers"):
        M.Mark.model_validate({**clear(a).model_dump(), "answers": {"P": "y"}})
    # the line round-trips through the store's one serialisation
    c = clear(a)
    assert M.Mark.model_validate_json(M.line(c)) == c


def test_builds_and_readers_ignore_cleared(tmp_path):
    a, t = slough("1"), tidal_unsure("t1")
    ms = [a, t, clear(a), clear(t)]
    # the shared proposed splits: a cleared mark's new split is taken back
    assert M.proposals([t]) and M.proposals(ms) == []
    # the deferred lake-split list, the marks health and the pilot's measure count none of them
    assert M.lake_splits_later(ms) == []
    assert M.health(ms, entries={}, ctx=None)["scopes"] == {}
    rep = gate.pilot_report([{"tier": "x", "kind": "slough", "id": "1"}], ms)
    assert rep["overall"]["marked"] == 0
    # the store: appended, never edited — load reads the cleared line back and `latest` drops it
    p = tmp_path / "marks.jsonl"
    p.write_text("")
    M.append(p, [a])
    before = p.read_text()
    M.append(p, [clear(a)])
    assert p.read_text().startswith(before)
    assert M.latest(M.load(p)) == {}
